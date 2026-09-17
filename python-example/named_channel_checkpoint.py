"""Coordinate retained source offsets with a named channel.

This is an alternative to the Elastic progression for integrations that need
a stable channel identity and source-offset checkpointing.

A named channel reports its latest committed offset token when opened and in
channel status. The token is checkpoint metadata, not a deduplication key.
Give each stable channel name one owner and retain source events until its
committed progress is confirmed.
"""

import os
import random
import time
from dataclasses import dataclass
from typing import Optional

from snowflake.ingest import streaming

from elastic_step1_quickstart import connection_properties


MAX_ATTEMPTS = 6
MAX_NO_PROGRESS_SECONDS = 30 * 60.0
MAX_PENDING_EVENTS = 100_000
POLL_SECONDS = 1.0
INVALIDATION_ERRORS = {
    "InvalidChannelError",
    "InvalidClientError",
    "ClosedChannelError",
    "ClosedElasticChannelError",
    "ClosedClientError",
}
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def create_client() -> streaming.StreamingIngestClient:
    properties = connection_properties()
    return streaming.StreamingIngestClient.from_table(
        client_name=f"recovery-{os.getpid()}",
        db_name=os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE"),
        schema_name=os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
        table_name=os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE"),
        profile_json=None if properties else os.environ.get("SNOWFLAKE_PROFILE", "profile.json"),
        properties=properties,
    )


@dataclass(frozen=True)
class Event:
    offset: int
    row: dict[str, object]


class SampleEventSource:
    """Regenerable sample data with an in-memory, non-durable checkpoint."""

    def __init__(self, total: int = 10_000, checkpoint: int = 0) -> None:
        if not 0 <= checkpoint <= total:
            raise ValueError("Require 0 <= checkpoint <= total")
        self.total = total
        self.committed = checkpoint
        self.next_offset = checkpoint + 1

    def read(self) -> Optional[Event]:
        if self.next_offset > self.total:
            return None
        offset = self.next_offset
        self.next_offset += 1
        return Event(
            offset,
            {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"},
        )

    def acknowledge(self, offset: int) -> None:
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed: int) -> None:
        self.acknowledge(committed)
        self.next_offset = committed + 1


def is_invalidation(error: streaming.StreamingIngestError) -> bool:
    return error.error_code.value in INVALIDATION_ERRORS


def is_retryable(error: BaseException) -> bool:
    return isinstance(error, streaming.StreamingIngestError) and (
        is_invalidation(error) or error.http_status_code in TRANSIENT_STATUS_CODES
    )


def remaining(deadline: float) -> float:
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError(
            "No confirmed progress before the deadline; retain unconfirmed source events"
        )
    return seconds


def backoff(attempt: int, deadline: float) -> None:
    cap = min(10.0, 0.25 * 2 ** min(attempt, 6))
    time.sleep(min(random.uniform(0, cap), remaining(deadline)))


CHANNEL_NAME = os.environ.get("SNOWFLAKE_CHANNEL", "production-source-0")
CHECKPOINT_ROWS = 1_000
CHECKPOINT_SECONDS = 5.0


# Ingestion and status polling

def main() -> None:
    source = SampleEventSource(
        total=int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000"))
    )
    producer = NamedProducer()
    completed = False
    try:
        run(producer, source)
        completed = True
        print(f"Committed source checkpoint: {source.committed}")
    finally:
        if not completed:
            print(f"Retain source events after checkpoint {source.committed}")
        producer.close(completed)


def run(producer: "NamedProducer", source: SampleEventSource) -> None:
    """Append continuously and periodically checkpoint committed progress."""
    # Snowflake's committed token determines where this retained source resumes.
    source.seek(producer.open())
    submitted = source.committed
    event = None
    exhausted = False
    failures = 0
    rows_since_poll = 0
    next_poll = time.monotonic() + CHECKPOINT_SECONDS
    deadline = time.monotonic() + MAX_NO_PROGRESS_SECONDS

    while True:
        try:
            outstanding = submitted > source.committed
            should_poll = outstanding and (
                exhausted
                or event is not None
                or rows_since_poll >= CHECKPOINT_ROWS
                or time.monotonic() >= next_poll
                or submitted - source.committed >= MAX_PENDING_EVENTS
            )
            if should_poll:
                previous = source.committed
                # One status call can confirm a partial prefix; it does not
                # drain or wait for every submitted row.
                collect_progress(producer, submitted, source)
                if source.committed > previous:
                    deadline = time.monotonic() + MAX_NO_PROGRESS_SECONDS
                    failures = 0
                rows_since_poll = 0
                next_poll = time.monotonic() + CHECKPOINT_SECONDS

            if submitted == source.committed and event is None:
                deadline = time.monotonic() + MAX_NO_PROGRESS_SECONDS
                if exhausted:
                    return

            remaining(deadline)
            if exhausted or submitted - source.committed >= MAX_PENDING_EVENTS:
                time.sleep(min(POLL_SECONDS, remaining(deadline)))
                continue

            if event is None:
                event = source.read()
            if event is None:
                exhausted = True
                continue

            producer.channel.append_row(event.row, str(event.offset))
            submitted = event.offset
            rows_since_poll += 1
            event = None

        except streaming.StreamingIngestError as error:
            if not is_retryable(error):
                raise
            if error.http_status_code != 429:
                failures += 1
                if failures >= MAX_ATTEMPTS:
                    raise
            if is_invalidation(error):
                previous = source.committed
                source.seek(producer.recover(error))
                if source.committed > previous:
                    deadline = time.monotonic() + MAX_NO_PROGRESS_SECONDS
                submitted = source.committed
                event = None
                exhausted = False
            backoff(2, deadline)


# Committed offset handling

def collect_progress(producer: "NamedProducer", submitted: int, source: SampleEventSource) -> None:
    """Fetch status once and checkpoint the confirmed source prefix."""
    status = producer.channel.get_channel_status()
    if status.rows_error_count:
        raise RuntimeError("Row errors require reconciliation before source handoff")
    if status.status_code != "SUCCESS":
        raise streaming.StreamingIngestError(
            streaming.StreamingIngestErrorCode.INVALID_CHANNEL_ERROR,
            status.status_code,
            409,
            "Conflict",
        )

    committed = min(submitted, parse_offset(status.latest_committed_offset_token))
    if committed > source.committed:
        source.acknowledge(committed)


def parse_offset(token: Optional[str]) -> int:
    """Decode this sample's numeric offset; application tokens may be opaque."""
    return 0 if token is None else int(token)


# Channel lifecycle

class NamedProducer:
    """Own one stable named channel and its client."""

    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.channel = None

    def open(self) -> int:
        if self.client is None:
            self.client = self.factory()
        self.channel, status = self.client.open_channel(CHANNEL_NAME)
        if status.rows_error_count:
            raise RuntimeError("Row errors require reconciliation before source handoff")
        return parse_offset(status.latest_committed_offset_token)

    def recover(self, error: streaming.StreamingIngestError) -> int:
        """Reopen the channel without replacing its committed offset."""
        if error.error_code.value == "InvalidClientError":
            self.close(False)
        elif self.channel is not None:
            try:
                # Close only the local handle. Do not drop the named channel or
                # replace its server-side committed offset.
                self.channel.close(wait_for_flush=False, timeout_seconds=0)
            except streaming.StreamingIngestError:
                pass

        try:
            return self.open()
        except streaming.StreamingIngestError as reopened:
            if reopened.error_code.value not in {
                "InvalidClientError",
                "ClosedClientError",
            }:
                raise
            self.close(False)
            return self.open()

    def close(self, flush: bool) -> None:
        if self.client is None:
            return
        try:
            self.client.close(wait_for_flush=flush, timeout_seconds=30)
        finally:
            self.client = None
            self.channel = None


if __name__ == "__main__":
    main()
