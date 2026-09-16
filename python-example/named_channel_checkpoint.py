"""Coordinate retained source offsets with a named channel.

This is an alternative to the Elastic progression for integrations that need
a stable channel identity and source-offset checkpointing.

A named channel reports its latest committed offset token when opened and in
channel status. The token is checkpoint metadata, not a deduplication key.
Give each stable channel name one owner and retain source events until its
committed progress is confirmed.
"""

import os
import time
from typing import Optional

from snowflake.ingest import streaming

from elastic_step3_production import (
    MAX_ATTEMPTS,
    MAX_NO_PROGRESS_SECONDS,
    MAX_PENDING_EVENTS,
    POLL_SECONDS,
    SampleEventSource,
    backoff,
    create_client,
    is_invalidation,
    remaining,
    is_retryable,
)


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
