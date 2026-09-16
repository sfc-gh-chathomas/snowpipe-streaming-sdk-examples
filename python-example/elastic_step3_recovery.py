"""Elastic step 3: retain source events until acknowledgements confirm progress.

This adds source checkpointing, bounded retry, and client recovery to the
continuous example. Replaying an Elastic append can create a duplicate, so a
real source must retain events and provide a stable event ID.
"""

from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
import os
import random
import time
from typing import Optional

from snowflake.ingest.streaming import StreamingIngestClient, StreamingIngestError


MAX_PENDING_EVENTS = 100_000
MAX_NO_PROGRESS_SECONDS = 30 * 60.0
POLL_SECONDS = 1.0
MAX_ATTEMPTS = 6
INVALIDATION_ERRORS = {
    "InvalidChannelError",
    "InvalidClientError",
    "ClosedChannelError",
    "ClosedElasticChannelError",
    "ClosedClientError",
}
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}


# Ingestion and checkpointing

def main() -> None:
    source = SampleEventSource(
        total=int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000")),
        checkpoint=int(os.environ.get("SNOWFLAKE_SOURCE_CHECKPOINT", "0")),
    )
    producer = ElasticProducer()
    completed = False
    try:
        producer.open()
        run(producer, source)
        completed = True
        print(f"Durable source checkpoint: {source.committed}")
    finally:
        if not completed:
            print(f"Retain source events after checkpoint {source.committed}")
        producer.close(completed)


def run(producer: "ElasticProducer", source: "SampleEventSource") -> None:
    """Submit while capacity is available and checkpoint confirmed prefixes."""
    pending = []
    exhausted = False
    deadline = None

    while True:
        if pending:
            previous = source.committed
            # Poll without waiting while there is capacity; wait only at the
            # safety limit or after the source reaches its end.
            must_wait = exhausted or len(pending) >= MAX_PENDING_EVENTS
            collect_progress(producer, pending, source, deadline, wait=must_wait)
            if source.committed > previous:
                # Successful submission is not progress; a durable source
                # checkpoint is what resets the outage deadline.
                deadline = time.monotonic() + MAX_NO_PROGRESS_SECONDS

        if exhausted and not pending:
            return

        if pending:
            remaining(deadline)
        else:
            deadline = None

        if exhausted or len(pending) >= MAX_PENDING_EVENTS:
            continue

        event = source.read()
        if event is None:
            exhausted = True
            continue

        if deadline is None:
            deadline = time.monotonic() + MAX_NO_PROGRESS_SECONDS
        # The source still owns this event until collect_progress checkpoints it.
        pending.append(append_event(producer, event, deadline))


def collect_progress(producer, pending: list["Pending"], source, deadline: float, wait: bool = False) -> None:
    """Checkpoint only the completed prefix; keep unfinished appends alive."""
    if not pending:
        return

    # Later appends may finish first, but only the oldest one can extend the
    # contiguous source checkpoint.
    first = pending[0]
    if wait and not first.future.done():
        try:
            first.future.result(timeout=min(POLL_SECONDS, remaining(deadline)))
        except FutureTimeoutError:
            return

    confirmed = 0
    for item in pending:
        if not item.future.done():
            break
        try:
            item.future.result()
        except StreamingIngestError as error:
            if confirmed:
                # Commit the successful prefix before handling the failed append.
                break
            if not retryable(error) or item.retries >= MAX_ATTEMPTS - 1:
                raise
            if invalidation(error):
                producer.swap_client(item.client)
            backoff(item.retries, deadline)
            # Retry the same retained event on whichever client is active now.
            pending[0] = append_event(
                producer, item.event, deadline, retries=item.retries + 1
            )
            return
        confirmed += 1

    if confirmed:
        source.acknowledge(pending[confirmed - 1].event.offset)
        del pending[:confirmed]


def append_event(producer: "ElasticProducer", event: "Event", deadline: float, retries: int = 0) -> "Pending":
    """Submit one retained event, retrying immediate transient failures."""
    attempt = retries
    while True:
        remaining(deadline)
        try:
            future = producer.channel.append_row_with_wait(
                event.row, str(event.offset)
            )
            return Pending(event, future, producer.client, attempt)
        except StreamingIngestError as error:
            if error.http_status_code == 429:
                # An immediate 429 means the SDK rejected this append, so keep
                # the event and wait for capacity without spending a retry.
                backoff(2, deadline)
                continue
            if not retryable(error) or attempt >= MAX_ATTEMPTS - 1:
                raise
            if invalidation(error):
                producer.swap_client(producer.client)
            backoff(attempt, deadline)
            attempt += 1


# Retry policy

def invalidation(error: StreamingIngestError) -> bool:
    return error.error_code.value in INVALIDATION_ERRORS


def retryable(error: BaseException) -> bool:
    return isinstance(error, StreamingIngestError) and (
        invalidation(error) or error.http_status_code in TRANSIENT_STATUS_CODES
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


# Connection and sample source

def create_client() -> StreamingIngestClient:
    return StreamingIngestClient.from_table(
        client_name=f"recovery-{os.getpid()}",
        db_name=os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE"),
        schema_name=os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
        table_name=os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE"),
        profile_json=os.environ.get("SNOWFLAKE_PROFILE", "profile.json"),
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
        """Replace this with a non-destructive read from the retained source."""
        if self.next_offset > self.total:
            return None
        offset = self.next_offset
        self.next_offset += 1
        return Event(
            offset,
            {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"},
        )

    def acknowledge(self, offset: int) -> None:
        """Replace this with the source's durable checkpoint operation."""
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed: int) -> None:
        self.acknowledge(committed)
        self.next_offset = committed + 1


@dataclass
class Pending:
    event: Event
    future: Future
    client: object
    retries: int = 0


# Client lifecycle

class ElasticProducer:
    """Own the active client and its Elastic Channel."""

    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.channel = None

    def open(self) -> None:
        client = self.factory()
        try:
            channel = client.get_elastic_channel()
        except BaseException:
            client.close(wait_for_flush=False, timeout_seconds=0)
            raise
        self.client = client
        self.channel = channel

    def swap_client(self, failed_client: object) -> None:
        """Replace the active client unless this failure came from an old one."""
        # Several old Futures can report the same invalid client after a swap.
        if failed_client is not self.client:
            return
        self.close(False)
        self.open()

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
