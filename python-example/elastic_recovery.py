"""3. Retain source events until Elastic acknowledgements confirm progress.

This adds source checkpointing, bounded retry, and client recovery to the
continuous example. Replaying an Elastic append can create a duplicate, so a
real source must retain events and provide a stable event ID.
"""

from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
import os
import random
import time

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


def main():
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


def run(producer, source):
    """Submit while capacity is available and checkpoint confirmed prefixes."""
    pending = []
    exhausted = False
    deadline = None

    while True:
        if pending:
            previous = source.committed
            must_wait = exhausted or len(pending) >= MAX_PENDING_EVENTS
            collect_progress(producer, pending, source, deadline, wait=must_wait)
            if source.committed > previous:
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
        pending.append(append_event(producer, event, deadline))


def collect_progress(producer, pending, source, deadline, wait=False):
    """Checkpoint only the completed prefix; keep unfinished appends alive."""
    if not pending:
        return

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
                break
            if not retryable(error) or item.retries >= MAX_ATTEMPTS - 1:
                raise
            if invalidation(error):
                producer.swap_client(item.client)
            backoff(item.retries, deadline)
            pending[0] = append_event(
                producer, item.event, deadline, retries=item.retries + 1
            )
            return
        confirmed += 1

    if confirmed:
        source.acknowledge(pending[confirmed - 1].event.offset)
        del pending[:confirmed]


def append_event(producer, event, deadline, retries=0):
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
                backoff(2, deadline)
                continue
            if not retryable(error) or attempt >= MAX_ATTEMPTS - 1:
                raise
            if invalidation(error):
                producer.swap_client(producer.client)
            backoff(attempt, deadline)
            attempt += 1


def invalidation(error):
    return error.error_code.value in INVALIDATION_ERRORS


def retryable(error):
    return isinstance(error, StreamingIngestError) and (
        invalidation(error) or error.http_status_code in TRANSIENT_STATUS_CODES
    )


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError(
            "No confirmed progress before the deadline; retain unconfirmed source events"
        )
    return seconds


def backoff(attempt, deadline):
    cap = min(10.0, 0.25 * 2 ** min(attempt, 6))
    time.sleep(min(random.uniform(0, cap), remaining(deadline)))


def create_client():
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
    row: dict


class SampleEventSource:
    """Regenerable sample data with an in-memory, non-durable checkpoint."""

    def __init__(self, total=10_000, checkpoint=0):
        if not 0 <= checkpoint <= total:
            raise ValueError("Require 0 <= checkpoint <= total")
        self.total = total
        self.committed = checkpoint
        self.next_offset = checkpoint + 1

    def read(self):
        """Replace this with a non-destructive read from the retained source."""
        if self.next_offset > self.total:
            return None
        offset = self.next_offset
        self.next_offset += 1
        return Event(
            offset,
            {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"},
        )

    def acknowledge(self, offset):
        """Replace this with the source's durable checkpoint operation."""
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed):
        self.acknowledge(committed)
        self.next_offset = committed + 1


@dataclass
class Pending:
    event: Event
    future: object
    client: object
    retries: int = 0


class ElasticProducer:
    """Own the active client and its Elastic Channel."""

    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.channel = None

    def open(self):
        client = self.factory()
        try:
            channel = client.get_elastic_channel()
        except BaseException:
            client.close(wait_for_flush=False, timeout_seconds=0)
            raise
        self.client = client
        self.channel = channel

    def swap_client(self, failed_client):
        """Replace the active client unless this failure came from an old one."""
        if failed_client is not self.client:
            return
        self.close(False)
        self.open()

    def close(self, flush):
        if self.client is None:
            return
        try:
            self.client.close(wait_for_flush=flush, timeout_seconds=30)
        finally:
            self.client = None
            self.channel = None


if __name__ == "__main__":
    main()
