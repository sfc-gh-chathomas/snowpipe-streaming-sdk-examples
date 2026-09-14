"""Append events immediately; checkpoint all acknowledgements before source handoff.

The SDK owns batching and transport retries. Caller timeouts keep the original
Future alive. Retain/replay unacknowledged source events across restarts; Elastic
replay may duplicate events, so EVENT_ID must be stable and source-unique.
"""

import logging
import time
from dataclasses import dataclass

from snowflake.ingest.streaming import StreamingIngestError
import os
import random
from snowflake.ingest.streaming import StreamingIngestClient

CHECKPOINT_ROWS = 1_000
CHECKPOINT_SECONDS = 5.0
OUTAGE_SECONDS = 300.0
POLL_SECONDS = 1.0
MAX_ATTEMPTS = 6
INVALIDATION = {"InvalidChannelError", "InvalidClientError", "ClosedChannelError",
                "ClosedElasticChannelError", "ClosedClientError"}
TRANSIENT = {408, 429, 500, 502, 503, 504}


def retryable(error):
    return isinstance(error, StreamingIngestError) and (
        error.error_code.value in INVALIDATION or error.http_status_code in TRANSIENT
    )


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("Outage deadline exceeded; source checkpoint unchanged; retain events for replay")
    return seconds


def backoff(attempt, deadline):
    delay = random.uniform(0, min(10.0, 0.25 * 2 ** min(attempt, 6)))
    time.sleep(min(delay, remaining(deadline)))


def create_client():
    properties = None
    if os.environ.get("SNOWFLAKE_PAT"):
        properties = {
            "authorization_type": "PAT",
            "personal_access_token": os.environ["SNOWFLAKE_PAT"],
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "url": os.environ["SNOWFLAKE_URL"],
        }
        if os.environ.get("SNOWFLAKE_ROLE"):
            properties["role"] = os.environ["SNOWFLAKE_ROLE"]
    return StreamingIngestClient.from_table(
        client_name=f"production-{os.getpid()}",
        db_name=os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE"),
        schema_name=os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
        table_name=os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE"),
        profile_json=None if properties else os.environ.get("SNOWFLAKE_PROFILE", "profile.json"),
        properties=properties,
    )


class ReplaySource:
    """Regenerates fixed events after restart; acknowledgement is only in-memory."""

    def __init__(self, total=10_000, checkpoint=0):
        if not 0 <= checkpoint <= total:
            raise ValueError("Require 0 <= source checkpoint <= total")
        self.total = total
        self.committed = checkpoint
        self.next_offset = checkpoint + 1

    def read(self):
        # Replace this deterministic fixture with reads from your retained source.
        if self.next_offset > self.total:
            return None
        offset = self.next_offset
        self.next_offset += 1
        return offset, {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"}

    def acknowledge(self, offset):
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed):
        self.acknowledge(committed)
        self.next_offset = committed + 1


@dataclass
class Pending:
    event: tuple
    future: object
    generation: int


class ElasticProducer:
    """Owns the current SDK client; old retry_attempts must not replace a fresh client."""
    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.generation = 0

    def open(self):
        client = self.factory()
        try:
            self.channel = client.get_elastic_channel()
        except BaseException:
            client.close(wait_for_flush=False, timeout_seconds=0)
            raise
        self.client = client
        self.generation += 1

    def recover(self, generation):
        # Several pending appends can fail from the same old client. Replace it once.
        if generation != self.generation:
            return
        self.close(False)
        self.open()

    def close(self, flush):
        if self.client is not None:
            try:
                self.client.close(wait_for_flush=flush, timeout_seconds=30)
            finally:
                self.client = None


def append_event(producer, event, deadline):
    for attempt in range(MAX_ATTEMPTS):
        remaining(deadline)
        try:
            return Pending(event, producer.channel.append_row_with_wait(event[1], str(event[0])),
                           producer.generation)
        except StreamingIngestError as error:
            if not retryable(error) or attempt == MAX_ATTEMPTS - 1:
                raise
            if error.error_code.value in INVALIDATION:
                producer.recover(producer.generation)
            backoff(attempt, deadline)
    raise RuntimeError("Submission retry budget exhausted")


def confirm_checkpoint(producer, pending, source, deadline):
    for item in pending:
        retries = 0
        while True:
            budget = remaining(deadline)
            try:
                item.future.result(timeout=min(POLL_SECONDS, budget))
                break
            except TimeoutError:
                # A caller timeout neither cancels nor resubmits this append.
                continue
            except StreamingIngestError as error:
                if not retryable(error) or retries >= MAX_ATTEMPTS - 1:
                    raise
                if error.error_code.value in INVALIDATION:
                    producer.recover(item.generation)
                logging.warning("Replaying EVENT_ID=%s after terminal SDK failure; duplicates possible",
                                item.event[0])
                backoff(retries, deadline)
                item = append_event(producer, item.event, deadline)
                retries += 1
    if pending:
        # Every original append succeeded; the source may now retire this window.
        source.acknowledge(pending[-1].event[0])
        pending.clear()


def run(producer, source):
    pending = []
    checkpoint_at = time.monotonic() + CHECKPOINT_SECONDS
    deadline = time.monotonic() + OUTAGE_SECONDS
    while True:
        # Read only while the previous checkpoint has capacity.
        event = source.read()
        if event is None:
            break
        pending.append(append_event(producer, event, deadline))
        failed = any(item.future.done() and item.future.exception() is not None for item in pending)
        if failed or len(pending) >= CHECKPOINT_ROWS or time.monotonic() >= checkpoint_at:
            confirm_checkpoint(producer, pending, source, deadline)
            checkpoint_at = time.monotonic() + CHECKPOINT_SECONDS
            deadline = time.monotonic() + OUTAGE_SECONDS
    confirm_checkpoint(producer, pending, source, deadline)


def main():
    source = ReplaySource(
        int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000")),
        int(os.environ.get("SNOWFLAKE_SOURCE_CHECKPOINT", "0")),
    )
    producer = ElasticProducer()
    completed = False
    try:
        producer.open()
        run(producer, source)
        completed = True
        print(f"Durable source checkpoint: {source.committed}; materialization is separate")
    finally:
        if not completed:
            print(f"Stopped. Retain source events after checkpoint {source.committed} for replay")
        producer.close(completed)


if __name__ == "__main__":
    main()
