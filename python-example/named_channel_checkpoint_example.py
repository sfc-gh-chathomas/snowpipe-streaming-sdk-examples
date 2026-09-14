"""Single-writer named-channel producer with source-offset recovery.

Stream individual rows and checkpoint committed offsets, never local submission.
Source events remain replayable until confirmed. A caller timeout pauses reading;
only SDK invalidation reopens the channel. Do not share channel ownership.
"""

import os
import time

from snowflake.ingest.streaming import StreamingIngestError, StreamingIngestErrorCode
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


# Start here: create a source, connect, and stream retained events.

def main():
    source = SampleEventSource(
        int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000")),
        int(os.environ.get("SNOWFLAKE_SOURCE_CHECKPOINT", "0")),
    )
    producer = NamedProducer()
    completed = False
    try:
        run(producer, source)
        completed = True
        print(f"Committed source checkpoint: {source.committed}")
    finally:
        if not completed:
            print(f"Stopped. Retain source events after checkpoint {source.committed} for replay")
        producer.close(completed)



def run(producer, source):
    # Snowflake, not local submission, determines the restart position.
    source.seek(producer.open())
    last_submitted_offset = source.committed
    uncommitted_count = 0
    retry_attempts = 0
    deadline = time.monotonic() + OUTAGE_SECONDS
    checkpoint_at = time.monotonic() + CHECKPOINT_SECONDS
    event = None
    while True:
        try:
            if event is None:
                event = source.read()
            if event is None:
                if uncommitted_count:
                    confirm_checkpoint(producer, last_submitted_offset, source, deadline)
                return
            remaining(deadline)
            source_offset, row = event
            # Write immediately; the SDK, not this loop, batches the transport.
            producer.channel.append_row(row, str(source_offset))
            last_submitted_offset = event[0]
            event = None
            uncommitted_count += 1
            if uncommitted_count >= CHECKPOINT_ROWS or time.monotonic() >= checkpoint_at:
                confirm_checkpoint(producer, last_submitted_offset, source, deadline)
                uncommitted_count = 0
                retry_attempts = 0
                deadline = time.monotonic() + OUTAGE_SECONDS
                checkpoint_at = time.monotonic() + CHECKPOINT_SECONDS
        except StreamingIngestError as error:
            retry_attempts += 1
            if not retryable(error) or retry_attempts >= MAX_ATTEMPTS:
                raise
            if error.error_code.value in INVALIDATION:
                source.seek(producer.recover(error))
                last_submitted_offset = source.committed
                uncommitted_count = 0
                event = None
            backoff(retry_attempts - 1, deadline)


# Supporting delivery and connection details.

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


class SampleEventSource:
    """Synthetic input only: no external source and no persisted checkpoint.

    Replace read/acknowledge/seek with your retained source operations.
    """

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
        # Replace this mapping with your target table columns and stable event ID.
        row = {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"}
        return offset, row

    def acknowledge(self, offset):
        # In production, persist/commit source progress here before retiring events.
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed):
        # Position the retained source strictly after Snowflake committed progress.
        self.acknowledge(committed)
        self.next_offset = committed + 1

CHANNEL_NAME = os.environ.get("SNOWFLAKE_CHANNEL", "production-source-0")


def parse_offset(token):
    return 0 if token is None else int(token)


class NamedProducer:
    """Owns one stable channel and preserves its server offset during recovery."""
    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.channel = None

    def open(self):
        if self.client is None:
            self.client = self.factory()
        self.channel, status = self.client.open_channel(CHANNEL_NAME)
        if status.rows_error_count:
            raise RuntimeError("Row errors require reconciliation before source handoff")
        return parse_offset(status.latest_committed_offset_token)

    def recover(self, error):
        if error.error_code.value == "InvalidClientError":
            self.close(False)
        elif self.channel is not None:
            try:
                self.channel.close(wait_for_flush=False, timeout_seconds=0)
            except StreamingIngestError:
                pass
        try:
            return self.open()
        except StreamingIngestError as reopened:
            if reopened.error_code.value not in {"InvalidClientError", "ClosedClientError"}:
                raise
            self.close(False)
            return self.open()

    def close(self, flush):
        if self.client is not None:
            try:
                self.client.close(wait_for_flush=flush, timeout_seconds=30)
            finally:
                self.client = None


def confirm_checkpoint(producer, target, source, deadline):
    while True:
        budget = remaining(deadline)
        try:
            producer.channel.wait_for_commit(
                lambda token: parse_offset(token) >= target,
                timeout_seconds=max(1, min(5, int(budget))),
            )
            status = producer.channel.get_channel_status()
            if status.rows_error_count:
                raise RuntimeError("Row errors require reconciliation before source handoff")
            if status.status_code != "SUCCESS":
                raise StreamingIngestError(StreamingIngestErrorCode.INVALID_CHANNEL_ERROR,
                                           status.status_code, 409, "Conflict")
            source.acknowledge(target)
            return
        except TimeoutError:
            continue
        except StreamingIngestError as error:
            if error.error_code.value in INVALIDATION or not retryable(error):
                raise
            backoff(0, deadline)


if __name__ == "__main__":
    main()
