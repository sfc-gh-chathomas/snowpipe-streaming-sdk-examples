"""Elastic step 2: keep appending while bounding unacknowledged work.

The SDK batches rows for transport. This example submits individual rows,
collects completed acknowledgements without waiting after every append, and
blocks intake only when the application limit is full.

The deque stores Future handles, not rows. Its limit bounds application
acknowledgement bookkeeping independently of the SDK's byte-based buffer.

The SDK retries transient network and service failures internally. This
example does not retry errors that still reach the caller: invalid input,
local backpressure, closed or invalid SDK state, non-retryable API errors, or
exhausted SDK retries. Later row-materialization errors are separate from
append acknowledgement and must be monitored separately. Step 3 adds
application retries and persisted source progress.
"""

from collections import deque
from concurrent.futures import Future
import os
from typing import Deque, Iterator
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest import streaming


MAX_PENDING_EVENTS = 10_000
Row = dict[str, object]
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")
PROFILE = os.environ.get("SNOWFLAKE_PROFILE", "profile.json")


def create_client() -> streaming.StreamingIngestClient:
    return streaming.StreamingIngestClient.from_table(
        client_name=f"continuous-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=PROFILE,
    )


def wait_and_remove_confirmed_prefix(pending: Deque[Future]) -> int:
    """Wait for the oldest append and remove the confirmed submission-order prefix."""
    # An exceptional result is terminal from the SDK's perspective; this step propagates it.
    pending[0].result()
    confirmed = 0
    # One SDK acknowledgement may complete several consecutive append Futures.
    while pending and pending[0].done():
        pending.popleft().result()
        confirmed += 1
    return confirmed


def sample_rows(total: int) -> Iterator[Row]:
    for event_id in range(1, total + 1):
        yield {
            "EVENT_ID": event_id,
            "C1": event_id,
            "C2": f"event-{event_id}",
        }


def main() -> None:
    total = int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000"))
    client = create_client()
    pending = deque()
    confirmed = 0
    completed = False
    try:
        channel = client.get_elastic_channel()
        for row in sample_rows(total):
            # None opts out of callback correlation; the Future identifies this append.
            pending.append(channel.append_row_with_wait(row, None))
            if len(pending) >= MAX_PENDING_EVENTS:
                # Pause source intake until at least one acknowledgement slot is released.
                confirmed += wait_and_remove_confirmed_prefix(pending)

        # End of input: wait until every accepted append is durably acknowledged.
        while pending:
            confirmed += wait_and_remove_confirmed_prefix(pending)
        completed = True
        print(f"Durably acknowledged {confirmed} rows")
    finally:
        # Success already drained every Future; after failure, close without waiting again.
        client.close(wait_for_flush=completed, timeout_seconds=60)


if __name__ == "__main__":
    main()
