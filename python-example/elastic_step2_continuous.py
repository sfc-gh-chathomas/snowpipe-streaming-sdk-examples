"""Elastic step 2: keep appending while bounding unacknowledged work.

The SDK batches rows for transport. This example submits individual rows,
collects completed acknowledgements without waiting after every append, and
blocks intake only when the application limit is full.

This step does not retry failures or persist source progress. Step 3 adds
those production concerns.
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
    pending[0].result()
    confirmed = 0
    while pending and pending[0].done():
        pending.popleft().result()
        confirmed += 1
    return confirmed


def drain(pending: Deque[Future]) -> int:
    """Wait for all accepted appends."""
    confirmed = 0
    while pending:
        confirmed += wait_and_remove_confirmed_prefix(pending)
    return confirmed


def sample_rows(total: int) -> Iterator[tuple[int, Row]]:
    for event_id in range(1, total + 1):
        yield event_id, {
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
        for event_id, row in sample_rows(total):
            # The append token correlates the acknowledgement; Elastic does not order by it.
            pending.append(channel.append_row_with_wait(row, str(event_id)))
            if len(pending) >= MAX_PENDING_EVENTS:
                confirmed += wait_and_remove_confirmed_prefix(pending)

        confirmed += drain(pending)
        completed = True
        print(f"Durably acknowledged {confirmed} rows")
    finally:
        client.close(wait_for_flush=completed, timeout_seconds=60)


if __name__ == "__main__":
    main()
