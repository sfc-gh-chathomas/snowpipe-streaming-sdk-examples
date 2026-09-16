"""Elastic step 2: keep appending while bounding unacknowledged work.

The SDK batches rows for transport. This example submits individual rows,
collects completed acknowledgements without waiting after every append, and
blocks intake only when the application limit is full.

This step does not retry failures or persist source progress. Step 3 adds
those production concerns.
"""

from collections import deque
import os
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest.streaming import StreamingIngestClient


MAX_PENDING_EVENTS = 10_000
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")
PROFILE = os.environ.get("SNOWFLAKE_PROFILE", "profile.json")


def create_client():
    return StreamingIngestClient.from_table(
        client_name=f"continuous-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=PROFILE,
    )


def collect_ready(pending):
    """Remove and count the contiguous prefix of completed appends."""
    confirmed = 0
    while pending and pending[0].done():
        pending.popleft().result()
        confirmed += 1
    return confirmed


def run(channel, rows):
    pending = deque()
    confirmed = 0

    for offset, row in rows:
        # Keep appends pipelined until the application's own safety limit is full.
        if len(pending) >= MAX_PENDING_EVENTS:
            pending[0].result()
            confirmed += collect_ready(pending)

        # Keep the original Future; the SDK handles transport batching internally.
        pending.append(channel.append_row_with_wait(row, str(offset)))
        confirmed += collect_ready(pending)

    # Intake has stopped, so wait for every accepted append before returning.
    while pending:
        pending[0].result()
        confirmed += collect_ready(pending)

    return confirmed


def sample_rows(total):
    for offset in range(1, total + 1):
        yield offset, {
            "EVENT_ID": offset,
            "C1": offset,
            "C2": f"event-{offset}",
        }


def main():
    total = int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000"))
    client = create_client()
    completed = False
    try:
        confirmed = run(client.get_elastic_channel(), sample_rows(total))
        completed = True
        print(f"Durably acknowledged {confirmed} rows")
    finally:
        client.close(wait_for_flush=completed, timeout_seconds=60)


if __name__ == "__main__":
    main()
