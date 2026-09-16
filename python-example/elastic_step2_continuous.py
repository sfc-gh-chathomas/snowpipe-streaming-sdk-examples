"""Elastic step 2: let the SDK buffer continuous appends.

``append_row`` returns after the SDK accepts an event without waiting for its
acknowledgement. If the SDK input buffer or memory threshold is full, the call
raises a backpressure error before accepting the event. This pull-based source
pauses and retries that event before reading the next one.

This step handles synchronous flow control but does not track asynchronous
delivery outcomes. Step 3 adds acknowledgement Futures and recovery.
"""

import os
import time
from typing import Iterable, Iterator
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest import streaming


BACKPRESSURE_RETRY_SECONDS = 0.1
SDK_BACKPRESSURE_ERRORS = {
    streaming.StreamingIngestErrorCode.RECEIVER_SATURATED,
    streaming.StreamingIngestErrorCode.MEMORY_THRESHOLD_EXCEEDED,
    streaming.StreamingIngestErrorCode.MEMORY_THRESHOLD_EXCEEDED_IN_CONTAINER,
}
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


def run(
    channel: streaming.StreamingIngestElasticChannel, rows: Iterable[tuple[int, Row]]
) -> int:
    accepted = 0
    for event_id, row in rows:
        while True:
            try:
                # This call only enqueues; the SDK handles batching and delivery.
                channel.append_row(row, str(event_id))
                accepted += 1
                break
            except streaming.StreamingIngestError as error:
                if error.error_code not in SDK_BACKPRESSURE_ERRORS:
                    raise
                # Backpressure means this event was not accepted. Retry it unchanged.
                time.sleep(BACKPRESSURE_RETRY_SECONDS)
    return accepted


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
    try:
        accepted = run(client.get_elastic_channel(), sample_rows(total))
    finally:
        # Flush every event accepted before normal completion or an exception.
        client.close(wait_for_flush=True, timeout_seconds=60)
    print(f"Submitted {accepted} rows; flush complete")


if __name__ == "__main__":
    main()
