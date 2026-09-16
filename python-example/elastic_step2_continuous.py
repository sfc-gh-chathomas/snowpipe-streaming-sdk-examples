"""Elastic step 2: let the SDK buffer continuous appends.

The producer keeps appending while the SDK accepts work. A synchronous HTTP
429 means the current event was not accepted, so this pull-based source pauses
and retries that event before reading the next one.

Acknowledgement Futures are retained only to observe delivery outcomes. This
step does not retry asynchronous failures or persist source progress. Step 3
adds those production concerns.
"""

from concurrent.futures import Future
import os
import time
from typing import Iterable, Iterator
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest import streaming


BACKPRESSURE_RETRY_SECONDS = 0.1
COMPLETION_CHECK_INTERVAL = 1_000
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


def collect_completed(pending: list[Future]) -> int:
    """Remove completed Futures and surface asynchronous failures."""
    unfinished = []
    confirmed = 0
    for future in pending:
        if future.done():
            future.result()
            confirmed += 1
        else:
            unfinished.append(future)
    pending[:] = unfinished
    return confirmed


def run(
    channel: streaming.StreamingIngestElasticChannel, rows: Iterable[tuple[int, Row]]
) -> int:
    pending = []
    confirmed = 0

    try:
        for submitted, (event_id, row) in enumerate(rows, start=1):
            while True:
                try:
                    # The token correlates the acknowledgement; Elastic does not order by it.
                    future = channel.append_row_with_wait(row, str(event_id))
                    pending.append(future)
                    break
                except streaming.StreamingIngestError as error:
                    if error.http_status_code != 429:
                        raise
                    # The SDK did not accept this event. Pause intake and retry it unchanged.
                    confirmed += collect_completed(pending)
                    time.sleep(BACKPRESSURE_RETRY_SECONDS)

            if submitted % COMPLETION_CHECK_INTERVAL == 0:
                confirmed += collect_completed(pending)

        for future in pending:
            future.result()
            confirmed += 1
        return confirmed
    except Exception:
        print(f"Durable acknowledgements before failure: at least {confirmed}")
        raise


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
    completed = False
    try:
        confirmed = run(client.get_elastic_channel(), sample_rows(total))
        completed = True
        print(f"Durably acknowledged {confirmed} rows")
    finally:
        client.close(wait_for_flush=completed, timeout_seconds=60)


if __name__ == "__main__":
    main()
