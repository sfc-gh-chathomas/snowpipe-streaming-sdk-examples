"""Elastic step 1: append one row and wait for durability.

Use this example to verify authentication and table configuration before
building a continuous producer.
"""

import os
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest.streaming import StreamingIngestClient


DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")
PROFILE = os.environ.get("SNOWFLAKE_PROFILE", "profile.json")


def create_client() -> StreamingIngestClient:
    return StreamingIngestClient.from_table(
        client_name=f"quickstart-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=PROFILE,
    )


def main() -> None:
    client = create_client()
    try:
        # Elastic Channels belong to their client and are not closed separately.
        channel = client.get_elastic_channel()
        row = {
            "DATA": {"event_id": 1, "status": "active"},
            "C1": 1,
            "C2": "example",
        }
        # The Future completes when Snowflake durably accepts this append.
        channel.append_row_with_wait(row, "event-1").result(timeout=60)
        print("Row durably acknowledged")
    finally:
        client.close(wait_for_flush=True, timeout_seconds=60)


if __name__ == "__main__":
    main()
