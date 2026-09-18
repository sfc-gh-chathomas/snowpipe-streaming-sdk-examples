"""Elastic step 1: append one row and wait for durability.

This minimal example demonstrates authentication and table configuration with
one durably acknowledged row.
"""

import os
from typing import Optional
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest import streaming


DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")
PROFILE = os.environ.get("SNOWFLAKE_PROFILE", "profile.json")


def connection_properties() -> Optional[dict[str, str]]:
    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        return None
    if not os.environ.get("SNOWFLAKE_ACCOUNT") or not os.environ.get("SNOWFLAKE_URL"):
        raise ValueError("PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL")

    properties = {
        "authorization_type": "PAT",
        "personal_access_token": pat,
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "url": os.environ["SNOWFLAKE_URL"],
    }
    if os.environ.get("SNOWFLAKE_ROLE"):
        properties["role"] = os.environ["SNOWFLAKE_ROLE"]
    return properties


def create_client() -> streaming.StreamingIngestClient:
    properties = connection_properties()
    return streaming.StreamingIngestClient.from_table(
        client_name=f"quickstart-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=None if properties else PROFILE,
        properties=properties,
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
