"""Level 1: pipelined first ingest. SDK batching is automatic; source retention remains your responsibility."""

import os
import time
import uuid
from typing import Optional

os.environ.setdefault("SS_LOG_LEVEL", "warn")
from snowflake.ingest import streaming

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")
PROFILE = os.environ.get("SNOWFLAKE_PROFILE", "profile.json")
RUN_ID = os.environ.get("SNOWFLAKE_RUN_ID", str(uuid.uuid4()))
Row = dict[str, object]

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
        client_name=f"ingest-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=None if properties else PROFILE,
        properties=properties,
    )


def sample_row(event_id: int) -> Row:
    return {
        "EVENT_ID": event_id,
        "C1": event_id,
        "C2": f"{RUN_ID}-{event_id}",
    }


def main():
    """Pipeline ten rows, observe every acknowledgement, then close."""
    client = create_client()
    complete = False
    try:
        channel = client.get_elastic_channel()
        # Submit first: waiting after each append would serialize ingestion.
        pending = [channel.append_row_with_wait(sample_row(event_id), None)
                   for event_id in range(10)]
        for acknowledgement in pending:
            acknowledgement.result()
        complete = True
        print(f"Durably acknowledged 10 rows; run={RUN_ID}. Check materialization separately.")
    finally:
        client.close(wait_for_flush=complete, timeout_seconds=30)


if __name__ == "__main__":
    main()
