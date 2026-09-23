"""Send ten rows through an Elastic channel and wait for durable acknowledgements."""

import uuid

from snowflake.ingest.streaming import StreamingIngestClient

DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
TABLE = "MY_TABLE"


def main():
    """Load profile.json, pipeline sample rows, confirm, and close the client."""
    client = StreamingIngestClient.from_table(
        client_name=f"quickstart-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json="profile.json",
    )
    complete = False
    try:
        channel = client.get_elastic_channel()
        pending = []
        for event_id in range(1, 11):
            row = {"C1": event_id, "C2": str(event_id)}
            pending.append(channel.append_row_with_wait(row, None))
        for acknowledgement in pending:
            acknowledgement.result()
        complete = True
        print("Durably acknowledged 10 rows. Check table contents separately.")
    finally:
        client.close(wait_for_flush=complete, timeout_seconds=30)


if __name__ == "__main__":
    main()
