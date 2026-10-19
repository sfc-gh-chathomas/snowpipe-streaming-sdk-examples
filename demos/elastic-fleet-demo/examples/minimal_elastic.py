"""Minimal Snowpipe Streaming Elastic channel producer (SDK 1.8.1).

One client, one Elastic channel, fire-and-forget appends, acknowledgements on handlers.
No channel open/close, no offset tokens: every client appends to the same ELASTIC channel.

    pip install snowpipe-streaming==1.8.1
    export SNOWFLAKE_ACCOUNT=<org-account> SNOWFLAKE_USER=<user>   # see README
    python examples/minimal_elastic.py        # prompts for a PAT; nothing is stored
"""

import getpass
import os
import threading
import uuid
from datetime import datetime, timezone

from snowflake.ingest.streaming import StreamingIngestClient

ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]  # account identifier, e.g. myorg-myaccount
DATABASE = os.environ.get("ELASTIC_DEMO_DATABASE", "ELASTIC_DEMO_DB")
SCHEMA = os.environ.get("ELASTIC_DEMO_SCHEMA", "FLEET")

acked, failed, lock = [0], [0], threading.Lock()


def on_success(detail):  # runs on the SDK ack pool: bookkeeping only
    with lock:
        acked[0] += len(detail.append_tokens)


def on_error(detail):  # request_id None = SDK-local failure; rows may still have landed
    with lock:
        failed[0] += len(detail.append_tokens)
    print(f"append failed (request {detail.request_id}): {detail.error}")


def main():
    props = {
        "account": ACCOUNT,
        "user": os.environ["SNOWFLAKE_USER"],
        "url": os.environ.get("SNOWFLAKE_URL", f"https://{ACCOUNT.lower()}.snowflakecomputing.com"),
        "authorization_type": "PAT",
        "personal_access_token": getpass.getpass("PAT (hidden): "),
    }
    client = StreamingIngestClient.from_table(
        "elastic-minimal", DATABASE, SCHEMA, "ROUTER_TELEMETRY",
        properties=props)
    channel = client.get_elastic_channel()  # always named ELASTIC
    channel.set_success_handler(on_success)
    channel.set_error_handler(on_error)

    run_id = str(uuid.uuid4())
    try:
        for seq in range(10):
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            row = {
                "EVENT_ID": f"{run_id}:1:RTR-001-01:{seq}", "RUN_ID": run_id,
                "CLIENT_ID": 1, "DEVICE_ID": "RTR-001-01", "REGION": "us-west",
                "SEQUENCE": seq, "EVENT_TS": ts, "TEMPERATURE_C": 41.5,
                "CPU_PCT": 22.0, "TRAFFIC_MBPS": 180.0, "SCENARIO": "normal",
            }
            channel.append_rows([row], append_token=seq)  # stable EVENT_ID, small token
        channel.wait_for_flush(timeout_seconds=60)
    finally:
        client.close(wait_for_flush=True, timeout_seconds=60)
    print(f"run {run_id}: acknowledged {acked[0]} appends, failed {failed[0]}")


if __name__ == "__main__":
    main()
