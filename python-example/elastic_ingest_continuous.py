"""Level 2: continuous Futures with in-process recovery. SDK batching is automatic; source retention remains your responsibility."""

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


MAX_PENDING = 1_000
STALL_SECONDS = 30 * 60
MAX_RECOVERIES = 6


def main():
    """Stream generated rows; pause on capacity, drain on interrupt, recover in process."""
    import signal

    total = int(os.environ.get("SNOWFLAKE_TEST_ROWS", "5000"))
    if total < 0:
        raise ValueError("SNOWFLAKE_TEST_ROWS must be nonnegative")
    stopping = False
    def stop_intake(signum, frame):
        nonlocal stopping
        stopping = True
    previous_signals = {sig: signal.signal(sig, stop_intake) for sig in (signal.SIGINT, signal.SIGTERM)}
    client = None
    pending = {}
    next_id = 0
    confirmed = 0
    attempts = 0
    generation = 0
    deadline = time.monotonic() + STALL_SECONDS
    complete = False
    waiting_for_capacity = False

    try:
        client = create_client()
        channel = client.get_elastic_channel()

        while next_id < total and not stopping or pending:
            try:

                # Observe outcomes without waiting for an entire submission window.
                progress = False
                for event_id, acknowledgement in list(pending.items()):
                    if not acknowledgement.done():
                        continue
                    acknowledgement.result()
                    del pending[event_id]
                    confirmed += 1
                    progress = True
                if progress or (not pending and not waiting_for_capacity):
                    deadline = time.monotonic() + STALL_SECONDS
                if time.monotonic() >= deadline:
                    raise TimeoutError("No durable progress for 30 minutes; retain unresolved source events")
                if next_id < total and not stopping and len(pending) < MAX_PENDING:
                    # Replace sample_row with a retained source read and row mapping.
                    event_id = next_id
                    pending[event_id] = channel.append_row_with_wait(sample_row(event_id), None)
                    next_id += 1
                    waiting_for_capacity = False
                else:
                    time.sleep(0.01)
            except streaming.StreamingIngestError as error:
                if error.http_status_code == 429:
                    # A terminally rejected append may be retried; untouched Futures stay pending.
                    for event_id, acknowledgement in list(pending.items()):
                        if acknowledgement.done() and acknowledgement.exception() is error:
                            try:
                                pending[event_id] = channel.append_row_with_wait(sample_row(event_id), None)
                            except streaming.StreamingIngestError as retry:
                                if retry.http_status_code != 429:
                                    raise
                    waiting_for_capacity = True
                    # Rejected append retains next_id; accepted work keeps its original outcome.
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Backpressure persisted for 30 minutes") from error
                    time.sleep(0.25)
                    continue
                invalid = error.error_code.value in {
                    "InvalidChannelError", "InvalidClientError", "ClosedClientError", "ClosedElasticChannelError"
                }
                if not invalid or attempts >= MAX_RECOVERIES:
                    raise
                attempts += 1
                # First keep successes already observed; unknown outcomes may duplicate on replay.
                for event_id, acknowledgement in list(pending.items()):
                    if acknowledgement.done() and acknowledgement.exception() is None:
                        del pending[event_id]
                        confirmed += 1
                try:
                    client.close(wait_for_flush=False, timeout_seconds=30)
                except streaming.StreamingIngestError as closing:
                    if closing.error_code.value not in {"InvalidClientError", "ClosedClientError", "ClosedElasticChannelError"}:
                        raise
                client = None
                generation += 1
                client = create_client()
                channel = client.get_elastic_channel()

                # Regenerate this sample's unresolved rows. A real source must retain them.
                replay = list(pending)
                pending.clear()
                print(f"Recreated client; replaying {len(replay)} unresolved rows; duplicates possible")
                for event_id in replay:
                    while True:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Recovery exceeded stalled-progress budget")
                        try:
                            pending[event_id] = channel.append_row_with_wait(sample_row(event_id), None)
                            break
                        except streaming.StreamingIngestError as retry:
                            if retry.http_status_code != 429:
                                raise
                            time.sleep(0.25)
        complete = True
        print(f"Durably acknowledged {confirmed} rows; submitted={next_id}; run={RUN_ID}; stopped={stopping}")
    finally:
        for sig, handler in previous_signals.items():
            signal.signal(sig, handler)
        if not complete:
            print(f"Stopped with unconfirmed work; confirmed={confirmed}, submitted={next_id}; retain source for replay")
        if client is not None:
            client.close(wait_for_flush=complete, timeout_seconds=30)


if __name__ == "__main__":
    main()
