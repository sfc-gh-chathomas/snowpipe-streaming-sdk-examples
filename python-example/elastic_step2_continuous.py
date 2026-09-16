"""Elastic step 2: let the SDK buffer continuous appends.

``append_row`` returns after the SDK accepts an event without waiting for its
acknowledgement. If the SDK input buffer or memory threshold is full, the call
raises a backpressure error before accepting the event. This pull-based source
pauses and retries that event before reading the next one.

Success and error callbacks only enqueue metrics because they run on an SDK
acknowledgement thread. The main thread logs those metrics after flushing.
Step 3 adds acknowledgement Futures and recovery.
"""

from dataclasses import dataclass
import logging
import os
from queue import Empty, SimpleQueue
import time
from typing import Iterator
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest import streaming


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class AppendToken:
    event_id: int
    submitted_at: float


def create_client() -> streaming.StreamingIngestClient:
    return streaming.StreamingIngestClient.from_table(
        client_name=f"continuous-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=PROFILE,
    )


def log_completion_stats(accepted: int, successes: SimpleQueue, error_counts: SimpleQueue) -> None:
    latencies_ms = []
    while True:
        try:
            acknowledged_at, tokens = successes.get_nowait()
        except Empty:
            break
        latencies_ms.extend(
            (acknowledged_at - token.submitted_at) * 1_000 for token in tokens
        )

    errors = 0
    while True:
        try:
            errors += error_counts.get_nowait()
        except Empty:
            break

    acknowledged = len(latencies_ms)
    unreported = accepted - acknowledged - errors
    average_ms = sum(latencies_ms) / acknowledged if acknowledged else 0.0
    maximum_ms = max(latencies_ms, default=0.0)
    logger.info(
        "Completion stats: accepted=%d acknowledged=%d errors=%d unreported=%d "
        "average_ack_latency_ms=%.1f max_ack_latency_ms=%.1f",
        accepted,
        acknowledged,
        errors,
        unreported,
        average_ms,
        maximum_ms,
    )


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
    successes = SimpleQueue()
    error_counts = SimpleQueue()
    accepted = 0

    def record_success(detail: streaming.SuccessDetail) -> None:
        successes.put((time.monotonic(), detail.append_tokens))

    def record_error(detail: streaming.ErrorDetail) -> None:
        error_counts.put(len(detail.append_tokens))

    try:
        channel = client.get_elastic_channel()
        channel.set_success_handler(record_success)
        channel.set_error_handler(record_error)

        for event_id, row in sample_rows(total):
            while True:
                try:
                    # This call only enqueues; the SDK handles batching and delivery.
                    token = AppendToken(event_id, time.monotonic())
                    channel.append_row(row, token)
                    accepted += 1
                    break
                except streaming.StreamingIngestError as error:
                    if error.error_code not in SDK_BACKPRESSURE_ERRORS:
                        raise
                    # Backpressure means this event was not accepted. Retry it unchanged.
                    time.sleep(BACKPRESSURE_RETRY_SECONDS)
    finally:
        try:
            # Flush accepted events and wait for their callbacks before reading the queues.
            client.close(wait_for_flush=True, timeout_seconds=60)
        finally:
            log_completion_stats(accepted, successes, error_counts)


if __name__ == "__main__":
    main()
