"""Shared configuration and a deterministic replay fixture, not a durable queue."""

import os
import random
import time

from snowflake.ingest.streaming import StreamingIngestClient, StreamingIngestError

CHECKPOINT_ROWS = 1_000
CHECKPOINT_SECONDS = 5.0
OUTAGE_SECONDS = 300.0
POLL_SECONDS = 1.0
MAX_ATTEMPTS = 6
INVALIDATION = {"InvalidChannelError", "InvalidClientError", "ClosedChannelError",
                "ClosedElasticChannelError", "ClosedClientError"}
TRANSIENT = {408, 429, 500, 502, 503, 504}


def code(error):
    return error.error_code.value if isinstance(error, StreamingIngestError) else None


def retryable(error):
    return isinstance(error, StreamingIngestError) and (
        code(error) in INVALIDATION or error.http_status_code in TRANSIENT
    )


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("Outage deadline exceeded; source checkpoint unchanged; retain events for replay")
    return seconds


def backoff(attempt, deadline):
    delay = random.uniform(0, min(10.0, 0.25 * 2 ** min(attempt, 6)))
    time.sleep(min(delay, remaining(deadline)))


def create_client():
    properties = None
    if os.environ.get("SNOWFLAKE_PAT"):
        properties = {
            "authorization_type": "PAT",
            "personal_access_token": os.environ["SNOWFLAKE_PAT"],
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "url": os.environ["SNOWFLAKE_URL"],
        }
        if os.environ.get("SNOWFLAKE_ROLE"):
            properties["role"] = os.environ["SNOWFLAKE_ROLE"]
    return StreamingIngestClient.from_table(
        client_name=f"production-{os.getpid()}",
        db_name=os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE"),
        schema_name=os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
        table_name=os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE"),
        profile_json=None if properties else os.environ.get("SNOWFLAKE_PROFILE", "profile.json"),
        properties=properties,
    )


class ReplaySource:
    """Regenerates fixed events after restart; acknowledgement is only in-memory."""

    def __init__(self, total=10_000, checkpoint=0):
        if not 0 <= checkpoint <= total:
            raise ValueError("Require 0 <= source checkpoint <= total")
        self.total = total
        self.committed = checkpoint
        self.next_offset = checkpoint + 1

    def read(self):
        if self.next_offset > self.total:
            return None
        offset = self.next_offset
        self.next_offset += 1
        return offset, {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"}

    def acknowledge(self, offset):
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed):
        self.acknowledge(committed)
        self.next_offset = committed + 1


def source_from_env():
    return ReplaySource(int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000")),
                        int(os.environ.get("SNOWFLAKE_SOURCE_CHECKPOINT", "0")))
