"""
Production-oriented example of streaming rows into an Elastic Channel using
the Snowpipe Streaming REST API directly (no SDK dependency).

The REST API is intended for lightweight or language-agnostic workloads.
Most applications should prefer the SDK (see ../python-example), which
provides higher throughput and simpler error handling. This example exists
to show a robust reference implementation of the REST path, covering:

  - Key-pair JWT generation and ingest-host discovery/scoped-token exchange,
    including proactive + reactive token refresh.
  - NDJSON batching bounded by row count and byte size, with gzip
    compression.
  - Bounded in-flight requests (backpressure on the producer).
  - Retry of 429 / 5xx / ambiguous network failures with capped exponential
    backoff and full jitter, honoring `Retry-After` when present.
  - Fail-fast on permanent 400 / 401 (after one forced token refresh) / 403
    / 404 errors.
  - Reuse of the same `requestId` (and incrementing `retryCount`) across
    retries of the same rowset, plus stable per-row event IDs for downstream
    deduplication of possible duplicates from ambiguous outcomes.
  - Graceful shutdown: stop intake, flush in-flight batches with a timeout.

See the REST getting-started tutorial for the minimal cURL quickstart:
https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-rest-getting-started
"""

from __future__ import annotations

import base64
import concurrent.futures
import gzip
import hashlib
import json
import logging
import os
import random
import re
import signal
import threading
import time
import uuid
from urllib.parse import quote, urlparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
import requests
from cryptography.hazmat.primitives import serialization

logging.basicConfig(
    level=os.environ.get("SS_LOG_LEVEL", "WARN").upper(),
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
log = logging.getLogger("elastic_rest_example")

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

# Replace these with your Snowflake object names, or set via profile.json.
DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
TABLE = "MY_TABLE"
# Optional: set to a custom pipe name to use the pipe endpoint instead of the
# default table endpoint. Leave as None to stream into `<TABLE>-STREAMING`.
PIPE: Optional[str] = None

MAX_ROWS_TO_SEND = 5_000

BATCH_MAX_ROWS = 500
BATCH_MAX_BYTES = 900_000  # conservative uncompressed cap; wire limit is 4 MB
MAX_IN_FLIGHT_BATCHES = 8
MAX_WORKER_THREADS = 4
RETRY_BUDGET_SECONDS = 30 * 60
WIRE_MAX_BYTES = 4_000_000

JWT_LIFETIME = timedelta(minutes=55)
INITIAL_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 30.0
MAX_RETRY_ATTEMPTS = 8
REQUEST_TIMEOUT_SECONDS = 30

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
FAIL_FAST_STATUS_CODES = {400, 403, 404}


# --------------------------------------------------------------------------
# Key-pair JWT + scoped-token acquisition
# --------------------------------------------------------------------------


class TokenProvider:
    """Generates key-pair JWTs and exchanges them for a scoped ingest token.

    Handles proactive refresh (before expiry) and reactive refresh (on a
    401 from the data endpoint), following the flow documented in the
    Elastic Channels REST getting-started tutorial: generate a JWT, discover
    the account-specific ingest host, then exchange the JWT for a scoped
    token bound to that host.
    """

    def __init__(self, account: str, user: str, private_key_file: str, control_host: Optional[str] = None, pat: Optional[str] = None, role: Optional[str] = None):
        self.account = account
        self.user = user
        self.private_key_file = private_key_file
        self.control_host = control_host or f"{account}.snowflakecomputing.com"

        self._pat = pat
        self.role = role
        self._lock = threading.Lock()
        self._private_key = None if pat else self._load_private_key()
        self._jwt: Optional[str] = None
        self._jwt_expiry: Optional[datetime] = None
        self.ingest_host: Optional[str] = None
        self._scoped_token: Optional[str] = None
        self._scoped_token_expiry: Optional[datetime] = None

    def _load_private_key(self):
        passphrase = os.environ.get("PRIVATE_KEY_PASSPHRASE")
        with open(self.private_key_file, "rb") as f:
            key_bytes = f.read()
        return serialization.load_pem_private_key(
            key_bytes,
            password=passphrase.encode("utf-8") if passphrase else None,
        )

    def _public_key_fingerprint(self) -> str:
        public_key_der = self._private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        digest = hashlib.sha256(public_key_der).digest()
        return "SHA256:" + base64.b64encode(digest).decode("utf-8")

    def _generate_jwt(self) -> str:
        account = self.account.upper()
        user = self.user.upper()
        qualified_username = f"{account}.{user}"
        now = datetime.now(timezone.utc)
        payload = {
            "iss": f"{qualified_username}.{self._public_key_fingerprint()}",
            "sub": qualified_username,
            "iat": now,
            "exp": now + JWT_LIFETIME,
        }
        token = jwt.encode(payload, key=self._private_key, algorithm="RS256")
        self._jwt = token
        self._jwt_expiry = now + JWT_LIFETIME
        log.info("Generated new key-pair JWT (expires %s)", self._jwt_expiry.isoformat())
        return token

    def _valid_jwt(self) -> str:
        if self._jwt is None or self._jwt_expiry is None or datetime.now(timezone.utc) >= self._jwt_expiry - timedelta(minutes=2):
            return self._generate_jwt()
        return self._jwt

    def _discover_ingest_host(self, jwt_token: str) -> str:
        resp = requests.get(
            f"https://{self.control_host}/v2/streaming/hostname",
            headers={
                "Authorization": f"Bearer {jwt_token}",
                "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN" if self._pat else "KEYPAIR_JWT",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        resp.raise_for_status()
        # The account name in the ingest hostname must use dashes, not
        # underscores, for all subsequent scoped-token and data calls.
        host = response_value(resp, "hostname").replace("_", "-")
        if not re.fullmatch(r"[A-Za-z0-9.-]+\.snowflakecomputing\.com", host):
            raise ValueError("Unexpected ingest hostname")
        return host

    def _exchange_scoped_token(self, jwt_token: str, ingest_host: str) -> tuple:
        scope = ingest_host + (f" session:role:{self.role}" if self.role else "")
        form = {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "scope": scope}
        if self._pat:
            form = {"grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token_type": "programmatic_access_token", "subject_token": jwt_token,
                    "scope": scope}
        resp = requests.post(
            f"https://{self.control_host}/oauth/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {jwt_token}",
            },
            data=form,
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        resp.raise_for_status()
        scoped_token = response_value(resp, "token")
        # Scoped tokens are bearer-only opaque strings with no published
        # expiry claim; refresh proactively on the same cadence as the JWT
        # that minted them.
        expiry = datetime.now(timezone.utc) + JWT_LIFETIME
        return scoped_token, expiry

    def get_scoped_token(self, force_refresh: bool = False) -> tuple:
        """Returns (ingest_host, scoped_token), refreshing as needed."""
        with self._lock:
            needs_refresh = (
                force_refresh
                or self._scoped_token is None
                or self._scoped_token_expiry is None
                or datetime.now(timezone.utc) >= self._scoped_token_expiry - timedelta(minutes=2)
            )
            if not needs_refresh:
                return self.ingest_host, self._scoped_token

            jwt_token = self._pat or self._valid_jwt()
            if self.ingest_host is None or force_refresh:
                self.ingest_host = self._discover_ingest_host(jwt_token)
                log.info("Discovered ingest host: %s", self.ingest_host)

            self._scoped_token, self._scoped_token_expiry = self._exchange_scoped_token(
                jwt_token, self.ingest_host
            )
            log.info("Obtained scoped ingest token (expires %s)", self._scoped_token_expiry.isoformat())
            return self.ingest_host, self._scoped_token


# --------------------------------------------------------------------------
# Retry / backoff helpers
# --------------------------------------------------------------------------


def compute_backoff_seconds(attempt: int, retry_after: Optional[float] = None) -> float:
    """Capped exponential backoff with full jitter, honoring Retry-After."""
    if retry_after is not None and retry_after >= 0:
        return retry_after
    cap = min(MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS * (2 ** attempt))
    return random.uniform(0, cap)


def parse_retry_after(response: "requests.Response") -> Optional[float]:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime

            retry_at = parsedate_to_datetime(value)
            return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


class PermanentIngestError(Exception):
    """Raised for non-retryable REST errors (400/401/403/404 after refresh)."""


class RetryBudgetExhaustedError(Exception):
    """Raised when a rowset could not be delivered within MAX_RETRY_ATTEMPTS."""


# --------------------------------------------------------------------------
# NDJSON batching + append
# --------------------------------------------------------------------------


@dataclass
class RowBatch:
    rows: list = field(default_factory=list)
    byte_size: int = 0

    def add(self, row_json_bytes: bytes) -> None:
        self.rows.append(row_json_bytes)
        self.byte_size += len(row_json_bytes) + 1  # + newline

    def is_empty(self) -> bool:
        return not self.rows

    def to_ndjson_bytes(self) -> bytes:
        return b"\n".join(self.rows) + b"\n"


class ElasticRestIngestClient:
    """Bounded-concurrency, retrying Elastic Channel REST append client."""

    def __init__(
        self,
        token_provider: TokenProvider,
        database: str,
        schema: str,
        table: str,
        pipe: Optional[str] = None,
        batch_max_rows: int = BATCH_MAX_ROWS,
        batch_max_bytes: int = BATCH_MAX_BYTES,
        max_in_flight: int = MAX_IN_FLIGHT_BATCHES,
        max_workers: int = MAX_WORKER_THREADS,
    ):
        if min(batch_max_rows, batch_max_bytes, max_in_flight, max_workers) <= 0:
            raise ValueError("Batch and concurrency limits must be positive")
        self._tokens = token_provider
        self._database = quote(database, safe="")
        self._schema = quote(schema, safe="")
        self._table = quote(table, safe="")
        self._pipe = quote(pipe, safe="") if pipe else None
        self._batch_max_rows = batch_max_rows
        self._batch_max_bytes = batch_max_bytes

        self._batch_lock = threading.Lock()
        self._current_batch = RowBatch()
        self._in_flight = threading.Semaphore(max_in_flight)
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="elastic-rest-sender"
        )
        self._pending_futures: list = []
        self._pending_lock = threading.Lock()
        self._closed = False
        self._last_flush = time.monotonic()
        self.acknowledged_rows = 0

    def _endpoint_url(self, ingest_host: str) -> str:
        base = (
            f"https://{ingest_host}/v2/streaming/data/databases/{self._database}"
            f"/schemas/{self._schema}"
        )
        if self._pipe:
            return f"{base}/pipes/{self._pipe}/channels/ELASTIC/rows"
        return f"{base}/tables/{self._table}/rows"

    def submit_row(self, row: dict, event_id) -> None:
        """Adds a row to the current batch, flushing it if now full.

        `event_id` should be a stable identifier for the row so that
        downstream consumers can deduplicate possible duplicates caused by
        retries after ambiguous (timeout / 5xx) outcomes.
        """
        if self._closed:
            raise RuntimeError("Cannot submit rows after close() has been called")

        self._collect_completed()
        payload = dict(row)
        payload.setdefault("EVENT_ID", event_id)
        row_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        if len(row_bytes) + 1 > self._batch_max_bytes:
            raise ValueError("Single row exceeds configured uncompressed batch limit")
        with self._batch_lock:
            if (
                not self._current_batch.is_empty()
                and (
                    len(self._current_batch.rows) >= self._batch_max_rows
                    or self._current_batch.byte_size + len(row_bytes) + 1 > self._batch_max_bytes
                    or time.monotonic() - self._last_flush >= 1.0
                )
            ):
                self._flush_locked()
            self._current_batch.add(row_bytes)

    def flush(self) -> None:
        """Sends any buffered rows as a final batch for the current window."""
        with self._batch_lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if self._current_batch.is_empty():
            return
        batch = self._current_batch
        self._current_batch = RowBatch()
        self._dispatch(batch)
        self._last_flush = time.monotonic()

    def _dispatch(self, batch: RowBatch) -> None:
        # Bound in-flight work: this blocks the producer once
        # `max_in_flight` batches are outstanding, providing backpressure
        # instead of unbounded memory growth.
        self._in_flight.acquire()
        try:
            self._collect_completed()
            request_id = str(uuid.uuid4())
            future = self._executor.submit(self._send_with_retry, batch, request_id)
        except BaseException:
            self._in_flight.release()
            raise
        future.add_done_callback(lambda f: self._in_flight.release())
        with self._pending_lock:
            self._pending_futures.append(future)

    def _collect_completed(self):
        """Retire successful Futures and surface failures without unbounded history."""
        with self._pending_lock:
            completed = [future for future in self._pending_futures if future.done()]
            for future in completed:
                self.acknowledged_rows += future.result()
                self._pending_futures.remove(future)

    def _send_with_retry(self, batch: RowBatch, request_id: str) -> int:
        body = gzip.compress(batch.to_ndjson_bytes())
        if len(body) > WIRE_MAX_BYTES:
            raise ValueError("Compressed batch exceeds the 4 MB wire payload limit")
        deadline = time.monotonic() + RETRY_BUDGET_SECONDS
        attempt = 0
        force_token_refresh_once = True

        while True:
            if time.monotonic() >= deadline:
                raise RetryBudgetExhaustedError("Append retry time budget exhausted")
            ingest_host, scoped_token = self._tokens.get_scoped_token()
            url = self._endpoint_url(ingest_host)
            try:
                response = requests.post(
                    url,
                    params={"requestId": request_id, "retryCount": attempt},
                    headers={
                        "Authorization": f"Bearer {scoped_token}",
                        "Content-Type": "application/x-ndjson",
                        "Content-Encoding": "gzip",
                        "X-Snowflake-Authorization-Token-Type": "OAUTH",
                    },
                    data=body,
                    timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                # Ambiguous outcome: the request may have been durably
                # accepted even though we saw no response. Retry with the
                # same requestId; downstream dedup by stable event id
                # covers the possible-duplicate case.
                if attempt >= MAX_RETRY_ATTEMPTS:
                    log.error(
                        "requestId=%s giving up after %d attempts: %s", request_id, attempt, exc
                    )
                    raise RetryBudgetExhaustedError(str(exc)) from exc
                delay = compute_backoff_seconds(attempt)
                log.warning(
                    "requestId=%s ambiguous network failure (attempt %d): %s. Retrying in %.2fs",
                    request_id,
                    attempt,
                    exc,
                    delay,
                )
                if time.monotonic() + delay >= deadline:
                    raise RetryBudgetExhaustedError("Required retry delay exceeds remaining budget")
                time.sleep(delay)
                attempt += 1
                continue

            if response.status_code == 200:
                if attempt > 0:
                    log.warning(
                        "requestId=%s succeeded on retry %d; rows may be duplicated (%d rows)",
                        request_id,
                        attempt,
                        len(batch.rows),
                    )
                else:
                    log.debug("requestId=%s appended %d rows", request_id, len(batch.rows))
                return len(batch.rows)

            if response.status_code == 401 and force_token_refresh_once:
                # A scoped token can expire slightly ahead of our proactive
                # refresh window; force one refresh and retry the exact
                # same attempt before treating it as permanent.
                log.warning("requestId=%s got 401; forcing a token refresh and retrying once", request_id)
                force_token_refresh_once = False
                self._tokens.get_scoped_token(force_refresh=True)
                attempt += 1
                continue

            if response.status_code in FAIL_FAST_STATUS_CODES or response.status_code == 401:
                log.error(
                    "requestId=%s permanent error status=%d body=%s",
                    request_id,
                    response.status_code,
                    response.text[:500],
                )
                raise PermanentIngestError(
                    f"status={response.status_code} body={response.text[:500]}"
                )

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt >= MAX_RETRY_ATTEMPTS:
                    log.error(
                        "requestId=%s giving up after %d attempts: status=%d",
                        request_id,
                        attempt,
                        response.status_code,
                    )
                    raise RetryBudgetExhaustedError(
                        f"status={response.status_code} body={response.text[:500]}"
                    )
                delay = compute_backoff_seconds(attempt, retry_after=parse_retry_after(response))
                log.warning(
                    "requestId=%s retryable status=%d (attempt %d). Retrying in %.2fs",
                    request_id,
                    response.status_code,
                    attempt,
                    delay,
                )
                if time.monotonic() + delay >= deadline:
                    raise RetryBudgetExhaustedError("Required retry delay exceeds remaining budget")
                time.sleep(delay)
                attempt += 1
                continue

            # Unrecognized status: treat conservatively as permanent so we
            # do not retry forever on an unexpected response shape.
            log.error(
                "requestId=%s unexpected status=%d body=%s",
                request_id,
                response.status_code,
                response.text[:500],
            )
            raise PermanentIngestError(
                f"unexpected status={response.status_code} body={response.text[:500]}"
            )

    def wait_for_pending(self, timeout: Optional[float] = None) -> None:
        """Blocks until all dispatched batches complete or timeout elapses."""
        with self._pending_lock:
            futures = list(self._pending_futures)
        done, not_done = concurrent.futures.wait(futures, timeout=timeout)
        self._collect_completed()
        if not_done:
            raise TimeoutError(f"{len(not_done)} batch(es) still in flight after timeout")

    def close(self, timeout: float = 30.0) -> None:
        """Graceful shutdown: stop intake, flush, wait for in-flight work."""
        if self._closed:
            return
        self._closed = True
        try:
            self.flush()
            self.wait_for_pending(timeout=timeout)
        finally:
            # HTTP requests already running cannot be cancelled; cleanup may outlast timeout.
            self._executor.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> "ElasticRestIngestClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# --------------------------------------------------------------------------
# Demo entry point
# --------------------------------------------------------------------------


def load_profile(path: str = "profile.json") -> dict:
    with open(path, "r") as f:
        return json.load(f)


def response_value(response, key):
    """Accept documented JSON objects and legacy raw string responses."""
    try:
        value = response.json()
    except ValueError:
        value = response.text.strip()
    if isinstance(value, dict):
        value = value.get(key) or value.get("access_token")
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {key} in authentication response")
    return value


def main() -> None:
    """Generate sample rows; use PAT explicitly or the default key-pair profile."""
    pat = os.environ.get("SNOWFLAKE_PAT")
    profile = {} if pat else load_profile(os.environ.get("SNOWFLAKE_PROFILE", "profile.json"))
    account = os.environ.get("SNOWFLAKE_ACCOUNT") or profile["account"]
    control_url = os.environ.get("SNOWFLAKE_URL") or profile.get("url", f"https://{account}.snowflakecomputing.com")
    parsed = urlparse(control_url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.path not in ("", "/"):
        raise ValueError("Use an HTTPS Snowflake account URL without credentials or path")
    token_provider = TokenProvider(account, profile.get("user", ""), profile.get("private_key_file", ""),
                                   control_host=parsed.netloc, pat=pat,
                                   role=os.environ.get("SNOWFLAKE_ROLE") or profile.get("role"))
    client = ElasticRestIngestClient(
        token_provider,
        os.environ.get("SNOWFLAKE_DATABASE", profile.get("database", DATABASE)),
        os.environ.get("SNOWFLAKE_SCHEMA", profile.get("schema", SCHEMA)),
        os.environ.get("SNOWFLAKE_TABLE", profile.get("table", TABLE)),
    )
    total = int(os.environ.get("SNOWFLAKE_TEST_ROWS", "1505"))
    if total < 0:
        raise ValueError("Row count must be nonnegative")
    run_id = os.environ.get("SNOWFLAKE_RUN_ID", str(uuid.uuid4()))
    stop_requested = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: stop_requested.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    submitted = 0
    try:
        for event_id in range(total):
            if stop_requested.is_set():
                break
            # Replace synthetic rows with a retained source and stable event IDs.
            client.submit_row({"EVENT_ID": event_id, "C1": event_id, "C2": run_id}, event_id)
            submitted += 1
        client.close(timeout=RETRY_BUDGET_SECONDS)
        print(f"Durably acknowledged {client.acknowledged_rows} rows; submitted={submitted}; run={run_id}; stopped={stop_requested.is_set()}")
    except BaseException:
        print(f"Retain source events for replay; submitted={submitted}, confirmed={client.acknowledged_rows}")
        raise
    finally:
        try:
            client.close(timeout=RETRY_BUDGET_SECONDS)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


if __name__ == "__main__":
    main()
