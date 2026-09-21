"""Sequential Elastic REST ingestion: read, batch, compress, send, confirm.

One request is in flight. Keep real source events recoverable until confirmed;
retries can duplicate rows. This sample does not provide crash-durable storage.
"""
import base64
import gzip
import hashlib
import json
import os
import random
import re
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlparse

import jwt
import requests
from cryptography.hazmat.primitives import serialization

COMPRESSED_LIMIT = 1_000_000  # Conservative sample cap; service wire limit is 4 MB.
MEMORY_LIMIT = 4_000_000     # Bound uncompressed input independently of compressibility.
BATCH_ROWS = 5_000
FLUSH_SECONDS = 1.0         # Checked between source reads, not by a background timer.
REQUEST_TIMEOUT = 30
RETRY_SECONDS = 30 * 60
MAX_RETRIES = 8
RETRYABLE = {408, 429, 500, 502, 503, 504}


def main():
    """Generate sample rows; stop intake on signal and drain sequentially."""
    profile = {}
    if not os.environ.get("SNOWFLAKE_PAT"):
        with open(os.environ.get("SNOWFLAKE_PROFILE", "profile.json")) as source:
            profile = json.load(source)
    total = int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10505"))
    if total < 0:
        raise ValueError("Row count must be nonnegative")
    target = [os.environ.get("SNOWFLAKE_" + key.upper(), profile.get(key, "MY_" + key.upper()))
              for key in ("database", "schema", "table")]
    run_id = os.environ.get("SNOWFLAKE_RUN_ID", str(uuid.uuid4()))
    stopping = False
    confirmed = 0
    submitted = 0
    batches = 0
    def stop_intake(*_):
        nonlocal stopping
        stopping = True
    previous = {sig: signal.signal(sig, stop_intake) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        with requests.Session() as session:
            tokens = Tokens(session, profile)
            # Replace this generator with retained source reads and your table mapping.
            def events():
                nonlocal submitted
                for event_id in range(total):
                    if stopping:
                        break
                    submitted += 1
                    yield {"EVENT_ID": event_id, "C1": event_id, "C2": run_id}
            for rows in batch_rows(events()):
                for body, count in compressed_batches(rows):
                    send_batch(session, tokens, target, body)
                    # Commit these events to your source only after this successful response.
                    confirmed += count
                    batches += 1
            print(f"Durably acknowledged {confirmed} rows; submitted={submitted}; batches={batches}; run={run_id}; stopped={stopping}")
    except BaseException:
        print(f"Retain unconfirmed source events for replay; confirmed={confirmed}, read={submitted}")
        raise
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def batch_rows(events):
    """Bound row count, uncompressed bytes, and time between flushes."""
    rows, size = [], 0
    started = time.monotonic()
    for event in events:
        row = json.dumps(event, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        if len(row) > MEMORY_LIMIT:
            raise ValueError("Single row exceeds the sample uncompressed memory limit")
        if rows and (size + len(row) > MEMORY_LIMIT or len(rows) >= BATCH_ROWS
                     or time.monotonic() - started >= FLUSH_SECONDS):
            yield rows
            rows, size = [], 0
            started = time.monotonic()
        rows.append(row)
        size += len(row)
    if rows:
        yield rows


def compressed_batches(rows):
    """Split at row boundaries until each exact gzip body fits the 1 MB cap."""
    body = gzip.compress(b"".join(rows), mtime=0)
    if len(body) <= COMPRESSED_LIMIT:
        yield body, len(rows)
    elif len(rows) == 1:
        raise ValueError("Single row exceeds the sample 1 MB compressed payload cap")
    else:
        midpoint = len(rows) // 2
        yield from compressed_batches(rows[:midpoint])
        yield from compressed_batches(rows[midpoint:])


def retry_delay(response, attempt):
    """Honor numeric/date Retry-After; otherwise use capped exponential jitter."""
    value = response.headers.get("Retry-After") if response is not None else None
    if value is not None:
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                delay = -1
        if delay >= 0:
            return delay
    return random.uniform(0, min(30, 0.5 * 2 ** attempt))


def send_batch(session, tokens, target, body):
    """Send identical bytes and requestId on retry; success is HTTP 200, not materialization."""
    if len(body) > COMPRESSED_LIMIT:
        raise ValueError("Compressed request exceeds sample cap")
    request_id = str(uuid.uuid4())
    deadline = time.monotonic() + RETRY_SECONDS
    refreshed = False
    for attempt in range(MAX_RETRIES + 1):
        if time.monotonic() >= deadline:
            raise TimeoutError("Batch retry budget exhausted; retain source events")
        host, token = tokens.get()
        database, schema, table = [quote(value, safe="") for value in target]
        url = f"https://{host}/v2/streaming/data/databases/{database}/schemas/{schema}/tables/{table}/rows"
        response = None
        try:
            response = session.post(url, params={"requestId": request_id, "retryCount": attempt},
                                    headers={"Authorization": f"Bearer {token}",
                                             "X-Snowflake-Authorization-Token-Type": "OAUTH",
                                             "Content-Type": "application/x-ndjson", "Content-Encoding": "gzip"},
                                    data=body, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        except (requests.ConnectionError, requests.Timeout):
            # No response is ambiguous; replay can duplicate an accepted batch.
            pass
        if response is not None:
            status = response.status_code
            response.close()
            if status == 200:
                return
            if status == 401 and not refreshed and attempt < MAX_RETRIES:
                tokens.get(force=True)
                refreshed = True
                continue
            if status not in RETRYABLE:
                raise RuntimeError(f"Append failed HTTP {status}; requestId={request_id}")
        if attempt == MAX_RETRIES:
            raise TimeoutError(f"Append retries exhausted; requestId={request_id}")
        delay = retry_delay(response, attempt)
        if time.monotonic() + delay >= deadline:
            raise TimeoutError("Required retry delay exceeds remaining budget")
        print(f"Retrying requestId={request_id}; retryCount={attempt + 1}; duplicate rows possible")
        time.sleep(delay)


def response_value(response, key):
    """Support documented JSON responses and legacy raw token/hostname strings."""
    response.raise_for_status()
    if response.status_code != 200:
        raise RuntimeError(f"Authentication returned HTTP {response.status_code}")
    try:
        value = response.json()
    except ValueError:
        value = response.text.strip()
    if isinstance(value, dict):
        value = value.get(key) or value.get("access_token")
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing {key} in authentication response")
    return value


class Tokens:
    """Own JWT/PAT authentication and scoped-token refresh for one sequential producer."""
    def __init__(self, session, profile):
        self.session = session
        self.pat = os.environ.get("SNOWFLAKE_PAT")
        self.account = os.environ.get("SNOWFLAKE_ACCOUNT") or profile["account"]
        self.user = profile.get("user", "")
        self.role = os.environ.get("SNOWFLAKE_ROLE") or profile.get("role")
        url = os.environ.get("SNOWFLAKE_URL") or profile.get("url", f"https://{self.account}.snowflakecomputing.com")
        parsed = urlparse(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
            raise ValueError("Use an HTTPS account URL without credentials, query, or path")
        self.url = url.rstrip("/")
        self.key = None
        if not self.pat:
            with open(profile["private_key_file"], "rb") as key_file:
                password = os.environ.get("PRIVATE_KEY_PASSPHRASE")
                self.key = serialization.load_pem_private_key(key_file.read(),
                            password=password.encode() if password else None)
        self.token = None
        self.host = None
        self.expires = 0

    def get(self, force=False):
        """Refresh before assumed expiry, or once after an append receives HTTP 401."""
        if self.token and not force and time.monotonic() < self.expires:
            return self.host, self.token
        bearer = self.pat
        if not bearer:
            public = self.key.public_key().public_bytes(serialization.Encoding.DER,
                                                       serialization.PublicFormat.SubjectPublicKeyInfo)
            fingerprint = base64.b64encode(hashlib.sha256(public).digest()).decode()
            qualified = f"{self.account.upper()}.{self.user.upper()}"
            now = datetime.now(timezone.utc)
            bearer = jwt.encode({"iss": f"{qualified}.SHA256:{fingerprint}", "sub": qualified,
                                 "iat": now, "exp": now + timedelta(minutes=55)}, self.key, algorithm="RS256")
        headers = {"Authorization": f"Bearer {bearer}"}
        discovery_headers = dict(headers, **{"X-Snowflake-Authorization-Token-Type":
                                  "PROGRAMMATIC_ACCESS_TOKEN" if self.pat else "KEYPAIR_JWT"})
        with self.session.get(self.url + "/v2/streaming/hostname", headers=discovery_headers,
                              timeout=REQUEST_TIMEOUT, allow_redirects=False) as response:
            self.host = response_value(response, "hostname").replace("_", "-")
        if not re.fullmatch(r"[A-Za-z0-9.-]+\.snowflakecomputing\.com", self.host):
            raise ValueError("Unexpected ingest hostname")
        scope = self.host + (f" session:role:{self.role}" if self.role else "")
        form = {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "scope": scope}
        if self.pat:
            form = {"grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token_type": "programmatic_access_token", "subject_token": bearer, "scope": scope}
        with self.session.post(self.url + "/oauth/token", headers=headers, data=form,
                               timeout=REQUEST_TIMEOUT, allow_redirects=False) as response:
            self.token = response_value(response, "token")
        # Conservative refresh policy, not a claim about the opaque token lifetime.
        self.expires = time.monotonic() + 50 * 60
        return self.host, self.token


if __name__ == "__main__":
    main()
