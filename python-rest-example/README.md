# Elastic REST Examples

Prefer the Snowpipe Streaming SDK where possible: it owns transport batching and retries.
Use direct REST when an SDK is unsuitable. These two examples use the Elastic table endpoint,
not named-channel offsets or continuation tokens.

| Example | Purpose | What it handles |
| --- | --- | --- |
| `elastic_quickstart.sh` | First successful request using cURL and PAT | Host discovery, scoped-token exchange, two NDJSON rows, fail-fast HTTP handling |
| `elastic_production.py` | Application integration using Python HTTP requests, no SDK | Key-pair JWT or PAT, scoped-token refresh, sequential gzip batches, retries, and drain |

## Setup

Create a target table with `EVENT_ID NUMBER`, `C1 NUMBER`, and `C2 VARCHAR`.
Use an existing database/schema and a role authorized to ingest. Set
`SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, and `SNOWFLAKE_TABLE`.

### 1. cURL Quickstart

Requires bash, curl, jq, and uuidgen. Inject `SNOWFLAKE_PAT` through a credential manager.
Set `SNOWFLAKE_URL` to the HTTPS account URL; optionally set `SNOWFLAKE_ROLE`.
The PAT must be authorized for that account and role.

```bash
bash elastic_quickstart.sh
```

Expected output includes `Durably acknowledged 2 rows` and a run marker.
The script does not print credentials or write them to files. Do not enable shell tracing.
It uses a 30-second HTTP timeout and makes no automatic retries. A timeout is ambiguous:
Snowflake may already have accepted the request. Do not assume rerunning provides deduplication.
For controlled retries and stable request identity, use the application example.

### 2. Python Application

Requires Python 3.9+ and the dependencies below:

```bash
python -m pip install -r requirements.txt
python elastic_production.py
```

The default authentication path reads `profile.json` (or `SNOWFLAKE_PROFILE`) matching
`profile.json.example`: account, user, private key file, optional role and account URL,
and target objects. Register the public key on the Snowflake user first. An encrypted
private key can use credential-manager injection into `PRIVATE_KEY_PASSPHRASE`.

For PAT authentication, inject `SNOWFLAKE_PAT` and set `SNOWFLAKE_ACCOUNT` and
`SNOWFLAKE_URL`; no profile or private key is needed. `SNOWFLAKE_ROLE` is optional.
Both authentication paths discover the ingest hostname and exchange for a scoped token.
Never commit profiles or keys. Private connectivity requires DNS for the discovered host.

The actual entry point generates 10,505 rows by default. `SNOWFLAKE_TEST_ROWS` changes
the count; `SNOWFLAKE_RUN_ID` sets the `C2` run marker. IDs are deterministic within
the sample run. Replace them with source-unique stable IDs in a real application.
Successful output reports confirmed and submitted counts. SIGINT/SIGTERM stops intake
and drains accepted work.

## Delivery and Bounds

- One thread sends one request at a time. Retries naturally pause source intake; no executors,
  Futures, or background queues are required. The tradeoff is lower peak throughput than parallel requests.
- The sample caps each exact gzip payload at **1,000,000 compressed bytes**, below the 4 MB
  service wire limit. A candidate batch that exceeds the sample cap is split at row boundaries
  and recompressed; no oversized request is sent. A single row that cannot fit is rejected.
- Candidate batches have independent limits of 5,000 rows and 4,000,000 uncompressed bytes.
  These bound input buffering, not total process memory: encoded rows and compression copies add overhead.
  Highly compressible input need not reach the compressed cap before a memory/row bound triggers a flush.
- Partial batches flush after one second checked between source reads, or at end-of-input/shutdown.
  An idle or blocking live source needs its own periodic flush integration.
- Source progress advances only after each sequential request succeeds. If a split batch partly
  succeeds, confirmed counts include those successful requests; unconfirmed source events remain yours.
- Each batch keeps the same `requestId` across retries and increments `retryCount`, including the
  one permitted 401 refresh. This is correlation, not an exactly-once guarantee.
- Retryable HTTP/network failures use bounded retries and jitter. Server `Retry-After` delays are
  not shortened. If a requested delay exceeds the remaining 30-minute batch retry budget, the batch
  fails rather than retrying prematurely. Individual HTTP requests time out after 30 seconds.
- Token discovery/exchange failures stop the batch; they are not silently retried indefinitely.
- Shutdown requests stop intake, not an active HTTP request or its retries. The current batch and
  final partial batch finish under their retry budgets. This is not a hard process shutdown deadline.
  A failed drain must not be interpreted as successful delivery or permission to delete source events.

## Source Responsibility

Keep events recoverable outside this process until confirmed. Pending memory is not durable storage.
Ambiguous responses and replay can produce duplicates. SDK/HTTP acknowledgements confirm durable
buffering, not row validity or immediate table visibility. Verify table contents and error logging
separately. Persistent source checkpoints, crash replay, and host-loss durability are not supplied.

## Validation Scope

Both actual entry points have been run against a test account using PAT authentication.
The key-pair JWT flow has private construction tests but has not been live-tested in this validation.
No test credentials, private harnesses, or test folders are included here.

## References

- [Elastic REST tutorial](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-rest-getting-started)
- [REST endpoint reference](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api)
- [Elastic limitations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-limitations)
