# Python Elastic Examples

Python 3.9+ on an SDK-supported platform. SDK version: 1.8.0.

## Run

From this directory:

```bash
python -m pip install -r requirements.txt
python elastic_ingest.py
python elastic_ingest_continuous.py
python elastic_ingest_callbacks.py
```

## Choose a Level

| Level | Goal | Behavior |
| --- | --- | --- |
| 1: Quickstart | First successful ingest | Pipeline 10 single-row appends, observe acknowledgements, close. Fail fast on errors. |
| 2: Continuous | Adapt a sustained producer | Futures/Promises, 1,000 outstanding appends, backpressure, limited client recreation, stop-and-drain. |
| 3: Callbacks | Integrate with an event-driven app | Equivalent scope to Level 2; SDK handlers enqueue outcomes and the control loop owns recovery. |

Callbacks are an alternative completion style, not stronger delivery guarantees. Every file is self-contained. The SDK batches transport; the application does not need to assemble batches for wire efficiency.

## What You Must Adapt

`sample_row` / `sampleRow` generates synthetic `EVENT_ID`, `C1`, and `C2` values. Replace it and the integer input loop with a retained source and your table mapping. `SNOWFLAKE_RUN_ID` prefixes `C2` to identify a run; it is a diagnostic marker, not a deduplication key. Successful output reports durable acknowledgements; check table materialization and error logging separately.

Levels 2/3 default to 5,000 rows. Set `SNOWFLAKE_TEST_ROWS` for another nonnegative count. Pending work is bounded to 1,000 events, not bytes; size this for your payloads. On backpressure the rejected event remains eligible for retry. Client recreation occurs only on structured invalidation, up to six times per run. Other terminal errors stop the program. Recreation may replay unresolved events and therefore introduce duplicates.

There is no short per-Future acknowledgement deadline. Levels 2/3 stop after 30 minutes without observed durable progress while work is pending, or sustained capacity rejection. Recovery and close calls retain their own SDK timeouts. A stop signal requests intake to stop and accepted work to drain; a stalled drain still fails at the operational deadline. SIGKILL and machine loss cannot drain.

**This is not a durable source adapter.** Counters and pending data are in memory. Keep real events recoverable outside the SDK until confirmed. The sample regenerates unresolved rows by ID during in-process recovery; it does not persist checkpoints or automatically resume a previous process. Out-of-order acknowledgement counts are not a source offset. Define source acknowledgement, stable IDs, duplicate reconciliation, and restart semantics before deploying. The end-to-end retained-source/crash-recovery recipe is deferred to Level 4.

## Authentication and Target

Use `profile.json` based on `profile.json.example` for key-pair authentication, or inject `SNOWFLAKE_PAT` with explicit `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_URL`. `SNOWFLAKE_ROLE` is optional. Use a secure credential manager and a least-privilege role. Set `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, and `SNOWFLAKE_TABLE` to an existing target:

```sql
CREATE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    EVENT_ID NUMBER,
    C1 NUMBER,
    C2 VARCHAR
);
```

The table-mode SDK client uses the default streaming pipe. No manually created channel name is required for Elastic.

## Production Boundary

Handle source capacity/overflow, durable retention, permanent row errors, and host failure in your application. Do not interpret SDK buffering as disk persistence or automatic exactly-once replay. Validate your real source, shutdown, and failure paths before production. No Kafka hop is required solely for delivering events to Snowflake.

Legacy named-channel and monitoring examples remain separate from these Elastic levels.
