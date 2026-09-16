# Python Snowpipe Streaming SDK Examples

The Elastic examples build from one append to source-aware recovery. Read their
three steps in order. The named-channel example is an alternative for sources
that need offset checkpointing. The SDK requirement is `snowpipe-streaming`
**1.8.0 or later**.

## Examples

| Path | File | What it adds |
| --- | --- | --- |
| Elastic 1 | [`elastic_step1_quickstart.py`](./elastic_step1_quickstart.py) | Create a table client, append one row, wait for durability, and close. |
| Elastic 2 | [`elastic_step2_continuous.py`](./elastic_step2_continuous.py) | Keep appending, pause on SDK backpressure, observe acknowledgements, and drain at shutdown. |
| Elastic 3 | [`elastic_step3_recovery.py`](./elastic_step3_recovery.py) | Retain source events, checkpoint confirmed progress, retry transient failures, and swap an invalid client. |
| Named | [`named_channel_checkpoint.py`](./named_channel_checkpoint.py) | Use a stable channel and Snowflake's committed offset token to position a retained source after restart. |

The original [`streaming_ingest_example.py`](./streaming_ingest_example.py) is
retained for compatibility. New integrations should start with the progression
above. The [`monitoring`](./monitoring) directory contains separate monitoring
and abort examples.

## Setup

### Requirements

- Python 3.9 or later
- A Snowflake account with RSA key-pair authentication
- A role allowed to insert into the target table

Install the SDK and test dependency:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt pytest
```

### Target table

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    DATA VARIANT,
    EVENT_ID NUMBER,
    C1 NUMBER,
    C2 VARCHAR,
    TS TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is required. Table-mode clients use the default
`MY_TABLE-STREAMING` pipe.

### Authentication

Create `profile.json` from `profile.json.example`:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

Set object names through the environment or edit their example defaults:

```bash
export SNOWFLAKE_DATABASE=MY_DATABASE
export SNOWFLAKE_SCHEMA=MY_SCHEMA
export SNOWFLAKE_TABLE=MY_TABLE
```

## Run

```bash
python3 elastic_step1_quickstart.py
python3 elastic_step2_continuous.py
python3 elastic_step3_recovery.py
python3 named_channel_checkpoint.py
```

Set `SNOWFLAKE_TEST_ROWS` to change the generated row count. Elastic recovery
also accepts `SNOWFLAKE_SOURCE_CHECKPOINT`; named-channel recovery starts from
the offset returned by Snowflake.

## Semantics

### Elastic Channels

An Elastic acknowledgement confirms that Snowflake durably accepted the
append. It does not confirm row validity or immediate table visibility. Check
the target table and its error table separately.

`elastic_step2_continuous.py` relies on SDK byte and memory limits for flow
control. When an append is rejected with HTTP 429, it pauses without reading
another event and retries the same event. Its Future list tracks delivery
outcomes; it does not hold a second copy of the rows.

`elastic_step3_recovery.py` models a retained source with `SampleEventSource`.
Its data is regenerable and its checkpoint exists only in memory. Replace
`read`, `acknowledge`, and `seek` with operations from your retained log,
outbox, or source system.

The recovery example:

- Keeps each original acknowledgement future until it completes.
- Advances the source checkpoint only across a contiguous confirmed prefix.
- Pauses intake at `MAX_PENDING_EVENTS`; this is an event-count limit, not a
  byte-memory limit.
- Treats immediate HTTP 429 responses as backpressure.
- Retries terminal transient failures within a bounded attempt count.
- Swaps the client only when the failed append belongs to the active client.
- Stops after 30 minutes without confirmed source progress.

Replaying an Elastic append can create a duplicate. Use stable source event IDs
and define downstream reconciliation for your application.

### Named channels

Give each stable channel name one writer. Every append carries a caller-provided
offset token. Opening the channel and fetching channel status return Snowflake's
latest committed token, which the example uses to position its source.

Offset tokens are checkpoint metadata. Snowflake does not interpret this
example's numeric ordering or use arbitrary token values as deduplication keys.
Row errors must be reconciled before advancing the source checkpoint.

## Tests

The tests use fake SDK clients and do not connect to Snowflake:

```bash
python3 -m pytest tests -q
```

They cover bounded intake, late and out-of-order acknowledgements, contiguous
checkpoints, client swapping, retry limits, named-channel restart positioning,
and row-error handling.

## Additional Resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview)
- [Elastic Channels getting started](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Elastic Channels best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming SDK on PyPI](https://pypi.org/project/snowpipe-streaming/)
