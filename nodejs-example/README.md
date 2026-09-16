# Node.js Snowpipe Streaming SDK Examples

The three Elastic examples progress from one append to production concerns.
The named-channel example is an alternative for sources that need offset
checkpointing. These examples require `snowpipe-streaming` **1.8.0 or later**
and Node.js 20 or later.

## Examples

| Path | File | What it adds |
| --- | --- | --- |
| Elastic 1 | `elastic_step1_quickstart.js` | Create a table client, append one row, wait for durability, and close. |
| Elastic 2 | `elastic_step2_continuous.js` | Bound pending acknowledgement Promises and drain them at shutdown. |
| Elastic 3 | `elastic_step3_production.js` | Retain source events, checkpoint confirmed progress, retry, and swap an invalid client. |
| Named | `named_channel_checkpoint.js` | Position a retained source from a stable channel's committed offset token. |

`streaming_ingest_example.js` is retained for compatibility. New integrations
should start with the progression above.

## Setup

### Requirements

- Node.js 20 or later
- npm
- A Snowflake account with RSA key-pair authentication
- A role allowed to insert into the target table

Install the SDK:

```bash
npm install
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
npm start
node elastic_step2_continuous.js
node elastic_step3_production.js
node named_channel_checkpoint.js
```

Set `SNOWFLAKE_TEST_ROWS` to change the generated row count. Elastic step 3
also accepts `SNOWFLAKE_SOURCE_CHECKPOINT`; named-channel recovery starts from
the offset returned by Snowflake.

## Semantics

An Elastic acknowledgement confirms that Snowflake durably accepted the
append. It does not confirm row validity or immediate table visibility.

Step 2 keeps up to `MAX_PENDING_EVENTS` acknowledgement Promises. The array
contains handles, not rows; the SDK owns row buffering and transport batching.
When the window is full, source intake pauses until the oldest Promise
completes. The example propagates errors that remain after SDK retries.

Step 3 models a retained source with `SampleEventSource`. Its data is
regenerable and its checkpoint exists only in memory. A real producer must
replace its read, acknowledge, and seek operations with retained source APIs.
Replaying an Elastic append can create a duplicate, so production event IDs
must remain stable.

Give each named channel one writer. Offset tokens are checkpoint metadata, not
deduplication keys. Row errors must be reconciled before advancing the source
checkpoint.

## Tests

The tests use SDK-shaped fake clients and do not connect to Snowflake:

```bash
npm test
```
