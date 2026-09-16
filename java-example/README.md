# Java Snowpipe Streaming SDK Examples

The three Elastic examples progress from one append to production concerns.
The named-channel example is an alternative for sources that need offset
checkpointing. These examples require `snowpipe-streaming` **1.8.0 or later**.

## Examples

| Path | Class | What it adds |
| --- | --- | --- |
| Elastic 1 | `ElasticStep1Quickstart` | Create a table client, append one row, wait for durability, and close. |
| Elastic 2 | `ElasticStep2Continuous` | Bound pending acknowledgement Futures and drain them at shutdown. |
| Elastic 3 | `ElasticStep3Production` | Retain source events, checkpoint confirmed progress, retry, and swap an invalid client. |
| Named | `NamedChannelCheckpoint` | Position a retained source from a stable channel's committed offset token. |

`StreamingIngestExample` is retained for compatibility. New integrations
should start with the progression above. The `monitoring` directory contains
separate monitoring and abort examples.

## Setup

### Requirements

- Java 11 or later
- Maven 3.6 or later
- A Snowflake account with RSA key-pair authentication
- A role allowed to insert into the target table

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

## Build And Run

```bash
mvn clean package

# Step 1 is the default.
mvn exec:java
mvn exec:java -Dexec.mainClass=com.snowflake.example.ElasticStep2Continuous
mvn exec:java -Dexec.mainClass=com.snowflake.example.ElasticStep3Production
mvn exec:java -Dexec.mainClass=com.snowflake.example.NamedChannelCheckpoint
```

Set `SNOWFLAKE_TEST_ROWS` to change the generated row count. Elastic step 3
also accepts `SNOWFLAKE_SOURCE_CHECKPOINT`; named-channel recovery starts from
the offset returned by Snowflake.

## Semantics

An Elastic acknowledgement confirms that Snowflake durably accepted the
append. It does not confirm row validity or immediate table visibility.

Step 2 keeps up to `MAX_PENDING_EVENTS` acknowledgement Futures. The deque
contains handles, not rows; the SDK owns row buffering and transport batching.
When the window is full, source intake pauses until the oldest Future
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
mvn test
```
