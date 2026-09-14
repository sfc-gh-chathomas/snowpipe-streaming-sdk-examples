# Java Snowpipe Streaming SDK Examples

Examples demonstrating how to use the Snowflake Streaming Ingest SDK in Java to ingest data into Snowflake using the [high-performance architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview).

SDK requirement: `snowpipe-streaming` **1.8.0** or later.

## Examples

| File | Description |
|---|---|
| `ElasticQuickstart.java` | **Start here.** One waitable Elastic append, channel status, and client close. |
| `ElasticProducer.java` | Production Elastic Channel producer: individual row appends, bounded durability checkpoints, retry, and client recovery. |
| `NamedChannelCheckpoint.java` | Single-writer named channel: append immediately, poll committed offsets at checkpoints, reopen and seek on invalidation. |
| `StreamingIngestExample.java` | Original named-channel example (retained for reference). |

**Channel mode guidance:**
- Use **Elastic Channels** for most new applications. They scale automatically across concurrent producers and require no offset management.
- Use **named channels** when you need ordered, strictly-exactly-once delivery or source-offset integration (for example, Kafka partition offset tracking).

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- A Snowflake account with appropriate permissions
- RSA key-pair authentication configured

## Setup

### 1. Generate RSA Key Pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key with your Snowflake user:

```sql
ALTER USER MY_USER SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, without header/footer>';
```

### 2. Create a Target Table

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    DATA VARIANT,
    c1 NUMBER,
    c2 VARCHAR,
    ts  TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is required. The high-performance architecture automatically creates a default pipe named `MY_TABLE-STREAMING` when you first open a channel or call `getElasticChannel`.

Both production examples also expect an `event_id` column for replay identification:

```sql
ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE ADD COLUMN event_id NUMBER;
```

### 3. Configure Authentication

Copy `profile.json.example` to `profile.json` in the `java-example` directory and fill in your credentials:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

### 4. Update Object Names

Edit the `DATABASE`, `SCHEMA`, and `TABLE` defaults, or set the matching `SNOWFLAKE_*` environment variables.

Each production program includes its own configuration, retry timing, and replay fixture.
No separate production helper library is required. PAT mode reads `SNOWFLAKE_PAT` from the environment and requires explicit
`SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_URL`; `SNOWFLAKE_ROLE` is optional. Otherwise they use `profile.json`
(or `SNOWFLAKE_PROFILE`). No PM account or administrative role is selected by default.

## Production behavior

See the [shared retention contract](../README.md#production-retention-contract).
Each event is submitted before the next source read. At a count/time checkpoint, intake stops until
all Elastic Futures complete or the named-channel committed offset reaches the target. Local wait
timeouts preserve pending Futures; only SDK invalidation triggers client/channel recovery.
`ReplaySource` is regenerable test data with an in-memory checkpoint, not a disk spool.

Configure `SNOWFLAKE_TEST_ROWS` for sample size and `SNOWFLAKE_CHANNEL` for a dedicated named-channel
identity. A repeat named run resumes without resending committed records. Elastic replay uses
`SNOWFLAKE_SOURCE_CHECKPOINT` if supplied; duplicates remain possible.

## Build

```bash
mvn clean package
```

## Run

**Elastic Channel quickstart** (default):

```bash
mvn exec:java
```

**Specific example:**

```bash
mvn exec:java -Dexec.mainClass="com.snowflake.example.ElasticProducer"
mvn exec:java -Dexec.mainClass="com.snowflake.example.NamedChannelCheckpoint"
```

## Test

Unit tests verify checkpointing, error classification, recovery, and offset arithmetic without a live Snowflake connection:

```bash
mvn test
```

## Additional Resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [Elastic Channels getting started](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming SDK on Maven Central](https://repo1.maven.org/maven2/com/snowflake/snowpipe-streaming/)
