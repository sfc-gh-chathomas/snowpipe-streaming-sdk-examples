# Node.js Snowpipe Streaming SDK Examples

Examples for streaming data into Snowflake with the [Snowpipe Streaming SDK](https://www.npmjs.com/package/snowpipe-streaming) (Node.js).

Requires **snowpipe-streaming >= 1.8.0** and Node.js >= 20.

## Channel modes

| Mode | File | When to use |
|------|------|-------------|
| Elastic Channel (quickstart) | `elastic_quickstart.js` | New applications. Easiest to get started; Snowflake manages scaling and channel lifecycle. Delivery is at-least-once and unordered. |
| Elastic Channel (production) | `elastic_production.js` | Production workloads. Adds bounded durability checkpoints, retry, client recreation, and graceful shutdown. |
| Named channel (checkpoint) | `named_channel_checkpoint.js` | Strict exactly-once ingestion, ordered delivery within a channel, or explicit source-offset recovery after a restart. |

The legacy `streaming_ingest_example.js` (named-channel pattern) is preserved for reference.

## Prerequisites

- Node.js 20 or higher
- npm
- A Snowflake account with appropriate permissions
- RSA key-pair authentication configured

## Setup

### 1. Generate an RSA key pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key with your Snowflake user:

```sql
ALTER USER MY_USER SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, without header/footer>';
```

### 2. Create a Snowflake table

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    DATA   VARIANT,
    C1     NUMBER,
    C2     VARCHAR,
    ID     NUMBER,
    VALUE  VARCHAR,
    EVENT_ID NUMBER,
    ts     TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is needed. The SDK derives the pipe name automatically as `<TABLE>-STREAMING`.

### 3. Install dependencies

```bash
npm install
```

### 4. Configure authentication

Copy `profile.json.example` to `profile.json` and fill in your credentials:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

### 5. Update the table constants

Edit the object-name defaults or set the matching `SNOWFLAKE_*` environment variables.

## Run

```bash
# Elastic quickstart (recommended starting point)
node elastic_quickstart.js

# Production Elastic example
node elastic_production.js

# Named-channel checkpoint example
node named_channel_checkpoint.js
```

## Example details

### `elastic_quickstart.js`

Creates a table-mode client, gets the Elastic Channel, appends the same `DATA`/`C1`/`C2`
row shown in the product documentation with `appendRowWithWait`, retrieves channel status,
and closes the client.

### `elastic_production.js`

See the [shared retention contract](../README.md#production-retention-contract).
Appends are issued as events are read. Rejections are observed immediately so a later checkpoint
cannot cause an unhandled rejection. Count/time checkpoints pause intake and wait on every original
Promise. A caller timeout neither cancels nor resubmits it. Only terminal retryable SDK failures are
resubmitted; only SDK invalidation recreates the client. Old-generation failures reuse the new client.

Both production programs are self-contained. The included `ReplaySource` regenerates fixed
events and does not persist checkpoints. Replace it with the producer's retained source API.
PAT mode requires `SNOWFLAKE_PAT`, `SNOWFLAKE_ACCOUNT`, and `SNOWFLAKE_URL`; `SNOWFLAKE_ROLE` is optional.
Otherwise `profile.json` or `SNOWFLAKE_PROFILE` is used. Account/role defaults are not hard-coded for tests.

### `named_channel_checkpoint.js`

Demonstrates the named-channel pattern for strict exactly-once ingestion:

- Opens a stable, exclusively owned channel without replacing its server offset and seeks after committed progress.
- Appends each event with `appendRow`; the SDK buffers and batches it internally.
- Polls channel status only at bounded count/time checkpoints and end of input; timeout pauses reading, not the SDK's retries.
- Checks row errors before source handoff. Invalidation reopens and seeks to the newly returned committed offset.
- `SNOWFLAKE_CHANNEL` selects the stable channel. Running again resumes after committed events instead of starting from zero.

## Tests

Tests exercise the real loops with controlled failures: paused reads, late acknowledgements,
out-of-order completion, invalidation generations, backpressure, and restart offsets.

```bash
npm install
npm test
```

## Troubleshooting

- **Connection errors**: verify `profile.json` credentials and network access to Snowflake.
- **Permission errors**: ensure your role has INSERT privilege on the table.
- **SDK invalidation**: the examples recover within an attempt budget. Persistent errors require
  investigating the underlying problem; a caller timeout alone does not trigger recreation.
- **Node.js version**: ensure Node.js 20 or higher (`node --version`).

## Additional resources

- [Snowpipe Streaming overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview)
- [Getting started guide](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started)
- [Snowpipe Streaming SDK on npm](https://www.npmjs.com/package/snowpipe-streaming)
