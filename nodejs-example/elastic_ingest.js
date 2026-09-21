"use strict";
// Level 1: pipelined first ingest. Sample rows are regenerable; SDK memory is not durable source storage.
process.env.SS_LOG_LEVEL ??= "warn";
const { randomUUID } = require("node:crypto");
const streaming = require("snowpipe-streaming");
const RUN_ID = process.env.SNOWFLAKE_RUN_ID || randomUUID();

function connectionProperties() {
  const pat = process.env.SNOWFLAKE_PAT;
  if (!pat) return null;
  if (!process.env.SNOWFLAKE_ACCOUNT || !process.env.SNOWFLAKE_URL) {
    throw new Error("PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
  }

  const properties = {
    authorization_type: "PAT",
    personal_access_token: pat,
    account: process.env.SNOWFLAKE_ACCOUNT,
    url: process.env.SNOWFLAKE_URL,
  };
  if (process.env.SNOWFLAKE_ROLE) {
    properties.role = process.env.SNOWFLAKE_ROLE;
  }
  return properties;
}

async function createClient() {
  const properties = connectionProperties();
  return streaming.createTableClient({
    clientName: `ingest-${randomUUID()}`,
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    ...(properties ? { properties } : { profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json" }),
  });
}

function sampleRow(eventId) {
  return {
    EVENT_ID: eventId,
    C1: eventId,
    C2: `${RUN_ID}-${eventId}`,
  };
}

// Pipeline first, then confirm; do not wait after each row.
async function main(clientFactory = createClient) {
  const client = await clientFactory();
  const pending = [];
  let complete = false;
  try {
    const channel = await client.getElasticChannel();
    for (let eventId = 0; eventId < 10; eventId++) {
      pending.push(channel.appendRowWithWait(sampleRow(eventId), null)
        .then(() => null, (error) => error));
    }
    const errors = await Promise.all(pending);
    if (errors.some(Boolean)) throw errors.find(Boolean);
    complete = true;
    console.log(`Durably acknowledged 10 rows; run=${RUN_ID}. Check materialization separately.`);
  } finally {
    await client.close({ waitForFlush: complete, timeoutMs: 30_000 });
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1000);
  main().catch((error) => { console.error(error); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}
module.exports = { main, createClient, sampleRow };
