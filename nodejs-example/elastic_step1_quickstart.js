#!/usr/bin/env node
/**
 * Elastic step 1: append one row and wait for durable acknowledgement.
 *
 * This minimal example demonstrates authentication and table configuration
 * with one durably acknowledged row.
 */
"use strict";

const streaming = require("snowpipe-streaming");

async function createClient() {
  return streaming.createTableClient({
    clientName: "quickstart",
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json",
  });
}

async function main() {
  const client = await createClient();
  try {
    // Elastic Channels belong to their client and are not closed separately.
    const channel = await client.getElasticChannel();
    const row = {
      DATA: { event_id: 1, status: "active" },
      C1: 1,
      C2: "example",
    };

    // The Promise resolves when Snowflake durably accepts this append.
    await channel.appendRowWithWait(row, "event-1");
    console.log("Row durably acknowledged");
  } finally {
    await client.close({ waitForFlush: true, timeoutMs: 60_000 });
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main()
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { createClient, main };
