"use strict";

const { randomUUID } = require("node:crypto");
const { createTableClient } = require("snowpipe-streaming");

const DATABASE = "MY_DATABASE";
const SCHEMA = "MY_SCHEMA";
const TABLE = "MY_TABLE";

async function main() {
  const client = await createTableClient({
    clientName: `quickstart-${randomUUID()}`,
    dbName: DATABASE,
    schemaName: SCHEMA,
    tableName: TABLE,
    profilePath: "profile.json",
  });
  let complete = false;
  try {
    const channel = await client.getElasticChannel();
    const pending = [];
    for (let eventId = 1; eventId <= 10; eventId++) {
      const row = { C1: eventId, C2: String(eventId) };
      pending.push(channel.appendRowWithWait(row, null)
        .then(() => null, (error) => error));
    }
    const errors = await Promise.all(pending);
    if (errors.some(Boolean)) throw errors.find(Boolean);
    complete = true;
    console.log("Durably acknowledged 10 rows. Check table contents separately.");
  } finally {
    await client.close({ waitForFlush: complete, timeoutMs: 30_000 });
  }
}

const keepAlive = setInterval(() => {}, 1000);
main().catch((error) => { console.error(error); process.exitCode = 1; })
  .finally(() => clearInterval(keepAlive));
