"use strict";
// Level 2: continuous Promise ingestion. Sample rows are regenerable; SDK memory is not durable source storage.
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

const MAX_PENDING = 1000;
const STALL_MS = 30 * 60_000;
const INVALID = new Set(["InvalidChannelError", "InvalidClientError", "ClosedClientError", "ClosedElasticChannelError"]);
const pause = () => new Promise((resolve) => setTimeout(resolve, 250));

// Observe outcomes immediately; never leave rejected Promises unhandled.
function submit(channel, eventId) {
  const item = { done: false, error: null };
  channel.appendRowWithWait(sampleRow(eventId), null).then(
    () => { item.done = true; },
    (error) => { item.error = error; item.done = true; },
  );
  return item;
}

// One control loop owns intake, progress, and recovery; SDK callbacks never do I/O.
async function main(clientFactory = createClient) {
  const total = Number(process.env.SNOWFLAKE_TEST_ROWS || 5000);
  if (!Number.isSafeInteger(total) || total < 0) throw new Error("Invalid row count");
  let stopping = false;
  const stop = () => { stopping = true; };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  let client;
  let generation = 0;
  let attempts = 0;
  let nextId = 0;
  let confirmed = 0;
  let complete = false;
  let waitingForCapacity = false;
  let deadline = performance.now() + STALL_MS;
  const pending = new Map();

  try {
    client = await clientFactory();
    let channel = await client.getElasticChannel();

    while ((!stopping && nextId < total) || pending.size) {
      try {

        let progress = false;
        for (const [eventId, item] of pending) {
          if (!item.done) continue;
          if (item.error) throw item.error;
          pending.delete(eventId);
          confirmed++;
          progress = true;
        }
        if (progress || (!pending.size && !waitingForCapacity)) deadline = performance.now() + STALL_MS;
        if (performance.now() >= deadline) throw new Error("No durable progress for 30 minutes; retain unresolved events");
        if (!stopping && nextId < total && pending.size < MAX_PENDING) {
          // Replace sampleRow with a retained source read and your target-column mapping.
          pending.set(nextId, submit(channel, nextId));
          nextId++;
          waitingForCapacity = false;
          if (nextId % 100 === 0) await new Promise((resolve) => setImmediate(resolve));
        } else {
          await pause();
        }
      } catch (error) {
        if (error.httpStatusCode === 429) {
          waitingForCapacity = true;
          // A settled rejected append can be retried; unresolved appends must not be replayed.
          for (const [eventId, item] of pending) {
            if (item.done && item.error === error) {
              try { pending.set(eventId, submit(channel, eventId)); }
              catch (retry) { if (retry.httpStatusCode !== 429) throw retry; }
            }
          }
          if (performance.now() >= deadline) throw new Error("Backpressure persisted for 30 minutes");
          await pause();
          continue;
        }
        if (!INVALID.has(error.errorCode) || attempts++ >= 6) throw error;
        for (const [eventId, item] of pending) {
          if (item.done && !item.error) { pending.delete(eventId); confirmed++; }
        }
        try { await client.close({ waitForFlush: false, timeoutMs: 30_000 }); }
        catch (closing) { if (!INVALID.has(closing.errorCode)) throw closing; }
        client = null;
        generation++;
        client = await clientFactory();
        channel = await client.getElasticChannel();

        console.warn(`Recreated client; replaying ${pending.size} unresolved events; duplicates possible`);
        for (const eventId of pending.keys()) {
          while (true) {
            if (performance.now() >= deadline) throw new Error("Recovery exceeded stalled-progress budget");
            try { pending.set(eventId, submit(channel, eventId)); break; }
            catch (retry) { if (retry.httpStatusCode !== 429) throw retry; await pause(); }
          }
        }
      }
    }
    complete = true;
    console.log(`Durably acknowledged ${confirmed} rows; submitted=${nextId}; run=${RUN_ID}; stopped=${stopping}`);
  } finally {
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    if (!complete) console.error(`Retain source for replay; confirmed=${confirmed}, submitted=${nextId}`);
    if (client) await client.close({ waitForFlush: complete, timeoutMs: 30_000 });
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1000);
  main().catch((error) => { console.error(error); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}
module.exports = { main, createClient, sampleRow };
