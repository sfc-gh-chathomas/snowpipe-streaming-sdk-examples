#!/usr/bin/env node
/**
 * Elastic step 2: keep appending while bounding unacknowledged work.
 *
 * The array stores Promise handles, not rows. The SDK owns row buffering,
 * batching, and transient transport retries. A rejection that reaches this
 * example is terminal and stops the program.
 */
"use strict";

const streaming = require("snowpipe-streaming");

const MAX_PENDING_EVENTS = 10_000;

async function createClient() {
  return streaming.createTableClient({
    clientName: "continuous",
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json",
  });
}

function sampleRow(eventId) {
  return {
    EVENT_ID: eventId,
    C1: eventId,
    C2: `event-${eventId}`,
  };
}

function track(promise) {
  // Observe rejection immediately so it cannot become an unhandled rejection.
  const pending = { settled: false, result: null };
  pending.outcome = Promise.resolve(promise).then(
    () => ({ ok: true }),
    (error) => ({ ok: false, error }),
  ).then((result) => {
    pending.settled = true;
    pending.result = result;
    return result;
  });
  return pending;
}

async function waitAndRemoveConfirmedPrefix(pending) {
  await pending[0].outcome;
  let confirmed = 0;
  while (pending[0]?.settled) {
    const item = pending.shift();
    if (!item.result.ok) throw item.result.error;
    confirmed++;
  }
  return confirmed;
}

async function main(clientFactory = createClient) {
  const total = Number(process.env.SNOWFLAKE_TEST_ROWS || 10_000);
  const client = await clientFactory();
  const pending = [];
  let confirmed = 0;
  let completed = false;
  try {
    const channel = await client.getElasticChannel();
    for (let eventId = 0; eventId < total; eventId++) {
      // No callback is registered, so the returned Promise identifies the append.
      pending.push(track(channel.appendRowWithWait(sampleRow(eventId), null)));
      if (pending.length >= MAX_PENDING_EVENTS) {
        // Pause intake until the oldest acknowledgement releases one slot.
        confirmed += await waitAndRemoveConfirmedPrefix(pending);
      }
    }

    // End of input: wait until every accepted append is durably acknowledged.
    while (pending.length) {
      confirmed += await waitAndRemoveConfirmedPrefix(pending);
    }
    completed = true;
    console.log(`Durably acknowledged ${confirmed} rows`);
  } finally {
    await client.close({ waitForFlush: completed, timeoutMs: 60_000 });
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

module.exports = {
  MAX_PENDING_EVENTS,
  createClient,
  main,
  sampleRow,
  track,
  waitAndRemoveConfirmedPrefix,
};
