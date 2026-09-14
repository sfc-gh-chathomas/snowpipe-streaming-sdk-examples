/**
 * Stream events immediately; checkpoint every acknowledgement before source handoff.
 * The SDK owns batching. Caller timeouts keep the original Promise alive.
 * The source fixture regenerates events but does not persist its checkpoint.
 * Replay can duplicate events; production EVENT_IDs must be stable and source-unique.
 */
"use strict";

const { createTableClient, StreamingIngestError } = require("snowpipe-streaming");

const CHECKPOINT_ROWS = 1_000;
const CHECKPOINT_MS = 5_000;
const OUTAGE_MS = 300_000;
const POLL_MS = 1_000;
const MAX_ATTEMPTS = 6;
const INVALIDATION = new Set([
  "InvalidChannelError", "InvalidClientError", "ClosedChannelError",
  "ClosedElasticChannelError", "ClosedClientError",
]);

// Start here: source, connection, then the streaming loop.

async function main() {
  const source = new SampleEventSource(Number(process.env.SNOWFLAKE_TEST_ROWS || 10_000),
    Number(process.env.SNOWFLAKE_SOURCE_CHECKPOINT || 0));
  const producer = new ElasticProducer();
  let completed = false;
  try {
    await producer.open();
    await run(producer, source);
    completed = true;
    console.log(`Durable source checkpoint: ${source.committed}; materialization is separate`);
  } finally {
    if (!completed) console.error(`Stopped. Retain events after checkpoint ${source.committed} for replay`);
    await producer.close(completed);
  }
}

async function run(producer, source) {
  const pending = [];
  let checkpointAt = performance.now() + CHECKPOINT_MS;
  let deadline = performance.now() + OUTAGE_MS;
  while (true) {
    // read() retains ownership; null means end-of-input, not temporary idle.
    const event = source.read();
    if (event === null) break;
    const item = appendEvent(producer, event);
    pending.push(item);
    await Promise.resolve();
    if (pending.some((entry) => entry.result?.error) || pending.length >= CHECKPOINT_ROWS
        || performance.now() >= checkpointAt) {
      await confirmCheckpoint(producer, pending, source, deadline);
      checkpointAt = performance.now() + CHECKPOINT_MS;
      deadline = performance.now() + OUTAGE_MS;
    }
  }
  await confirmCheckpoint(producer, pending, source, deadline);
}

// Supporting delivery and connection details.

function retryable(error) {
  return error instanceof StreamingIngestError &&
    (INVALIDATION.has(error.errorCode) || [408, 429, 500, 502, 503, 504].includes(error.httpStatusCode));
}

function remaining(deadline) {
  const millis = deadline - performance.now();
  if (millis <= 0) {
    throw new Error("Outage deadline exceeded; source checkpoint unchanged; retain events for replay");
  }
  return millis;
}

async function backoff(attempt, deadline) {
  const delay = Math.min(remaining(deadline), Math.random() * Math.min(10_000, 250 * 2 ** Math.min(attempt, 6)));
  await new Promise((resolve) => setTimeout(resolve, delay));
}

async function poll(outcome, timeoutMs) {
  let timer;
  try {
    return await Promise.race([
      outcome,
      new Promise((resolve) => { timer = setTimeout(() => resolve({ waiting: true }), timeoutMs); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function createClient() {
  let authentication = { profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json" };
  if (process.env.SNOWFLAKE_PAT) {
    if (!process.env.SNOWFLAKE_ACCOUNT || !process.env.SNOWFLAKE_URL) {
      throw new Error("PAT mode requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
    }
    authentication = { properties: {
      authorization_type: "PAT",
      personal_access_token: process.env.SNOWFLAKE_PAT,
      account: process.env.SNOWFLAKE_ACCOUNT,
      url: process.env.SNOWFLAKE_URL,
      ...(process.env.SNOWFLAKE_ROLE ? { role: process.env.SNOWFLAKE_ROLE } : {}),
    } };
  }
  return createTableClient({
    clientName: `production-${process.pid}`,
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    ...authentication,
  });
}

// Regenerable sample data only; a real source must retain events across restarts.
class SampleEventSource {
  constructor(total = 10_000, checkpoint = 0) {
    if (!Number.isSafeInteger(total) || !Number.isSafeInteger(checkpoint) || checkpoint < 0 || checkpoint > total) {
      throw new Error("Require integer 0 <= source checkpoint <= total");
    }
    this.total = total;
    this.committed = checkpoint;
    this.nextOffset = checkpoint + 1;
  }
  read() {
    if (this.nextOffset > this.total) return null;
    const offset = this.nextOffset++;
    // Replace this mapping with your target columns and stable event ID.
    return { offset, row: { EVENT_ID: offset, C1: offset, C2: `event-${offset}` } };
  }
  acknowledge(offset) {
    // Persist/commit source progress here before retiring real source events.
    if (offset < this.committed || offset > this.total) throw new Error("Invalid source checkpoint");
    this.committed = offset;
  }
  seek(committed) {
    this.acknowledge(committed);
    this.nextOffset = committed + 1;
  }
}

class ElasticProducer {
  constructor(factory = createClient) {
    this.factory = factory;
    this.client = null;
    this.generation = 0;
  }
  async open() {
    const client = await this.factory();
    try {
      this.channel = await client.getElasticChannel();
    } catch (error) {
      await client.close({ waitForFlush: false, timeoutMs: 30_000 });
      throw error;
    }
    this.client = client;
    this.generation++;
  }
  async recover(generation) {
    // Old pending failures must not close the replacement client.
    if (generation !== this.generation) return;
    await this.close(false);
    await this.open();
  }
  async close(flush) {
    if (this.client) {
      try {
        await this.client.close({ waitForFlush: flush, timeoutMs: 30_000 });
      } finally {
        this.client = null;
      }
    }
  }
}

function appendEvent(producer, event) {
  // Observe rejection immediately, even while other events are being read.
  let promise;
  try {
    // This is the Snowflake write; keep its original acknowledgement Promise.
    promise = producer.channel.appendRowWithWait(event.row, String(event.offset));
  } catch (error) {
    promise = Promise.reject(error);
  }
  const item = { event, generation: producer.generation, result: null };
  item.outcome = Promise.resolve(promise)
    .then(() => ({ ok: true }), (error) => ({ error }))
    .then((result) => {
      item.result = result;
      return result;
    });
  return item;
}

async function confirmCheckpoint(producer, pending, source, deadline) {
  for (let item of pending) {
    let retries = 0;
    while (true) {
      const result = await poll(item.outcome, Math.min(POLL_MS, remaining(deadline)));
      if (result.waiting) continue;
      if (result.ok) break;
      if (!retryable(result.error) || retries >= MAX_ATTEMPTS - 1) throw result.error;
      if (INVALIDATION.has(result.error.errorCode)) await producer.recover(item.generation);
      console.warn(`Retry EVENT_ID=${item.event.offset} after SDK failure; duplicates possible`);
      await backoff(retries++, deadline);
      item = appendEvent(producer, item.event);
    }
  }
  if (pending.length) {
    // Retire the source window only after every append acknowledgement succeeds.
    source.acknowledge(pending[pending.length - 1].event.offset);
    pending.length = 0;
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main().catch((error) => { console.error(error.message); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { SampleEventSource, retryable, remaining, backoff, createClient, CHECKPOINT_ROWS, MAX_ATTEMPTS, poll,  ElasticProducer, appendEvent, confirmCheckpoint, run, main };
