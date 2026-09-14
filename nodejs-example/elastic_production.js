/**
 * Stream events immediately; checkpoint every acknowledgement before source handoff.
 * The SDK owns batching. Caller timeouts keep the original Promise alive.
 * The source fixture regenerates events but does not persist its checkpoint.
 * Replay can duplicate events; production EVENT_IDs must be stable and source-unique.
 */
"use strict";

const support = require("./production_support.js");

class ElasticSession {
  constructor(factory = support.createClient) {
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

function submit(session, event) {
  // Observe rejection immediately, even while other events are being read.
  let promise;
  try {
    promise = session.channel.appendRowWithWait(event.row, String(event.offset));
  } catch (error) {
    promise = Promise.reject(error);
  }
  const item = { event, generation: session.generation, result: null };
  item.outcome = support.observe(promise).then((result) => { item.result = result; return result; });
  return item;
}

async function checkpoint(session, pending, source, deadline) {
  for (let item of pending) {
    let retries = 0;
    while (true) {
      const result = await support.poll(item.outcome, Math.min(support.POLL_MS, support.remaining(deadline)));
      if (result.waiting) continue;
      if (result.ok) break;
      if (!support.retryable(result.error) || retries >= support.MAX_ATTEMPTS - 1) throw result.error;
      if (support.INVALIDATION.has(result.error.errorCode)) await session.recover(item.generation);
      console.warn(`Retry EVENT_ID=${item.event.offset} after SDK failure; duplicates possible`);
      await support.backoff(retries++, deadline);
      item = submit(session, item.event);
    }
  }
  if (pending.length) {
    source.acknowledge(pending[pending.length - 1].event.offset);
    pending.length = 0;
  }
}

async function run(session, source) {
  const pending = [];
  let checkpointAt = performance.now() + support.CHECKPOINT_MS;
  let deadline = performance.now() + support.OUTAGE_MS;
  while (true) {
    const event = source.read();
    if (event === null) break;
    const item = submit(session, event);
    pending.push(item);
    await Promise.resolve();
    if (pending.some((entry) => entry.result?.error) || pending.length >= support.CHECKPOINT_ROWS
        || performance.now() >= checkpointAt) {
      await checkpoint(session, pending, source, deadline);
      checkpointAt = performance.now() + support.CHECKPOINT_MS;
      deadline = performance.now() + support.OUTAGE_MS;
    }
  }
  await checkpoint(session, pending, source, deadline);
}

async function main() {
  const source = support.sourceFromEnv();
  const session = new ElasticSession();
  let completed = false;
  try {
    await session.open();
    await run(session, source);
    completed = true;
    console.log(`Durable source checkpoint: ${source.committed}; materialization is separate`);
  } finally {
    if (!completed) console.error(`Stopped. Retain events after checkpoint ${source.committed} for replay`);
    await session.close(completed);
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main().catch((error) => { console.error(error.message); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { ElasticSession, submit, checkpoint, run, main };
