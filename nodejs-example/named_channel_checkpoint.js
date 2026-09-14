/**
 * Single-writer named-channel producer. Append immediately, then checkpoint the
 * committed offset before source handoff. Retain source events for replay;
 * timeout pauses intake without reopening. Do not share channel ownership.
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
  const producer = new NamedProducer();
  let completed = false;
  try {
    await run(producer, source);
    completed = true;
    console.log(`Committed source checkpoint: ${source.committed}`);
  } finally {
    if (!completed) console.error(`Stopped. Retain events after checkpoint ${source.committed} for replay`);
    await producer.close(completed);
  }
}

async function run(producer, source) {
  // Resume after the server checkpoint, never after the last submitted event.
  source.seek(await producer.open());
  let lastSubmittedOffset = source.committed;
  let uncommittedCount = 0;
  let retryAttempts = 0;
  let event = null;
  let deadline = performance.now() + OUTAGE_MS;
  let checkpointAt = performance.now() + CHECKPOINT_MS;
  while (true) {
    try {
      if (event === null) event = source.read();
      if (event === null) {
        if (uncommittedCount) await confirmCheckpoint(producer, lastSubmittedOffset, source, deadline);
        return;
      }
      remaining(deadline);
      // Write immediately; the SDK handles transport batching.
      producer.channel.appendRow(event.row, String(event.offset));
      lastSubmittedOffset = event.offset;
      event = null;
      uncommittedCount++;
      if (uncommittedCount >= CHECKPOINT_ROWS || performance.now() >= checkpointAt) {
        await confirmCheckpoint(producer, lastSubmittedOffset, source, deadline);
        uncommittedCount = 0;
        retryAttempts = 0;
        deadline = performance.now() + OUTAGE_MS;
        checkpointAt = performance.now() + CHECKPOINT_MS;
      }
    } catch (error) {
      if (!retryable(error) || ++retryAttempts >= MAX_ATTEMPTS) throw error;
      if (INVALIDATION.has(error.errorCode)) {
        source.seek(await producer.recover(error));
        lastSubmittedOffset = source.committed;
        uncommittedCount = 0;
        event = null;
      }
      await backoff(retryAttempts - 1, deadline);
    }
  }
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
const CHANNEL = process.env.SNOWFLAKE_CHANNEL || "production-source-0";

function parseOffset(token) {
  if (token == null) return 0;
  const offset = Number(token);
  if (!Number.isSafeInteger(offset) || offset < 0) throw new Error("Invalid committed source offset");
  return offset;
}

class NamedProducer {
  constructor(factory = createClient) {
    this.factory = factory;
    this.client = null;
    this.channel = null;
  }
  async open() {
    if (!this.client) this.client = await this.factory();
    const opened = await this.client.openChannel({ name: CHANNEL });
    this.channel = opened.channel;
    if (opened.status.rowsErrorCount) throw new Error("Reconcile row errors before source handoff");
    return parseOffset(opened.status.latestCommittedOffsetToken);
  }
  async recover(error) {
    if (error.errorCode === "InvalidClientError") {
      await this.close(false);
    } else if (this.channel) {
      await this.channel.close({ waitForFlush: false, timeoutMs: 30_000 }).catch(() => {});
    }
    try {
      return await this.open();
    } catch (reopened) {
      if (!["InvalidClientError", "ClosedClientError"].includes(reopened.errorCode)) throw reopened;
      await this.close(false);
      return this.open();
    }
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

async function confirmCheckpoint(producer, target, source, deadline) {
  while (true) {
    remaining(deadline);
    try {
      const status = await producer.channel.getChannelStatus();
      if (status.rowsErrorCount) throw new Error("Reconcile row errors before source handoff");
      if (status.statusCode !== "SUCCESS") {
        throw new StreamingIngestError("InvalidChannelError", status.statusCode, 409, "Conflict");
      }
      if (parseOffset(status.latestCommittedOffsetToken) >= target) {
        source.acknowledge(target);
        return;
      }
    } catch (error) {
      if (INVALIDATION.has(error.errorCode) || !retryable(error)) throw error;
    }
    await backoff(2, deadline);
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main().catch((error) => { console.error(error.message); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { SampleEventSource, retryable, remaining, backoff, createClient, CHECKPOINT_ROWS, MAX_ATTEMPTS,  NamedProducer, parseOffset, confirmCheckpoint, run, main };
