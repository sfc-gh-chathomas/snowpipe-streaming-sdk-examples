/**
 * Single-writer named-channel producer. Append immediately, then checkpoint the
 * committed offset before source handoff. Retain source events for replay;
 * timeout pauses intake without reopening. Do not share channel ownership.
 */
"use strict";

const support = require("./production_support.js");
const { StreamingIngestError } = require("snowpipe-streaming");
const CHANNEL = process.env.SNOWFLAKE_CHANNEL || "production-source-0";

function parseOffset(token) {
  if (token == null) return 0;
  const offset = Number(token);
  if (!Number.isSafeInteger(offset) || offset < 0) throw new Error("Invalid committed source offset");
  return offset;
}

class NamedSession {
  constructor(factory = support.createClient) {
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

async function checkpoint(session, target, source, deadline) {
  while (true) {
    support.remaining(deadline);
    try {
      const status = await session.channel.getChannelStatus();
      if (status.rowsErrorCount) throw new Error("Reconcile row errors before source handoff");
      if (status.statusCode !== "SUCCESS") {
        throw new StreamingIngestError("InvalidChannelError", status.statusCode, 409, "Conflict");
      }
      if (parseOffset(status.latestCommittedOffsetToken) >= target) {
        source.acknowledge(target);
        return;
      }
    } catch (error) {
      if (support.INVALIDATION.has(error.errorCode) || !support.retryable(error)) throw error;
    }
    await support.backoff(2, deadline);
  }
}

async function run(session, source) {
  source.seek(await session.open());
  let submitted = source.committed;
  let outstanding = 0;
  let failures = 0;
  let event = null;
  let deadline = performance.now() + support.OUTAGE_MS;
  let checkpointAt = performance.now() + support.CHECKPOINT_MS;
  while (true) {
    try {
      if (event === null) event = source.read();
      if (event === null) {
        if (outstanding) await checkpoint(session, submitted, source, deadline);
        return;
      }
      support.remaining(deadline);
      session.channel.appendRow(event.row, String(event.offset));
      submitted = event.offset;
      event = null;
      outstanding++;
      if (outstanding >= support.CHECKPOINT_ROWS || performance.now() >= checkpointAt) {
        await checkpoint(session, submitted, source, deadline);
        outstanding = 0;
        failures = 0;
        deadline = performance.now() + support.OUTAGE_MS;
        checkpointAt = performance.now() + support.CHECKPOINT_MS;
      }
    } catch (error) {
      if (!support.retryable(error) || ++failures >= support.MAX_ATTEMPTS) throw error;
      if (support.INVALIDATION.has(error.errorCode)) {
        source.seek(await session.recover(error));
        submitted = source.committed;
        outstanding = 0;
        event = null;
      }
      await support.backoff(failures - 1, deadline);
    }
  }
}

async function main() {
  const source = support.sourceFromEnv();
  const session = new NamedSession();
  let completed = false;
  try {
    await run(session, source);
    completed = true;
    console.log(`Committed source checkpoint: ${source.committed}`);
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

module.exports = { NamedSession, parseOffset, checkpoint, run, main };
