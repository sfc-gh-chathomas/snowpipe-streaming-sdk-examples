/**
 * Coordinate retained source offsets with one stable named channel.
 *
 * The offset token is checkpoint metadata, not a deduplication key. Give each
 * channel name one owner and retain events until committed progress is confirmed.
 */
"use strict";

const streaming = require("snowpipe-streaming");
const support = require("./elastic_step3_production.js");

const CHANNEL_NAME = process.env.SNOWFLAKE_CHANNEL || "production-source-0";
const CHECKPOINT_ROWS = 1_000;
const CHECKPOINT_MS = 5_000;

function parseOffset(token) {
  if (token == null) return 0;
  const offset = Number(token);
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new Error("Invalid committed source offset");
  }
  return offset;
}

class NamedProducer {
  constructor(factory = support.createClient) {
    this.factory = factory;
    this.client = null;
    this.channel = null;
  }

  async open() {
    if (!this.client) this.client = await this.factory();
    const opened = await this.client.openChannel({ name: CHANNEL_NAME });
    this.channel = opened.channel;
    if (opened.status.rowsErrorCount) {
      throw new Error("Row errors require reconciliation before source handoff");
    }
    return parseOffset(opened.status.latestCommittedOffsetToken);
  }

  async recover(error) {
    // Reopen without replacing the server-side committed offset.
    if (error.errorCode === "InvalidClientError") {
      await this.close(false);
    } else if (this.channel) {
      await this.channel.close({ waitForFlush: false, timeoutMs: 0 }).catch(() => {});
    }

    try {
      return await this.open();
    } catch (reopened) {
      if (!["InvalidClientError", "ClosedClientError"].includes(reopened.errorCode)) {
        throw reopened;
      }
      await this.close(false);
      return this.open();
    }
  }

  async close(flush) {
    if (!this.client) return;
    try {
      await this.client.close({ waitForFlush: flush, timeoutMs: 30_000 });
    } finally {
      this.client = null;
      this.channel = null;
    }
  }
}

async function collectProgress(producer, submitted, source) {
  const status = await producer.channel.getChannelStatus();
  if (status.rowsErrorCount) {
    throw new Error("Row errors require reconciliation before source handoff");
  }
  if (status.statusCode !== "SUCCESS") {
    throw new streaming.StreamingIngestError(
      "InvalidChannelError",
      status.statusCode,
      409,
      "Conflict",
    );
  }

  const committed = Math.min(
    submitted,
    parseOffset(status.latestCommittedOffsetToken),
  );
  if (committed > source.committed) source.acknowledge(committed);
}

async function run(producer, source) {
  // Snowflake's committed token determines where this retained source resumes.
  source.seek(await producer.open());
  let submitted = source.committed;
  let event = null;
  let exhausted = false;
  let failures = 0;
  let rowsSincePoll = 0;
  let nextPoll = performance.now() + CHECKPOINT_MS;
  let deadline = performance.now() + support.MAX_NO_PROGRESS_MS;

  while (true) {
    try {
      const outstanding = submitted > source.committed;
      const shouldPoll = outstanding
        && (exhausted
          || event
          || rowsSincePoll >= CHECKPOINT_ROWS
          || performance.now() >= nextPoll
          || submitted - source.committed >= support.MAX_PENDING_EVENTS);
      if (shouldPoll) {
        const previous = source.committed;
        await collectProgress(producer, submitted, source);
        if (source.committed > previous) {
          deadline = performance.now() + support.MAX_NO_PROGRESS_MS;
          failures = 0;
        }
        rowsSincePoll = 0;
        nextPoll = performance.now() + CHECKPOINT_MS;
      }

      if (submitted === source.committed && !event) {
        deadline = performance.now() + support.MAX_NO_PROGRESS_MS;
        if (exhausted) return;
      }

      support.remaining(deadline);
      if (exhausted
        || submitted - source.committed >= support.MAX_PENDING_EVENTS) {
        await new Promise((resolve) => {
          setTimeout(resolve, Math.min(support.POLL_MS, support.remaining(deadline)));
        });
        continue;
      }

      if (!event) event = source.read();
      if (!event) {
        exhausted = true;
        continue;
      }

      producer.channel.appendRow(event.row, String(event.offset));
      submitted = event.offset;
      rowsSincePoll++;
      event = null;
    } catch (error) {
      if (!support.isRetryable(error)) throw error;
      if (error.httpStatusCode !== 429 && ++failures >= support.MAX_ATTEMPTS) {
        throw error;
      }
      if (support.isInvalidation(error)) {
        const previous = source.committed;
        source.seek(await producer.recover(error));
        if (source.committed > previous) {
          deadline = performance.now() + support.MAX_NO_PROGRESS_MS;
        }
        submitted = source.committed;
        event = null;
        exhausted = false;
      }
      await support.backoff(2, deadline);
    }
  }
}

async function main() {
  const source = new support.SampleEventSource(
    Number(process.env.SNOWFLAKE_TEST_ROWS || 10_000),
  );
  const producer = new NamedProducer();
  let completed = false;
  try {
    await run(producer, source);
    completed = true;
    console.log(`Committed source checkpoint: ${source.committed}`);
  } finally {
    if (!completed) {
      console.error(`Retain source events after checkpoint ${source.committed}`);
    }
    await producer.close(completed);
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
  NamedProducer,
  collectProgress,
  main,
  parseOffset,
  run,
};
