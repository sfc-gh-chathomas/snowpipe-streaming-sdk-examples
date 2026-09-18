/**
 * Coordinate retained source offsets with one stable named channel.
 *
 * The offset token is checkpoint metadata, not a deduplication key. Give each
 * channel name one owner and retain events until committed progress is confirmed.
 */
"use strict";

const streaming = require("snowpipe-streaming");

const MAX_PENDING_EVENTS = 100_000;
const MAX_NO_PROGRESS_MS = 30 * 60_000;
const POLL_MS = 1_000;
const MAX_ATTEMPTS = 6;
const INVALIDATION_ERRORS = new Set([
  "InvalidChannelError",
  "InvalidClientError",
  "ClosedChannelError",
  "ClosedElasticChannelError",
  "ClosedClientError",
]);

async function createClient() {
  return streaming.createTableClient({
    clientName: `production-${process.pid}`,
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json",
  });
}

class SampleEventSource {
  constructor(total = 10_000, checkpoint = 0) {
    if (!Number.isSafeInteger(total) || checkpoint < 0 || checkpoint > total) {
      throw new Error("Require integer 0 <= checkpoint <= total");
    }
    this.total = total;
    this.committed = checkpoint;
    this.nextOffset = checkpoint + 1;
  }

  read() {
    if (this.nextOffset > this.total) return null;
    const offset = this.nextOffset++;
    return {
      offset,
      row: { EVENT_ID: offset, C1: offset, C2: `event-${offset}` },
    };
  }

  acknowledge(offset) {
    if (offset < this.committed || offset > this.total) {
      throw new Error("Invalid source checkpoint");
    }
    this.committed = offset;
  }

  seek(committed) {
    this.acknowledge(committed);
    this.nextOffset = committed + 1;
  }
}

function isInvalidation(error) {
  return INVALIDATION_ERRORS.has(error.errorCode);
}

function isRetryable(error) {
  return error instanceof streaming.StreamingIngestError
    && (isInvalidation(error)
      || [408, 429, 500, 502, 503, 504].includes(error.httpStatusCode));
}

function remaining(deadline) {
  const millis = deadline - performance.now();
  if (millis <= 0) {
    throw new Error("No confirmed progress before deadline; retain unconfirmed events");
  }
  return millis;
}

async function backoff(attempt, deadline) {
  const cap = Math.min(10_000, 250 * 2 ** Math.min(attempt, 6));
  const delay = Math.min(remaining(deadline), Math.random() * cap);
  await new Promise((resolve) => setTimeout(resolve, delay));
}

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
  constructor(factory = createClient) {
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
  let deadline = performance.now() + MAX_NO_PROGRESS_MS;

  while (true) {
    try {
      const outstanding = submitted > source.committed;
      const shouldPoll = outstanding
        && (exhausted
          || event
          || rowsSincePoll >= CHECKPOINT_ROWS
          || performance.now() >= nextPoll
          || submitted - source.committed >= MAX_PENDING_EVENTS);
      if (shouldPoll) {
        const previous = source.committed;
        await collectProgress(producer, submitted, source);
        if (source.committed > previous) {
          deadline = performance.now() + MAX_NO_PROGRESS_MS;
          failures = 0;
        }
        rowsSincePoll = 0;
        nextPoll = performance.now() + CHECKPOINT_MS;
      }

      if (submitted === source.committed && !event) {
        deadline = performance.now() + MAX_NO_PROGRESS_MS;
        if (exhausted) return;
      }

      remaining(deadline);
      if (exhausted
        || submitted - source.committed >= MAX_PENDING_EVENTS) {
        await new Promise((resolve) => {
          setTimeout(resolve, Math.min(POLL_MS, remaining(deadline)));
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
      if (!isRetryable(error)) throw error;
      if (error.httpStatusCode !== 429 && ++failures >= MAX_ATTEMPTS) {
        throw error;
      }
      if (isInvalidation(error)) {
        const previous = source.committed;
        source.seek(await producer.recover(error));
        if (source.committed > previous) {
          deadline = performance.now() + MAX_NO_PROGRESS_MS;
        }
        submitted = source.committed;
        event = null;
        exhausted = false;
      }
      await backoff(2, deadline);
    }
  }
}

async function main() {
  const source = new SampleEventSource(
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
  MAX_ATTEMPTS,
  MAX_NO_PROGRESS_MS,
  MAX_PENDING_EVENTS,
  POLL_MS,
  SampleEventSource,
  NamedProducer,
  backoff,
  collectProgress,
  createClient,
  isInvalidation,
  isRetryable,
  main,
  parseOffset,
  remaining,
  run,
};
