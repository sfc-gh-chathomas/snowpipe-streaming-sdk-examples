/**
 * Elastic step 3: retain source events until acknowledgements confirm progress.
 *
 * Replaying an Elastic append can create a duplicate, so a real source must
 * retain events and provide a stable event ID.
 */
"use strict";

const streaming = require("snowpipe-streaming");

const MAX_PENDING_EVENTS = 100_000;
const CHECKPOINT_ROWS = 1_000;
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

// Connection and source model

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
    // Replace this with the source's durable checkpoint operation.
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

// Client lifecycle

class ElasticProducer {
  constructor(factory = createClient) {
    this.factory = factory;
    this.client = null;
    this.channel = null;
  }

  async open() {
    const client = await this.factory();
    try {
      this.channel = await client.getElasticChannel();
    } catch (error) {
      await client.close({ waitForFlush: false, timeoutMs: 0 });
      throw error;
    }
    this.client = client;
  }

  async swapClient(failedClient) {
    // Late failures from an old client must not close its replacement.
    if (failedClient !== this.client) return;
    await this.close(false);
    await this.open();
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

// Retry policy

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

// Append and acknowledgement handling

function appendEvent(producer, event, retries = 0) {
  let promise;
  try {
    promise = producer.channel.appendRowWithWait(event.row, String(event.offset));
  } catch (error) {
    promise = Promise.reject(error);
  }

  const pending = {
    event,
    client: producer.client,
    retries,
    result: null,
  };
  pending.outcome = Promise.resolve(promise)
    .then(() => ({ ok: true }), (error) => ({ error }))
    .then((result) => {
      pending.result = result;
      return result;
    });
  return pending;
}

async function collectProgress(producer, pending, source, deadline) {
  let confirmed = 0;
  for (const item of pending) {
    if (!item.result) break;
    if (item.result.error) {
      if (confirmed) break;
      const error = item.result.error;
      if (!isRetryable(error) || item.retries >= MAX_ATTEMPTS - 1) throw error;
      if (isInvalidation(error)) await producer.swapClient(item.client);
      await backoff(item.retries, deadline);
      pending[0] = appendEvent(producer, item.event, item.retries + 1);
      return;
    }
    confirmed++;
  }

  if (confirmed) {
    source.acknowledge(pending[confirmed - 1].event.offset);
    pending.splice(0, confirmed);
  }
}

async function waitForOutcome(item, timeoutMs) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(resolve, timeoutMs);
  });
  await Promise.race([item.outcome, timeout]);
  clearTimeout(timer);
}

// Ingestion flow

async function run(producer, source) {
  const pending = [];
  let event = null;
  let exhausted = false;
  let submittedSinceYield = 0;
  let deadline = performance.now() + MAX_NO_PROGRESS_MS;

  while (true) {
    const previous = source.committed;
    if (pending.length
      && (exhausted || event || submittedSinceYield === 0
        || pending.length >= MAX_PENDING_EVENTS)) {
      await collectProgress(producer, pending, source, deadline);
    }
    if (source.committed > previous || (!pending.length && !event)) {
      deadline = performance.now() + MAX_NO_PROGRESS_MS;
    }
    if (exhausted && !pending.length) return;
    remaining(deadline);

    if (exhausted || pending.length >= MAX_PENDING_EVENTS) {
      await waitForOutcome(
        pending[0],
        Math.min(POLL_MS, remaining(deadline)),
      );
      continue;
    }

    if (!event) event = source.read();
    if (!event) {
      exhausted = true;
      continue;
    }

    const item = appendEvent(producer, event);
    await Promise.resolve();
    await Promise.resolve();
    if (item.result?.error?.httpStatusCode === 429) {
      await backoff(2, deadline);
      continue;
    }
    pending.push(item);
    event = null;

    if (++submittedSinceYield >= CHECKPOINT_ROWS) {
      await new Promise((resolve) => setImmediate(resolve));
      submittedSinceYield = 0;
    }
  }
}

async function main() {
  const source = new SampleEventSource(
    Number(process.env.SNOWFLAKE_TEST_ROWS || 10_000),
    Number(process.env.SNOWFLAKE_SOURCE_CHECKPOINT || 0),
  );
  const producer = new ElasticProducer();
  let completed = false;
  try {
    await producer.open();
    await run(producer, source);
    completed = true;
    console.log(`Durable source checkpoint: ${source.committed}`);
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
  ElasticProducer,
  appendEvent,
  backoff,
  collectProgress,
  createClient,
  isInvalidation,
  isRetryable,
  remaining,
  run,
};
