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

function observe(promise) {
  return Promise.resolve(promise).then(() => ({ ok: true }), (error) => ({ error }));
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

class ReplaySource {
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
    return { offset, row: { EVENT_ID: offset, C1: offset, C2: `event-${offset}` } };
  }
  acknowledge(offset) {
    if (offset < this.committed || offset > this.total) throw new Error("Invalid source checkpoint");
    this.committed = offset;
  }
  seek(committed) {
    this.acknowledge(committed);
    this.nextOffset = committed + 1;
  }
}

function sourceFromEnv() {
  return new ReplaySource(Number(process.env.SNOWFLAKE_TEST_ROWS || 10_000),
    Number(process.env.SNOWFLAKE_SOURCE_CHECKPOINT || 0));
}

module.exports = { CHECKPOINT_ROWS, CHECKPOINT_MS, OUTAGE_MS, POLL_MS, MAX_ATTEMPTS,
  INVALIDATION, retryable, remaining, backoff, poll, observe, createClient, ReplaySource, sourceFromEnv };
