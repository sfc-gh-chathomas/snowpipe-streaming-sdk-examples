"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const continuous = require("../elastic_step2_continuous.js");

test("step 2 launches up to the limit and drains all acknowledgements", async () => {
  const previous = process.env.SNOWFLAKE_TEST_ROWS;
  process.env.SNOWFLAKE_TEST_ROWS = String(continuous.MAX_PENDING_EVENTS + 1);
  let releaseFirst;
  const first = new Promise((resolve) => {
    releaseFirst = resolve;
  });
  const calls = [];
  const rows = [];
  const channel = {
    appendRowWithWait(row, token) {
      rows.push(row);
      calls.push(token);
      if (calls.length === continuous.MAX_PENDING_EVENTS) releaseFirst();
      return calls.length === 1 ? first : Promise.resolve();
    },
  };
  const closes = [];
  const client = {
    getElasticChannel: async () => channel,
    close: async (options) => closes.push(options),
  };

  try {
    await continuous.main(async () => client);
  } finally {
    if (previous === undefined) delete process.env.SNOWFLAKE_TEST_ROWS;
    else process.env.SNOWFLAKE_TEST_ROWS = previous;
  }

  assert.equal(calls.length, continuous.MAX_PENDING_EVENTS + 1);
  assert.ok(calls.every((token) => token === null));
  assert.equal(rows[0].EVENT_ID, 0);
  assert.equal(rows.at(-1).EVENT_ID, continuous.MAX_PENDING_EVENTS);
  assert.deepEqual(closes, [{ waitForFlush: true, timeoutMs: 60_000 }]);
});

test("step 2 observes asynchronous rejection and closes without flushing", async () => {
  const previous = process.env.SNOWFLAKE_TEST_ROWS;
  process.env.SNOWFLAKE_TEST_ROWS = "1";
  const channel = {
    appendRowWithWait: () => Promise.reject(new Error("delivery failed")),
  };
  const closes = [];
  const client = {
    getElasticChannel: async () => channel,
    close: async (options) => closes.push(options),
  };

  try {
    await assert.rejects(
      continuous.main(async () => client),
      /delivery failed/,
    );
  } finally {
    if (previous === undefined) delete process.env.SNOWFLAKE_TEST_ROWS;
    else process.env.SNOWFLAKE_TEST_ROWS = previous;
  }

  assert.deepEqual(closes, [{ waitForFlush: false, timeoutMs: 60_000 }]);
});
