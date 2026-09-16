"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const streaming = require("snowpipe-streaming");
const elastic = require("../elastic_step3_production.js");

const error = (code, status) =>
  new streaming.StreamingIngestError(code, "synthetic", status, String(status));
const deadline = () => performance.now() + 500;
const tick = () => new Promise((resolve) => setImmediate(resolve));

function fakeClient(outcomes = []) {
  const calls = [];
  const closes = [];
  const channel = {
    appendRowWithWait(row, token) {
      calls.push(token);
      const outcome = outcomes.shift();
      if (outcome instanceof Error) return Promise.reject(outcome);
      return outcome || Promise.resolve();
    },
  };
  return {
    calls,
    closes,
    channel,
    getElasticChannel: async () => channel,
    close: async (options) => closes.push(options),
  };
}

test("late and out-of-order success cannot acknowledge a gap", async () => {
  let resolveFirst;
  const first = new Promise((resolve) => {
    resolveFirst = resolve;
  });
  const client = fakeClient([first, Promise.resolve()]);
  const producer = new elastic.ElasticProducer(async () => client);
  await producer.open();
  const source = new elastic.SampleEventSource(2);
  const pending = [
    elastic.appendEvent(producer, source.read()),
    elastic.appendEvent(producer, source.read()),
  ];

  await tick();
  await elastic.collectProgress(producer, pending, source, deadline());
  assert.equal(source.committed, 0);

  resolveFirst();
  await tick();
  await elastic.collectProgress(producer, pending, source, deadline());
  assert.equal(source.committed, 2);
});

test("late errors from a replaced client swap only once", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const old = fakeClient([
    Promise.resolve(),
    error("InvalidChannelError", 409),
    error("InvalidClientError", 409),
  ]);
  const fresh = fakeClient();
  const clients = [old, fresh];
  const producer = new elastic.ElasticProducer(async () => clients.shift());
  await producer.open();
  const source = new elastic.SampleEventSource(3);
  const pending = Array.from(
    { length: 3 },
    () => elastic.appendEvent(producer, source.read()),
  );

  while (pending.length) {
    await tick();
    await elastic.collectProgress(producer, pending, source, deadline());
  }

  assert.deepEqual(fresh.calls, ["2", "3"]);
  assert.equal(old.closes.length, 1);
  assert.equal(source.committed, 3);
});

test("429 retries only the rejected event", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const client = fakeClient([error("ReceiverSaturated", 429)]);
  const producer = new elastic.ElasticProducer(async () => client);
  await producer.open();
  const source = new elastic.SampleEventSource(1);

  await elastic.run(producer, source);

  assert.deepEqual(client.calls, ["1", "1"]);
  assert.equal(source.committed, 1);
});

test("terminal error preserves the source checkpoint", async () => {
  const client = fakeClient([error("SfApiUserError", 400)]);
  const producer = new elastic.ElasticProducer(async () => client);
  await producer.open();
  const source = new elastic.SampleEventSource(1);

  await assert.rejects(elastic.run(producer, source));

  assert.equal(source.committed, 0);
  assert.deepEqual(client.calls, ["1"]);
});

test("retry exhaustion preserves the source checkpoint", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const failures = Array.from(
    { length: elastic.MAX_ATTEMPTS },
    () => error("HttpRetriesExhaustedError", 503),
  );
  const client = fakeClient(failures);
  const producer = new elastic.ElasticProducer(async () => client);
  await producer.open();
  const source = new elastic.SampleEventSource(1);

  await assert.rejects(elastic.run(producer, source));

  assert.equal(client.calls.length, elastic.MAX_ATTEMPTS);
  assert.equal(source.committed, 0);
});

test("sample source replay is deterministic", () => {
  const source = new elastic.SampleEventSource(3);
  source.read();
  const second = source.read();

  assert.deepEqual(new elastic.SampleEventSource(3, 1).read(), second);
  assert.equal(source.committed, 0);
});
