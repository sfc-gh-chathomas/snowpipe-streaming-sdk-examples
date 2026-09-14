"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { StreamingIngestError } = require("snowpipe-streaming");
const elastic = require("../elastic_production.js");
const named = require("../named_channel_checkpoint.js");
const support = require("../production_support.js");

const error = (code, status) => new StreamingIngestError(code, "synthetic", status, String(status));
const deadline = () => performance.now() + 500;

function elasticClient(outcomes = []) {
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
  return { calls, closes, channel,
    getElasticChannel: async () => channel,
    close: async (options) => { closes.push(options); },
  };
}

test("Elastic appends before reading the next retained event", async () => {
  const client = elasticClient();
  const session = new elastic.ElasticSession(async () => client);
  await session.open();
  class Source extends support.ReplaySource {
    read() {
      assert.equal(client.calls.length, this.nextOffset - 1);
      return super.read();
    }
  }
  const source = new Source(3);
  await elastic.run(session, source);
  assert.equal(source.committed, 3);
  assert.deepEqual(client.calls, ["1", "2", "3"]);
});

test("caller polling timeout retains original promise; late success advances checkpoint", async () => {
  let resolve;
  const original = new Promise((done) => { resolve = done; });
  const client = elasticClient([original]);
  const session = new elastic.ElasticSession(async () => client);
  await session.open();
  const source = new support.ReplaySource(1);
  const pending = [elastic.submit(session, source.read())];
  const waiting = await support.poll(pending[0].outcome, 1);
  assert.equal(waiting.waiting, true);
  assert.equal(source.committed, 0);
  resolve();
  await elastic.checkpoint(session, pending, source, deadline());
  assert.deepEqual(client.calls, ["1"]);
  assert.deepEqual(client.closes, []);
  assert.equal(session.generation, 1);
  assert.equal(source.committed, 1);
});

test("out-of-order success cannot acknowledge an earlier gap", async () => {
  const client = elasticClient([new Promise(() => {}), Promise.resolve()]);
  const session = new elastic.ElasticSession(async () => client);
  await session.open();
  const source = new support.ReplaySource(2);
  const pending = [elastic.submit(session, source.read()), elastic.submit(session, source.read())];
  await assert.rejects(elastic.checkpoint(session, pending, source, performance.now() + 5), /Outage/);
  assert.equal(source.committed, 0);
  assert.equal(pending.length, 2);
  assert.deepEqual(client.closes, []);
});

test("checkpoint count bounds source intake", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const limit = support.CHECKPOINT_ROWS;
  let resolve;
  const blocked = new Promise((done) => { resolve = done; });
  const client = elasticClient([blocked]);
  const session = new elastic.ElasticSession(async () => client);
  await session.open();
  const source = new support.ReplaySource(limit + 1);
  const running = elastic.run(session, source);
  await new Promise((done) => setTimeout(done, 10));
  assert.equal(client.calls.length, limit);
  assert.equal(source.nextOffset, limit + 1);
  assert.equal(source.committed, 0);
  resolve();
  await running;
  assert.equal(source.committed, limit + 1);
});

test("429 retries the rejected event on the same client", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const client = elasticClient([error("ReceiverSaturated", 429)]);
  const session = new elastic.ElasticSession(async () => client);
  await session.open();
  const source = new support.ReplaySource(1);
  await elastic.run(session, source);
  assert.deepEqual(client.calls, ["1", "1"]);
  assert.deepEqual(client.closes, []);
  assert.equal(source.committed, 1);
});

test("SDK invalidation rebuilds once for a failed generation and skips successful events", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const old = elasticClient([Promise.resolve(), error("InvalidChannelError", 409), error("InvalidClientError", 409)]);
  const fresh = elasticClient();
  const clients = [old, fresh];
  const session = new elastic.ElasticSession(async () => clients.shift());
  await session.open();
  const source = new support.ReplaySource(3);
  const pending = Array.from({ length: 3 }, () => elastic.submit(session, source.read()));
  await elastic.checkpoint(session, pending, source, deadline());
  assert.deepEqual(fresh.calls, ["2", "3"]);
  assert.equal(old.closes.length, 1);
  assert.equal(session.generation, 2);
  assert.equal(source.committed, 3);
});

for (const status of [400, 401, 403, 404]) {
  test(`permanent ${status} preserves retained source checkpoint`, async () => {
    const client = elasticClient([error("SfApiUserError", status)]);
    const session = new elastic.ElasticSession(async () => client);
    await session.open();
    const source = new support.ReplaySource(1);
    await assert.rejects(elastic.run(session, source), (failure) => failure.httpStatusCode === status);
    assert.equal(source.committed, 0);
    assert.deepEqual(client.calls, ["1"]);
  });
}

test("terminal SDK retry exhaustion stops with uncommitted source work", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const client = elasticClient(Array.from({ length: support.MAX_ATTEMPTS }, () => error("HttpRetriesExhaustedError", 503)));
  const session = new elastic.ElasticSession(async () => client);
  await session.open();
  const source = new support.ReplaySource(1);
  await assert.rejects(elastic.run(session, source));
  assert.equal(client.calls.length, support.MAX_ATTEMPTS);
  assert.equal(source.committed, 0);
});

function namedSession(committed = 0) {
  const session = { committed, calls: [], recoveries: 0, polls: 0,
    open: async () => session.committed,
    recover: async () => { session.recoveries++; return session.committed; },
  };
  session.channel = {
    appendRow(row, token) {
      session.calls.push(Number(token));
      session.onAppend?.(Number(token));
      session.committed = Number(token);
    },
    async getChannelStatus() {
      session.polls++;
      return { statusCode: "SUCCESS", rowsErrorCount: 0, latestCommittedOffsetToken: String(session.committed) };
    },
  };
  return session;
}

test("named restart seeks strictly after server committed offset", async () => {
  const session = namedSession(2);
  const source = new support.ReplaySource(5);
  await named.run(session, source);
  assert.deepEqual(session.calls, [3, 4, 5]);
  assert.equal(session.polls, 1);
  assert.equal(source.committed, 5);
});

test("named backpressure retains current event", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const session = namedSession();
  const source = new support.ReplaySource(3);
  session.onAppend = (offset) => {
    assert.equal(source.nextOffset, offset + 1);
    session.onAppend = null;
    throw error("ReceiverSaturated", 429);
  };
  await named.run(session, source);
  assert.deepEqual(session.calls, [1, 1, 2, 3]);
  assert.equal(session.recoveries, 0);
});

test("named invalidation resumes from server offset without resetting it", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const session = namedSession();
  session.onAppend = (offset) => {
    if (offset === 3) {
      session.onAppend = null;
      throw error("InvalidChannelError", 409);
    }
  };
  const source = new support.ReplaySource(4);
  await named.run(session, source);
  assert.deepEqual(session.calls, [1, 2, 3, 3, 4]);
  assert.equal(session.recoveries, 1);
  assert.equal(source.committed, 4);
});

test("named delayed status keeps intake paused without reopening", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const session = namedSession();
  const source = new support.ReplaySource(2);
  let polls = 0;
  session.channel.getChannelStatus = async () => {
    assert.equal(source.committed, 0);
    assert.equal(source.nextOffset, 3);
    return { statusCode: "SUCCESS", rowsErrorCount: 0, latestCommittedOffsetToken: ++polls > 1 ? "2" : "0" };
  };
  await named.run(session, source);
  assert.equal(session.recoveries, 0);
  assert.equal(source.committed, 2);
  assert.deepEqual(session.calls, [1, 2]);
});

test("named closed channel recovers from committed offset", async (context) => {
  context.mock.method(support, "backoff", async () => {});
  const session = namedSession();
  session.onAppend = () => {
    session.onAppend = null;
    throw error("ClosedChannelError", 409);
  };
  const source = new support.ReplaySource(2);
  await named.run(session, source);
  assert.deepEqual(session.calls, [1, 1, 2]);
  assert.equal(session.recoveries, 1);
  assert.equal(source.committed, 2);
});

test("named row errors and permanent failures prevent handoff", async () => {
  const session = namedSession();
  session.channel.getChannelStatus = async () => ({ statusCode: "SUCCESS", rowsErrorCount: 1, latestCommittedOffsetToken: "2" });
  const source = new support.ReplaySource(2);
  await assert.rejects(named.run(session, source), /row errors/);
  assert.equal(source.committed, 0);
});

test("replay source regenerates stable payload and source checkpoint is explicit", () => {
  const source = new support.ReplaySource(3);
  source.read();
  const event = source.read();
  assert.deepEqual(new support.ReplaySource(3, 1).read(), event);
  assert.equal(source.committed, 0);
});
