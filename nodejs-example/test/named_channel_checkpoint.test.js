"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const streaming = require("snowpipe-streaming");
const named = require("../named_channel_checkpoint.js");

const error = (code, status) =>
  new streaming.StreamingIngestError(code, "synthetic", status, String(status));

function fakeProducer(committed = 0) {
  const producer = {
    committed,
    calls: [],
    recoveries: 0,
    open: async () => producer.committed,
    recover: async () => {
      producer.recoveries++;
      return producer.committed;
    },
  };
  producer.channel = {
    appendRow(row, token) {
      const offset = Number(token);
      producer.calls.push(offset);
      producer.onAppend?.(offset);
      producer.committed = offset;
    },
    async getChannelStatus() {
      return {
        statusCode: "SUCCESS",
        rowsErrorCount: 0,
        latestCommittedOffsetToken: String(producer.committed),
      };
    },
  };
  return producer;
}

test("restart seeks after the server offset", async () => {
  const producer = fakeProducer(2);
  const source = new named.SampleEventSource(5);

  await named.run(producer, source);

  assert.deepEqual(producer.calls, [3, 4, 5]);
  assert.equal(source.committed, 5);
});

test("backpressure retries the current event without reopening", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const producer = fakeProducer();
  producer.onAppend = () => {
    producer.onAppend = null;
    throw error("ReceiverSaturated", 429);
  };
  const source = new named.SampleEventSource(2);

  await named.run(producer, source);

  assert.deepEqual(producer.calls, [1, 1, 2]);
  assert.equal(producer.recoveries, 0);
});

test("invalidation resumes from committed progress", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const producer = fakeProducer();
  producer.onAppend = (offset) => {
    if (offset === 3) {
      producer.onAppend = null;
      throw error("InvalidChannelError", 409);
    }
  };
  const source = new named.SampleEventSource(4);

  await named.run(producer, source);

  assert.deepEqual(producer.calls, [1, 2, 3, 3, 4]);
  assert.equal(producer.recoveries, 1);
  assert.equal(source.committed, 4);
});

test("row errors prevent source handoff", async () => {
  const producer = fakeProducer();
  producer.channel.getChannelStatus = async () => ({
    statusCode: "SUCCESS",
    rowsErrorCount: 1,
    latestCommittedOffsetToken: "2",
  });
  const source = new named.SampleEventSource(2);

  await assert.rejects(named.run(producer, source), /row errors/i);

  assert.equal(source.committed, 0);
});
