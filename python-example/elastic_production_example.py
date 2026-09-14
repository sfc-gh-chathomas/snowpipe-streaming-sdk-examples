"""Append events immediately; checkpoint all acknowledgements before source handoff.

The SDK owns batching and transport retries. Caller timeouts keep the original
Future alive. Retain/replay unacknowledged source events across restarts; Elastic
replay may duplicate events, so EVENT_ID must be stable and source-unique.
"""

import logging
import time
from dataclasses import dataclass

from snowflake.ingest.streaming import StreamingIngestError
import production_support as support


@dataclass
class Pending:
    event: tuple
    future: object
    generation: int


class ElasticSession:
    def __init__(self, factory=support.create_client):
        self.factory = factory
        self.client = None
        self.generation = 0

    def open(self):
        client = self.factory()
        try:
            self.channel = client.get_elastic_channel()
        except BaseException:
            client.close(wait_for_flush=False, timeout_seconds=0)
            raise
        self.client = client
        self.generation += 1

    def recover(self, generation):
        if generation != self.generation:
            return
        self.close(False)
        self.open()

    def close(self, flush):
        if self.client is not None:
            try:
                self.client.close(wait_for_flush=flush, timeout_seconds=30)
            finally:
                self.client = None


def submit(session, event, deadline):
    for attempt in range(support.MAX_ATTEMPTS):
        support.remaining(deadline)
        try:
            return Pending(event, session.channel.append_row_with_wait(event[1], str(event[0])),
                           session.generation)
        except StreamingIngestError as error:
            if not support.retryable(error) or attempt == support.MAX_ATTEMPTS - 1:
                raise
            if support.code(error) in support.INVALIDATION:
                session.recover(session.generation)
            support.backoff(attempt, deadline)
    raise RuntimeError("Submission retry budget exhausted")


def checkpoint(session, pending, source, deadline):
    for item in pending:
        retries = 0
        while True:
            budget = support.remaining(deadline)
            try:
                item.future.result(timeout=min(support.POLL_SECONDS, budget))
                break
            except TimeoutError:
                # A caller timeout neither cancels nor resubmits this append.
                continue
            except StreamingIngestError as error:
                if not support.retryable(error) or retries >= support.MAX_ATTEMPTS - 1:
                    raise
                if support.code(error) in support.INVALIDATION:
                    session.recover(item.generation)
                logging.warning("Replaying EVENT_ID=%s after terminal SDK failure; duplicates possible",
                                item.event[0])
                support.backoff(retries, deadline)
                item = submit(session, item.event, deadline)
                retries += 1
    if pending:
        source.acknowledge(pending[-1].event[0])
        pending.clear()


def run(session, source):
    pending = []
    checkpoint_at = time.monotonic() + support.CHECKPOINT_SECONDS
    deadline = time.monotonic() + support.OUTAGE_SECONDS
    while True:
        event = source.read()
        if event is None:
            break
        pending.append(submit(session, event, deadline))
        failed = any(item.future.done() and item.future.exception() is not None for item in pending)
        if failed or len(pending) >= support.CHECKPOINT_ROWS or time.monotonic() >= checkpoint_at:
            checkpoint(session, pending, source, deadline)
            checkpoint_at = time.monotonic() + support.CHECKPOINT_SECONDS
            deadline = time.monotonic() + support.OUTAGE_SECONDS
    checkpoint(session, pending, source, deadline)


def main():
    source = support.source_from_env()
    session = ElasticSession()
    completed = False
    try:
        session.open()
        run(session, source)
        completed = True
        print(f"Durable source checkpoint: {source.committed}; materialization is separate")
    finally:
        if not completed:
            print(f"Stopped. Retain source events after checkpoint {source.committed} for replay")
        session.close(completed)


if __name__ == "__main__":
    main()
