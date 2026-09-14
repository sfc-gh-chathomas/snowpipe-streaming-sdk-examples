"""Single-writer named-channel producer with source-offset recovery.

Stream individual rows and checkpoint committed offsets, never local submission.
Source events remain replayable until confirmed. A caller timeout pauses reading;
only SDK invalidation reopens the channel. Do not share channel ownership.
"""

import os
import time

from snowflake.ingest.streaming import StreamingIngestError, StreamingIngestErrorCode
import production_support as support

CHANNEL_NAME = os.environ.get("SNOWFLAKE_CHANNEL", "production-source-0")


def parse_offset(token):
    return 0 if token is None else int(token)


class NamedSession:
    def __init__(self, factory=support.create_client):
        self.factory = factory
        self.client = None
        self.channel = None

    def open(self):
        if self.client is None:
            self.client = self.factory()
        self.channel, status = self.client.open_channel(CHANNEL_NAME)
        if status.rows_error_count:
            raise RuntimeError("Row errors require reconciliation before source handoff")
        return parse_offset(status.latest_committed_offset_token)

    def recover(self, error):
        if support.code(error) == "InvalidClientError":
            self.close(False)
        elif self.channel is not None:
            try:
                self.channel.close(wait_for_flush=False, timeout_seconds=0)
            except StreamingIngestError:
                pass
        try:
            return self.open()
        except StreamingIngestError as reopened:
            if support.code(reopened) not in {"InvalidClientError", "ClosedClientError"}:
                raise
            self.close(False)
            return self.open()

    def close(self, flush):
        if self.client is not None:
            try:
                self.client.close(wait_for_flush=flush, timeout_seconds=30)
            finally:
                self.client = None


def checkpoint(session, target, source, deadline):
    while True:
        budget = support.remaining(deadline)
        try:
            session.channel.wait_for_commit(
                lambda token: parse_offset(token) >= target,
                timeout_seconds=max(1, min(5, int(budget))),
            )
            status = session.channel.get_channel_status()
            if status.rows_error_count:
                raise RuntimeError("Row errors require reconciliation before source handoff")
            if status.status_code != "SUCCESS":
                raise StreamingIngestError(StreamingIngestErrorCode.INVALID_CHANNEL_ERROR,
                                           status.status_code, 409, "Conflict")
            source.acknowledge(target)
            return
        except TimeoutError:
            continue
        except StreamingIngestError as error:
            if support.code(error) in support.INVALIDATION or not support.retryable(error):
                raise
            support.backoff(0, deadline)


def run(session, source):
    source.seek(session.open())
    submitted = source.committed
    outstanding = 0
    failures = 0
    deadline = time.monotonic() + support.OUTAGE_SECONDS
    checkpoint_at = time.monotonic() + support.CHECKPOINT_SECONDS
    event = None
    while True:
        try:
            if event is None:
                event = source.read()
            if event is None:
                if outstanding:
                    checkpoint(session, submitted, source, deadline)
                return
            support.remaining(deadline)
            session.channel.append_row(event[1], str(event[0]))
            submitted = event[0]
            event = None
            outstanding += 1
            if outstanding >= support.CHECKPOINT_ROWS or time.monotonic() >= checkpoint_at:
                checkpoint(session, submitted, source, deadline)
                outstanding = 0
                failures = 0
                deadline = time.monotonic() + support.OUTAGE_SECONDS
                checkpoint_at = time.monotonic() + support.CHECKPOINT_SECONDS
        except StreamingIngestError as error:
            failures += 1
            if not support.retryable(error) or failures >= support.MAX_ATTEMPTS:
                raise
            if support.code(error) in support.INVALIDATION:
                source.seek(session.recover(error))
                submitted = source.committed
                outstanding = 0
                event = None
            support.backoff(failures - 1, deadline)


def main():
    source = support.source_from_env()
    session = NamedSession()
    completed = False
    try:
        run(session, source)
        completed = True
        print(f"Committed source checkpoint: {source.committed}")
    finally:
        if not completed:
            print(f"Stopped. Retain source events after checkpoint {source.committed} for replay")
        session.close(completed)


if __name__ == "__main__":
    main()
