import os
import sys
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_production_example as elastic
support = elastic
from snowflake.ingest.streaming import StreamingIngestError, StreamingIngestErrorCode


def failure(code=StreamingIngestErrorCode.INVALID_CHANNEL_ERROR, status=409):
    return StreamingIngestError(code, "synthetic", status, str(status))


def completed(error=None):
    future = Future()
    if error:
        future.set_exception(error)
    else:
        future.set_result(None)
    return future


class Channel:
    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.calls = []

    def append_row_with_wait(self, row, token):
        self.calls.append(token)
        outcome = self.outcomes.pop(0) if self.outcomes else completed()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.closes = []

    def get_elastic_channel(self):
        return self.channel

    def close(self, **options):
        self.closes.append(options)


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch):
    monkeypatch.setattr(support, "backoff", lambda attempt, deadline: support.remaining(deadline))


def session_for(*channels):
    clients = [Client(channel) for channel in channels]
    iterator = iter(clients)
    session = elastic.ElasticProducer(lambda: next(iterator))
    session.open()
    return session, clients


def deadline():
    return support.time.monotonic() + 10


def test_append_happens_before_next_source_read():
    channel = Channel()

    class Source(support.SampleEventSource):
        def read(self):
            assert len(channel.calls) == self.next_offset - 1
            return super().read()

    session, _ = session_for(channel)
    source = Source(4)
    elastic.run(session, source)
    assert source.committed == 4
    assert channel.calls == ["1", "2", "3", "4"]


def test_checkpoint_pauses_source_reads_until_every_ack(monkeypatch):
    monkeypatch.setattr(support, "CHECKPOINT_ROWS", 2)
    source = support.SampleEventSource(3)

    class Delayed(Future):
        def result(self, timeout=None):
            assert source.next_offset == 3
            assert source.committed == 0
            self.set_result(None)
            return super().result(timeout)

    channel = Channel([Delayed(), completed()])
    session, _ = session_for(channel)
    elastic.run(session, source)
    assert source.committed == 3


def test_late_ack_keeps_original_future_and_client():
    class Late(Future):
        polls = 0

        def result(self, timeout=None):
            self.polls += 1
            if self.polls == 1:
                raise TimeoutError("caller wait only")
            self.set_result(None)
            return super().result(timeout)

    future = Late()
    channel = Channel([future])
    session, clients = session_for(channel)
    source = support.SampleEventSource(1)
    pending = [elastic.append_event(session, source.read(), deadline())]
    elastic.confirm_checkpoint(session, pending, source, deadline())
    assert future.polls == 2
    assert channel.calls == ["1"]
    assert session.generation == 1
    assert clients[0].closes == []
    assert source.committed == 1


def test_outage_deadline_preserves_source_checkpoint():
    session, _ = session_for(Channel())
    source = support.SampleEventSource(1)
    pending = [elastic.Pending(source.read(), Future(), session.generation)]
    with pytest.raises(TimeoutError, match="retain events"):
        elastic.confirm_checkpoint(session, pending, source, support.time.monotonic() - 1)
    assert source.committed == 0
    assert len(pending) == 1
    assert not pending[0].future.cancelled()


def test_429_midstream_retries_only_rejected_event():
    pressure = failure(StreamingIngestErrorCode.RECEIVER_SATURATED, 429)
    first = completed()
    channel = Channel([first, pressure, completed()])
    session, clients = session_for(channel)
    source = support.SampleEventSource(2)
    elastic.run(session, source)
    assert channel.calls == ["1", "2", "2"]
    assert source.committed == 2
    assert clients[0].closes == []


def test_stale_invalidations_rebuild_only_once_and_skip_acked_rows():
    old = Channel([completed(), completed(failure()), completed(failure())])
    fresh = Channel()
    session, clients = session_for(old, fresh)
    source = support.SampleEventSource(3)
    pending = [elastic.append_event(session, source.read(), deadline()) for _ in range(3)]
    elastic.confirm_checkpoint(session, pending, source, deadline())
    assert fresh.calls == ["2", "3"]
    assert session.generation == 2
    assert len(clients[0].closes) == 1
    assert source.committed == 3


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_permanent_failure_never_advances_source(status):
    error = failure(StreamingIngestErrorCode.SF_API_USER_ERROR, status)
    channel = Channel([completed(error)])
    session, _ = session_for(channel)
    source = support.SampleEventSource(3)
    with pytest.raises(StreamingIngestError):
        elastic.run(session, source)
    assert source.committed == 0
    assert channel.calls == ["1"]


def test_retry_exhaustion_does_not_ack_source():
    error = failure(StreamingIngestErrorCode.NON_FATAL, 503)
    channel = Channel([completed(error) for _ in range(support.MAX_ATTEMPTS)])
    session, _ = session_for(channel)
    source = support.SampleEventSource(1)
    with pytest.raises(StreamingIngestError):
        elastic.run(session, source)
    assert source.committed == 0
    assert len(channel.calls) == support.MAX_ATTEMPTS


def test_out_of_order_completion_does_not_commit_a_gap():
    source = support.SampleEventSource(2)
    future = Future()
    later = completed()
    session, _ = session_for(Channel())
    pending = [elastic.Pending(source.read(), future, 1), elastic.Pending(source.read(), later, 1)]
    with pytest.raises(TimeoutError):
        elastic.confirm_checkpoint(session, pending, source, support.time.monotonic() - 1)
    assert source.committed == 0
    future.set_result(None)
    elastic.confirm_checkpoint(session, pending, source, deadline())
    assert source.committed == 2


def test_source_replay_is_deterministic_and_checkpoint_is_explicit():
    first = support.SampleEventSource(3)
    events = [first.read() for _ in range(3)]
    restarted = support.SampleEventSource(3, checkpoint=1)
    assert restarted.read() == events[1]
    assert restarted.committed == 1


def test_production_examples_have_no_local_support_import():
    import inspect
    import named_channel_checkpoint_example as named
    for example in (elastic, named):
        assert "production_support" not in inspect.getsource(example)
