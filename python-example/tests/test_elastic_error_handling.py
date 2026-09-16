import os
import sys
from concurrent.futures import Future, TimeoutError as FutureTimeoutError

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_step3_error_handling as elastic
from snowflake.ingest import streaming


def failure(code=streaming.StreamingIngestErrorCode.INVALID_CHANNEL_ERROR, status=409):
    return streaming.StreamingIngestError(code, "synthetic", status, str(status))


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
def no_backoff(monkeypatch):
    monkeypatch.setattr(elastic, "backoff", lambda _, deadline: elastic.remaining(deadline))


def producer_for(*channels):
    clients = [Client(channel) for channel in channels]
    clients_iter = iter(clients)
    producer = elastic.ElasticProducer(lambda: next(clients_iter))
    producer.open()
    return producer, clients


def deadline():
    return elastic.time.monotonic() + 10


def test_append_happens_before_next_source_read():
    channel = Channel()

    class Source(elastic.SampleEventSource):
        def read(self):
            assert len(channel.calls) == self.next_offset - 1
            return super().read()

    producer, _ = producer_for(channel)
    source = Source(4)
    elastic.run(producer, source)
    assert source.committed == 4


def test_pending_limit_pauses_intake(monkeypatch):
    monkeypatch.setattr(elastic, "MAX_PENDING_EVENTS", 4)
    channel = Channel()

    class ReleaseAtLimit(Future):
        def result(self, timeout=None):
            if not self.done():
                assert len(channel.calls) == 4
                self.set_result(None)
            return super().result(timeout)

    channel.outcomes = [ReleaseAtLimit(), completed(), completed(), completed()]
    producer, _ = producer_for(channel)
    source = elastic.SampleEventSource(5)
    elastic.run(producer, source)
    assert source.committed == 5


def test_late_and_out_of_order_success_cannot_commit_a_gap():
    first = Future()
    source = elastic.SampleEventSource(2)
    producer, _ = producer_for(Channel())
    pending = [
        elastic.Pending(source.read(), first, producer.client),
        elastic.Pending(source.read(), completed(), producer.client),
    ]

    elastic.collect_progress(producer, pending, source, deadline())
    assert source.committed == 0

    first.set_result(None)
    elastic.collect_progress(producer, pending, source, deadline())
    assert source.committed == 2


def test_late_errors_from_replaced_client_swap_only_once():
    old_channel, new_channel = Channel(), Channel()
    producer, clients = producer_for(old_channel, new_channel)
    source = elastic.SampleEventSource(3)
    old_client = producer.client
    pending = [
        elastic.Pending(source.read(), completed(), old_client),
        elastic.Pending(source.read(), completed(failure()), old_client),
        elastic.Pending(source.read(), completed(failure()), old_client),
    ]

    while pending:
        elastic.collect_progress(producer, pending, source, deadline())

    assert new_channel.calls == ["2", "3"]
    assert len(clients[0].closes) == 1
    assert source.committed == 3


def test_429_retries_only_rejected_event_without_attempt_limit():
    pressure = failure(streaming.StreamingIngestErrorCode.RECEIVER_SATURATED, 429)
    channel = Channel([completed()] + [pressure] * 10 + [completed()])
    producer, _ = producer_for(channel)
    source = elastic.SampleEventSource(2)

    elastic.run(producer, source)
    assert channel.calls == ["1"] + ["2"] * 11
    assert source.committed == 2


@pytest.mark.parametrize("outcomes", [
    [completed(failure(streaming.StreamingIngestErrorCode.SF_API_USER_ERROR, 400))],
    [
        completed(failure(streaming.StreamingIngestErrorCode.NON_FATAL, 503))
        for _ in range(elastic.MAX_ATTEMPTS)
    ],
])
def test_terminal_errors_preserve_source_checkpoint(outcomes):
    channel = Channel(outcomes)
    producer, _ = producer_for(channel)
    source = elastic.SampleEventSource(1)

    with pytest.raises(streaming.StreamingIngestError):
        elastic.run(producer, source)

    assert source.committed == 0
    assert len(channel.calls) == len(outcomes)


def test_stalled_progress_preserves_original_future(monkeypatch):
    clock = [0.0]

    class StalledFuture(Future):
        def result(self, timeout=None):
            if timeout is not None and not self.done():
                clock[0] += elastic.MAX_NO_PROGRESS_SECONDS + 1
                raise FutureTimeoutError()
            return super().result(timeout)

    future = StalledFuture()
    producer, _ = producer_for(Channel([future]))
    source = elastic.SampleEventSource(1)
    monkeypatch.setattr(elastic.time, "monotonic", lambda: clock[0])

    with pytest.raises(TimeoutError):
        elastic.run(producer, source)

    assert source.committed == 0
    assert not future.cancelled()


def test_checkpoint_failure_keeps_acknowledgement_bookkeeping():
    class Source(elastic.SampleEventSource):
        def acknowledge(self, offset):
            raise OSError("checkpoint unavailable")

    producer, _ = producer_for(Channel())
    source = Source(1)
    pending = [elastic.append_event(producer, source.read(), deadline())]

    with pytest.raises(OSError):
        elastic.collect_progress(producer, pending, source, deadline())

    assert len(pending) == 1
    assert source.committed == 0
