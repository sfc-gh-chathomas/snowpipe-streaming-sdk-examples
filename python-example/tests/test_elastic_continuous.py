import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_step2_continuous as continuous
from snowflake.ingest import streaming


def sdk_error(code, status=429):
    return streaming.StreamingIngestError(
        code,
        "synthetic",
        status,
        "synthetic",
    )


class Channel:
    def __init__(self, outcomes=(), async_errors=0):
        self.outcomes = list(outcomes)
        self.calls = []
        self.accepted = []
        self.async_errors = async_errors
        self.success_handler = None
        self.error_handler = None

    def append_row(self, row, token):
        self.calls.append(token)
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if outcome is not None:
                raise outcome
        self.accepted.append(token)

    def set_success_handler(self, handler):
        self.success_handler = handler

    def set_error_handler(self, handler):
        self.error_handler = handler

    def complete(self):
        split = len(self.accepted) - self.async_errors
        if split and self.success_handler:
            self.success_handler(SimpleNamespace(append_tokens=tuple(self.accepted[:split])))
        if self.async_errors and self.error_handler:
            self.error_handler(SimpleNamespace(append_tokens=tuple(self.accepted[split:])))


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.closes = []

    def get_elastic_channel(self):
        return self.channel

    def close(self, **options):
        self.channel.complete()
        self.closes.append(options)


def run_main(monkeypatch, channel, total=3):
    client = Client(channel)
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", str(total))
    monkeypatch.setattr(continuous, "create_client", lambda: client)
    continuous.main()
    return client


def event_ids(tokens):
    return [token.event_id for token in tokens]


def test_main_appends_without_waiting_and_logs_completion(monkeypatch, caplog):
    channel = Channel()
    caplog.set_level("INFO", logger=continuous.logger.name)

    client = run_main(monkeypatch, channel)

    assert event_ids(channel.calls) == [1, 2, 3]
    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]
    assert "accepted=3 acknowledged=3 errors=0 unreported=0" in caplog.text


@pytest.mark.parametrize("code", continuous.SDK_BACKPRESSURE_ERRORS)
def test_main_pauses_and_retries_the_rejected_event(monkeypatch, code):
    channel = Channel([None, sdk_error(code), None])
    sleeps = []
    monkeypatch.setattr(continuous.time, "sleep", sleeps.append)

    run_main(monkeypatch, channel, total=2)

    assert event_ids(channel.calls) == [1, 2, 2]
    assert sleeps == [continuous.BACKPRESSURE_RETRY_SECONDS]


def test_backpressure_does_not_have_an_attempt_limit(monkeypatch):
    errors = [sdk_error(streaming.StreamingIngestErrorCode.RECEIVER_SATURATED)] * 10
    channel = Channel(errors + [None])
    monkeypatch.setattr(continuous.time, "sleep", lambda _: None)

    run_main(monkeypatch, channel, total=1)

    assert event_ids(channel.calls) == [1] * 11


def test_non_backpressure_error_propagates_after_flush(monkeypatch):
    error = sdk_error(streaming.StreamingIngestErrorCode.SF_API_USER_ERROR, 400)
    channel = Channel([error])
    client = Client(channel)
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", "1")
    monkeypatch.setattr(continuous, "create_client", lambda: client)

    with pytest.raises(streaming.StreamingIngestError):
        continuous.main()

    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]


def test_main_logs_asynchronous_error_count(monkeypatch, caplog):
    channel = Channel(async_errors=1)
    caplog.set_level("INFO", logger=continuous.logger.name)

    run_main(monkeypatch, channel, total=2)

    assert "accepted=2 acknowledged=1 errors=1 unreported=0" in caplog.text


def test_sample_rows_include_stable_event_ids():
    rows = list(continuous.sample_rows(2))

    assert rows[0][1]["EVENT_ID"] == 1
    assert rows[1][1]["EVENT_ID"] == 2
