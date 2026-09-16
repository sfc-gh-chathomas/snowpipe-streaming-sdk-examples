import os
import sys

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
    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.calls = []

    def append_row(self, row, token):
        self.calls.append(token)
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if outcome is not None:
                raise outcome


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.closes = []

    def get_elastic_channel(self):
        return self.channel

    def close(self, **options):
        self.closes.append(options)


def run_main(monkeypatch, channel, total=3):
    client = Client(channel)
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", str(total))
    monkeypatch.setattr(continuous, "create_client", lambda: client)
    continuous.main()
    return client


def test_main_appends_without_waiting_and_flushes(monkeypatch):
    channel = Channel()

    client = run_main(monkeypatch, channel)

    assert channel.calls == ["1", "2", "3"]
    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]


@pytest.mark.parametrize("code", continuous.SDK_BACKPRESSURE_ERRORS)
def test_main_pauses_and_retries_the_rejected_event(monkeypatch, code):
    channel = Channel([None, sdk_error(code), None])
    sleeps = []
    monkeypatch.setattr(continuous.time, "sleep", sleeps.append)

    run_main(monkeypatch, channel, total=2)

    assert channel.calls == ["1", "2", "2"]
    assert sleeps == [continuous.BACKPRESSURE_RETRY_SECONDS]


def test_backpressure_does_not_have_an_attempt_limit(monkeypatch):
    errors = [sdk_error(streaming.StreamingIngestErrorCode.RECEIVER_SATURATED)] * 10
    channel = Channel(errors + [None])
    monkeypatch.setattr(continuous.time, "sleep", lambda _: None)

    run_main(monkeypatch, channel, total=1)

    assert channel.calls == ["1"] * 11


def test_non_backpressure_error_propagates_after_flush(monkeypatch):
    error = sdk_error(streaming.StreamingIngestErrorCode.SF_API_USER_ERROR, 400)
    channel = Channel([error])
    client = Client(channel)
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", "1")
    monkeypatch.setattr(continuous, "create_client", lambda: client)

    with pytest.raises(streaming.StreamingIngestError):
        continuous.main()

    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]


def test_sample_rows_include_stable_event_ids():
    rows = list(continuous.sample_rows(2))

    assert rows[0][1]["EVENT_ID"] == 1
    assert rows[1][1]["EVENT_ID"] == 2
