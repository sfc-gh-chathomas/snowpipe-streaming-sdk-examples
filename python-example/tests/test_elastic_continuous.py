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


def test_run_appends_without_waiting():
    channel = Channel()

    assert continuous.run(channel, continuous.sample_rows(3)) == 3
    assert channel.calls == ["1", "2", "3"]


@pytest.mark.parametrize("code", continuous.SDK_BACKPRESSURE_ERRORS)
def test_run_pauses_and_retries_the_rejected_event(monkeypatch, code):
    channel = Channel([None, sdk_error(code), None])
    sleeps = []
    monkeypatch.setattr(continuous.time, "sleep", sleeps.append)

    assert continuous.run(channel, continuous.sample_rows(2)) == 2
    assert channel.calls == ["1", "2", "2"]
    assert sleeps == [continuous.BACKPRESSURE_RETRY_SECONDS]


def test_backpressure_does_not_have_an_attempt_limit(monkeypatch):
    errors = [sdk_error(streaming.StreamingIngestErrorCode.RECEIVER_SATURATED)] * 10
    channel = Channel(errors + [None])
    monkeypatch.setattr(continuous.time, "sleep", lambda _: None)

    assert continuous.run(channel, continuous.sample_rows(1)) == 1
    assert channel.calls == ["1"] * 11


def test_non_backpressure_error_propagates():
    error = sdk_error(streaming.StreamingIngestErrorCode.SF_API_USER_ERROR, 400)
    channel = Channel([error])

    with pytest.raises(streaming.StreamingIngestError):
        continuous.run(channel, continuous.sample_rows(1))


def test_sample_rows_include_stable_event_ids():
    rows = list(continuous.sample_rows(2))

    assert rows[0][1]["EVENT_ID"] == 1
    assert rows[1][1]["EVENT_ID"] == 2
