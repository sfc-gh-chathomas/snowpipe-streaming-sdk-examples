import os
import sys
from concurrent.futures import Future

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_step2_continuous as continuous
from snowflake.ingest import streaming


def completed(error=None):
    future = Future()
    if error:
        future.set_exception(error)
    else:
        future.set_result(None)
    return future


def backpressure():
    return streaming.StreamingIngestError(
        streaming.StreamingIngestErrorCode.RECEIVER_SATURATED,
        "synthetic",
        429,
        "Too Many Requests",
    )


def test_collect_completed_removes_only_finished_futures():
    unfinished = Future()
    pending = [unfinished, completed()]

    assert continuous.collect_completed(pending) == 1
    assert pending == [unfinished]


def test_run_pauses_and_retries_the_rejected_event(monkeypatch):
    first = Future()
    outcomes = [first, backpressure(), completed()]
    calls = []

    class Channel:
        def append_row_with_wait(self, row, token):
            calls.append(token)
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(continuous.time, "sleep", lambda _: first.set_result(None))

    assert continuous.run(Channel(), continuous.sample_rows(2)) == 2
    assert calls == ["1", "2", "2"]


def test_429_backpressure_does_not_have_an_attempt_limit(monkeypatch):
    outcomes = [backpressure()] * 10 + [completed()]

    class Channel:
        def append_row_with_wait(self, row, token):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(continuous.time, "sleep", lambda _: None)

    assert continuous.run(Channel(), continuous.sample_rows(1)) == 1


def test_run_reports_confirmed_rows_before_late_failure(capsys):
    outcomes = [completed(), completed(RuntimeError("late failure"))]

    class Channel:
        def append_row_with_wait(self, row, token):
            return outcomes.pop(0)

    with pytest.raises(RuntimeError, match="late failure"):
        continuous.run(Channel(), continuous.sample_rows(2))

    assert "at least 1" in capsys.readouterr().out


def test_sample_rows_include_stable_event_ids():
    rows = list(continuous.sample_rows(2))

    assert rows[0][1]["EVENT_ID"] == 1
    assert rows[1][1]["EVENT_ID"] == 2
