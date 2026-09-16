import os
import sys
from concurrent.futures import Future

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import step2_elastic_continuous as continuous


def completed():
    future = Future()
    future.set_result(None)
    return future


def test_collect_ready_stops_at_first_unfinished_append():
    first = Future()
    pending = continuous.deque([first, completed()])

    assert continuous.collect_ready(pending) == 0

    first.set_result(None)
    assert continuous.collect_ready(pending) == 2
    assert not pending


def test_run_bounds_pending_work(monkeypatch):
    monkeypatch.setattr(continuous, "MAX_PENDING_EVENTS", 3)
    calls = []

    class ReleaseAtLimit(Future):
        def result(self, timeout=None):
            if not self.done():
                assert len(calls) == 3
                self.set_result(None)
            return super().result(timeout)

    outcomes = [ReleaseAtLimit(), completed(), completed(), completed()]

    class Channel:
        def append_row_with_wait(self, row, token):
            calls.append(token)
            return outcomes.pop(0)

    assert continuous.run(Channel(), continuous.sample_rows(4)) == 4
    assert calls == ["1", "2", "3", "4"]


def test_sample_rows_include_stable_event_ids():
    rows = list(continuous.sample_rows(2))

    assert rows[0][1]["EVENT_ID"] == 1
    assert rows[1][1]["EVENT_ID"] == 2
