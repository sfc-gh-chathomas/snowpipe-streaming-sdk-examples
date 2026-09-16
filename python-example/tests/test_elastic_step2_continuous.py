import os
import sys
from concurrent.futures import Future

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_step2_continuous as continuous


def completed():
    future = Future()
    future.set_result(None)
    return future


class Channel:
    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.calls = []

    def append_row_with_wait(self, row, token):
        self.calls.append(token)
        return self.outcomes.pop(0) if self.outcomes else completed()


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.closes = []

    def get_elastic_channel(self):
        return self.channel

    def close(self, **options):
        self.closes.append(options)


def test_wait_and_remove_stops_at_first_unfinished_append():
    second = Future()
    pending = continuous.deque([completed(), second, completed()])

    assert continuous.wait_and_remove_confirmed_prefix(pending) == 1

    second.set_result(None)
    assert continuous.wait_and_remove_confirmed_prefix(pending) == 2
    assert not pending


def test_main_bounds_pending_work(monkeypatch):
    monkeypatch.setattr(continuous, "MAX_PENDING_EVENTS", 3)

    class ReleaseAtLimit(Future):
        def result(self, timeout=None):
            if not self.done():
                assert len(channel.calls) == 3
                self.set_result(None)
            return super().result(timeout)

    outcomes = [ReleaseAtLimit(), completed(), completed(), completed()]

    channel = Channel(outcomes)
    client = Client(channel)
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", "4")
    monkeypatch.setattr(continuous, "create_client", lambda: client)

    continuous.main()

    assert channel.calls == [None, None, None, None]
    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]


def test_sample_rows_include_stable_event_ids():
    rows = list(continuous.sample_rows(2))

    assert rows[0]["EVENT_ID"] == 0
    assert rows[1]["EVENT_ID"] == 1
