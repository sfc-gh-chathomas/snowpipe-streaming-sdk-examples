"""Exercise the UI control flow without Snowflake or a running web server."""

from contextlib import nullcontext
import os
from pathlib import Path
import runpy
import sys
from unittest.mock import patch
import unittest

import pandas as pd


class Stopped(Exception):
    pass


class Connection:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def query(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        if self.fail:
            raise RuntimeError('PRIVATE_DATABASE_ERROR_MUST_NOT_BE_DISPLAYED')
        if 'AS event_count' in sql:
            return pd.DataFrame([{
                'EVENT_COUNT': 2, 'COMMIT_EVENTS': 1, 'LATENCY_EVENTS': 1,
                'ROWS_INGESTED': 0, 'ROWS_PARSED': 100, 'ERRORS': 100,
                'MEASURED_SAMPLES': 0, 'AVG_LATENCY_MS': None,
                'P95_LATENCY_MS': None, 'LATEST_EVENT': None,
            }])
        return pd.DataFrame()


class StreamlitMock:
    def __init__(self, submitted=True, fail_connection=False, fail_query=False):
        self.submitted = submitted
        self.fail_connection = fail_connection
        self.conn = Connection(fail_query)
        self.connection_names = []
        self.messages = []
        self.metrics = {}
        self.sidebar = self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def connection(self, name, **kwargs):
        self.connection_names.append(name)
        if self.fail_connection:
            raise RuntimeError('PRIVATE_CONNECTION_ERROR')
        return self.conn

    def text_input(self, label, value=''):
        return {'Database': 'DB', 'Schema': 'SCHEMA', 'Table': 'TABLE'}.get(label, value)

    def selectbox(self, label, options):
        return options[0]

    def checkbox(self, *args, **kwargs):
        return False

    def form_submit_button(self, *args, **kwargs):
        return self.submitted

    def columns(self, count):
        return [self] * count

    def metric(self, label, value):
        self.metrics[label] = value

    def stop(self):
        raise Stopped()

    def __getattr__(self, name):
        if name in {'form', 'spinner', 'expander'}:
            return lambda *args, **kwargs: nullcontext()
        return lambda *args, **kwargs: self.messages.append((name, args))


class AppTests(unittest.TestCase):
    def run_app(self, mock):
        with patch.dict(sys.modules, {'streamlit': mock}), patch.dict(os.environ, {}, clear=True):
            try:
                runpy.run_path(str(Path(__file__).resolve().parents[1] / 'streamlit_app.py'))
            except Stopped:
                pass

    def test_no_query_before_submit(self):
        mock = StreamlitMock(submitted=False)
        self.run_app(mock)
        self.assertEqual(mock.connection_names, ['snowflake-callers-rights'])
        self.assertEqual(mock.conn.calls, [])

    def test_connection_failure_has_no_fallback(self):
        mock = StreamlitMock(fail_connection=True)
        self.run_app(mock)
        self.assertEqual(mock.connection_names, ['snowflake-callers-rights'])
        self.assertNotIn('PRIVATE_CONNECTION_ERROR', str(mock.messages))

    def test_all_rejected_and_missing_latency(self):
        mock = StreamlitMock()
        self.run_app(mock)
        self.assertEqual(mock.metrics['Row error rate'], '100.00%')
        self.assertEqual(mock.metrics['Average processing time'], 'Unavailable')
        self.assertEqual(len(mock.conn.calls), 5)
        for sql, kwargs in mock.conn.calls:
            self.assertEqual(kwargs['ttl'], 0)
            self.assertEqual(kwargs['timeout'], 30)
            self.assertNotIn('error_message', sql)

    def test_query_failure_hides_raw_details_and_partial_totals(self):
        mock = StreamlitMock(fail_query=True)
        self.run_app(mock)
        self.assertEqual(mock.metrics, {})
        self.assertNotIn('PRIVATE_DATABASE_ERROR', str(mock.messages))
        self.assertEqual(len(mock.conn.calls), 1)


if __name__ == '__main__':
    unittest.main()
