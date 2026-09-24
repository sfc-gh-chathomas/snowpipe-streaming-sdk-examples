from datetime import datetime, timedelta, timezone
import math
import unittest

from monitoring import EVENT_VIEW, Scope, collection_state, display, error_rate, make_scope, queries


class MonitoringTests(unittest.TestCase):
    def setUp(self):
        self.scope = make_scope(EVENT_VIEW, 'DB', 'SCHEMA', 'TABLE', 'Last hour', now=datetime(2026, 1, 1, tzinfo=timezone.utc))

    def test_error_rate(self):
        self.assertEqual(error_rate(20, 100), 20)
        self.assertEqual(error_rate(100, 100), 100)
        self.assertEqual(error_rate(0, 100), 0)
        for errors, parsed in [(None, 100), (10, 0), (101, 100), (-1, 100), (math.nan, 100)]:
            self.assertIsNone(error_rate(errors, parsed))

    def test_missing_measurements(self):
        for value in [None, math.nan, math.inf]:
            self.assertEqual(display(value), 'Unavailable')
        self.assertEqual(display(0), '0')

    def test_no_data_is_not_healthy(self):
        self.assertIn('not evidence', collection_state({'event_count': 0}))
        self.assertIn('no commit', collection_state({'event_count': 10, 'commit_events': 0}))
        self.assertIsNone(collection_state({'event_count': 10, 'commit_events': 2}))

    def test_all_queries_bounded_and_bound(self):
        for sql, params in queries(self.scope).values():
            self.assertIn('FROM IDENTIFIER(%s)', sql)
            self.assertIn("RECORD_TYPE = 'EVENT'", sql)
            self.assertIn('TIMESTAMP >= %s AND TIMESTAMP < %s', sql)
            self.assertEqual(sql.count('%s'), len(params))
            self.assertIn(self.scope.start, params)
            self.assertIn(self.scope.end, params)
            self.assertNotIn('uncompressed_bytes', sql)

    def test_filter_values_are_not_sql(self):
        scoped = make_scope(EVENT_VIEW, "NAME'WITH_QUOTE", 'SCHEMA', 'TABLE', 'Last hour', channel='CHANNEL')
        for sql, params in queries(scoped).values():
            self.assertNotIn(scoped.database, sql)
            self.assertIn(scoped.database, params)
            self.assertIn(scoped.channel, params)
            self.assertEqual(sql.count('%s'), len(params))

    def test_sensitive_messages_opt_in(self):
        self.assertNotIn('error_message', queries(self.scope)['errors'][0])
        self.assertIn("LEFT(VALUE['error_message']::STRING, 1000)", queries(self.scope, True)['errors'][0])

    def test_source_and_scope_validation(self):
        for source in ['EVENTS', 'DB.SCHEMA.VIEW;SELECT', 'DB.SCHEMA.*', '"DB".SCHEMA.VIEW']:
            with self.assertRaises(ValueError):
                make_scope(source, 'DB', 'SCHEMA', 'TABLE', 'Last hour')
        for period in ['All', 'Last 7D']:
            with self.assertRaises(ValueError):
                make_scope(EVENT_VIEW, 'DB', 'SCHEMA', 'TABLE', period)
        with self.assertRaises(ValueError):
            make_scope(EVENT_VIEW, '', 'SCHEMA', 'TABLE', 'Last hour')
        with self.assertRaises(ValueError):
            Scope(EVENT_VIEW, 'DB', 'SCHEMA', 'TABLE', self.scope.end - timedelta(days=2), self.scope.end, 60)


if __name__ == '__main__':
    unittest.main()
