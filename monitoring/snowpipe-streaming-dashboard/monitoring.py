"""Bounded, read-only queries and telemetry metric interpretation."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re


EVENT_VIEW = 'SNOWFLAKE.TELEMETRY.EVENTS_VIEW'
RANGES = {'Last hour': (1, 60), 'Last 6 hours': (6, 300), 'Last 24 hours': (24, 900)}
SOURCE_PATTERN = re.compile(r'[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){2}\Z')


@dataclass(frozen=True)
class Scope:
    source: str
    database: str
    schema: str
    table: str
    start: datetime
    end: datetime
    bucket_seconds: int
    channel: str = ''

    def __post_init__(self):
        if not SOURCE_PATTERN.fullmatch(self.source):
            raise ValueError('Event source must be three unquoted identifiers: DATABASE.SCHEMA.VIEW.')
        for name in ('database', 'schema', 'table'):
            value = getattr(self, name)
            if not value or value != value.strip() or len(value) > 255:
                raise ValueError(f'Enter a nonempty {name} name, exactly as stored, without surrounding spaces.')
        if len(self.channel) > 1024:
            raise ValueError('Channel name is too long.')
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError('Time bounds must include a timezone.')
        if not timedelta(0) < self.end - self.start <= timedelta(hours=24):
            raise ValueError('Select a time window of at most 24 hours.')
        if self.bucket_seconds not in {60, 300, 900}:
            raise ValueError('Unsupported time bucket.')


def make_scope(source, database, schema, table, range_name, channel='', now=None):
    if range_name not in RANGES:
        raise ValueError('Select a supported time range.')
    hours, bucket = RANGES[range_name]
    end = now or datetime.now(timezone.utc)
    return Scope(source, database, schema, table, end - timedelta(hours=hours), end, bucket, channel)


def _where(scope):
    clause = """FROM IDENTIFIER(%s)
WHERE RECORD_TYPE = 'EVENT'
  AND SCOPE['name']::STRING = 'snow.snowpipe.streaming'
  AND RESOURCE_ATTRIBUTES['snow.database.name']::STRING = %s
  AND RESOURCE_ATTRIBUTES['snow.schema.name']::STRING = %s
  AND RESOURCE_ATTRIBUTES['snow.table.name']::STRING = %s
  AND TIMESTAMP >= %s AND TIMESTAMP < %s"""
    params = [scope.source, scope.database, scope.schema, scope.table, scope.start, scope.end]
    if scope.channel:
        clause += "\n  AND VALUE['channel_name']::STRING = %s"
        params.append(scope.channel)
    return clause, params


def queries(scope, include_messages=False):
    where, params = _where(scope)
    summary = """SELECT
  COUNT(*) AS event_count,
  COUNT_IF(RECORD['name']::STRING = 'commit') AS commit_events,
  COUNT_IF(RECORD['name']::STRING = 'latency') AS latency_events,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['row_count']::NUMBER, NULL)) AS rows_ingested,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['rows_parsed']::NUMBER, NULL)) AS rows_parsed,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['error_count']::NUMBER, NULL)) AS errors,
  COUNT(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::NUMBER, NULL)) AS measured_samples,
  AVG(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::NUMBER, NULL)) AS avg_latency_ms,
  APPROX_PERCENTILE(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::NUMBER, NULL), 0.95) AS p95_latency_ms,
  MAX(TIMESTAMP) AS latest_event
""" + where
    volume = """SELECT TIME_SLICE(TIMESTAMP::TIMESTAMP_NTZ, %s, 'SECOND') AS bucket,
  SUM(VALUE['row_count']::NUMBER) AS rows_ingested,
  SUM(VALUE['rows_parsed']::NUMBER) AS rows_parsed,
  SUM(VALUE['error_count']::NUMBER) AS errors
""" + where + "\n  AND RECORD['name']::STRING = 'commit'\nGROUP BY 1 ORDER BY 1"
    channels = """SELECT VALUE['channel_name']::STRING AS channel_name,
  SUM(VALUE['row_count']::NUMBER) AS rows_ingested,
  SUM(VALUE['error_count']::NUMBER) AS errors
""" + where + "\n  AND RECORD['name']::STRING = 'commit'\nGROUP BY 1 ORDER BY rows_ingested DESC LIMIT 20"
    lifecycle = """SELECT TIME_SLICE(TIMESTAMP::TIMESTAMP_NTZ, %s, 'SECOND') AS bucket,
  COUNT_IF(VALUE['event_type']::STRING = 'OPEN') AS opens,
  COUNT_IF(VALUE['event_type']::STRING = 'DROP') AS drops
""" + where + "\n  AND RECORD['name']::STRING = 'channel_lifecycle'\nGROUP BY 1 ORDER BY 1"
    message_column = ", LEFT(VALUE['error_message']::STRING, 1000) AS error_message" if include_messages else ''
    errors = """SELECT TIMESTAMP AS event_time, RECORD['name']::STRING AS event_name,
  VALUE['channel_name']::STRING AS channel_name,
  VALUE['error_type']::STRING AS error_type,
  VALUE['error_code']::STRING AS error_code""" + message_column + '\n' + where + "\n  AND RECORD['name']::STRING IN ('row_error', 'channel_error')\nORDER BY TIMESTAMP DESC LIMIT 50"
    return {
        'summary': (summary, list(params)),
        'volume': (volume, [scope.bucket_seconds, *params]),
        'channels': (channels, list(params)),
        'lifecycle': (lifecycle, [scope.bucket_seconds, *params]),
        'errors': (errors, list(params)),
    }


def number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return float(result) if result.is_finite() else None


def error_rate(errors, parsed):
    numerator, denominator = number(errors), number(parsed)
    if numerator is None or denominator is None or denominator <= 0 or not 0 <= numerator <= denominator:
        return None
    return 100 * numerator / denominator


def display(value, suffix='', decimals=0):
    numeric = number(value)
    return 'Unavailable' if numeric is None else f'{numeric:,.{decimals}f}{suffix}'


def collection_state(summary):
    if not number(summary.get('event_count')):
        return 'No matching events. Check the source view, target names, time range, permissions, and LOG_EVENT_LEVEL. This is not evidence of healthy ingestion.'
    if not number(summary.get('commit_events')):
        return 'Events exist, but no commit events were returned. Ingestion totals and error rate are unavailable. Check INFO-level collection and pipeline activity.'
    return None
