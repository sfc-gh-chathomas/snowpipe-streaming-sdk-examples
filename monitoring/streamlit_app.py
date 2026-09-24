"""Snowpipe Streaming monitoring for Streamlit in Snowflake."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
import streamlit as st


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
    clause = """FROM IDENTIFIER(?)
WHERE RECORD_TYPE = 'EVENT'
  AND SCOPE['name']::STRING = 'snow.snowpipe.streaming'
  AND RESOURCE_ATTRIBUTES['snow.database.name']::STRING = ?
  AND RESOURCE_ATTRIBUTES['snow.schema.name']::STRING = ?
  AND RESOURCE_ATTRIBUTES['snow.table.name']::STRING = ?
  AND TIMESTAMP >= ? AND TIMESTAMP < ?"""
    params = [scope.source, scope.database, scope.schema, scope.table, scope.start, scope.end]
    if scope.channel:
        clause += "\n  AND VALUE['channel_name']::STRING = ?"
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
    volume = """SELECT TIME_SLICE(TIMESTAMP::TIMESTAMP_NTZ, ?, 'SECOND') AS bucket,
  SUM(VALUE['row_count']::NUMBER) AS rows_ingested,
  SUM(VALUE['rows_parsed']::NUMBER) AS rows_parsed,
  SUM(VALUE['error_count']::NUMBER) AS errors
""" + where + "\n  AND RECORD['name']::STRING = 'commit'\nGROUP BY 1 ORDER BY 1"
    channels = """SELECT VALUE['channel_name']::STRING AS channel_name,
  SUM(VALUE['row_count']::NUMBER) AS rows_ingested,
  SUM(VALUE['error_count']::NUMBER) AS errors
""" + where + "\n  AND RECORD['name']::STRING = 'commit'\nGROUP BY 1 ORDER BY rows_ingested DESC LIMIT 20"
    lifecycle = """SELECT TIME_SLICE(TIMESTAMP::TIMESTAMP_NTZ, ?, 'SECOND') AS bucket,
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


st.set_page_config(page_title='Snowpipe Streaming monitoring', layout='wide')
st.title('Snowpipe Streaming monitoring')

try:
    connection = st.connection('snowflake-callers-rights')
except Exception:
    st.error('Connection unavailable. Use a container runtime with restricted caller rights and administrator-configured caller grants. See README. No owner-rights fallback is used.')
    st.stop()

with st.sidebar.form('scope'):
    st.subheader('Event source')
    source = st.text_input('Event table or view', value=EVENT_VIEW)
    st.subheader('Ingestion target')
    database = st.text_input('Database')
    schema = st.text_input('Schema')
    table = st.text_input('Table')
    channel = st.text_input('Channel (optional)')
    time_range = st.selectbox('Time range', list(RANGES))
    include_messages = st.checkbox('Show error messages', value=False, help='Messages can contain customer data. Leave hidden when presenting or sharing screenshots.')
    submitted = st.form_submit_button('Load / refresh', icon=':material/refresh:')

if not submitted:
    st.info('Select an ingestion target to load telemetry. No event data is queried until you submit.')
    st.stop()

try:
    scope = make_scope(source, database, schema, table, time_range, channel)
except ValueError as error:
    st.error(str(error))
    st.stop()

results = {}
try:
    with st.spinner('Loading telemetry'):
        for name, (sql, params) in queries(scope, include_messages).items():
            frame = connection.query(sql, params=params, ttl=0, timeout=30)
            frame.columns = [column.lower() for column in frame.columns]
            results[name] = frame
except Exception:
    results.clear()
    st.error('Telemetry could not be loaded. Check the source view, viewer permissions and caller grants, warehouse availability, and time range. No partial totals are displayed. Ask an administrator to inspect query history; raw database errors are not shown here.')
    st.stop()

summary = results['summary'].iloc[0].to_dict() if not results['summary'].empty else {}
st.caption(f'{database}.{schema}.{table} | {scope.start:%Y-%m-%d %H:%M:%S} to {scope.end:%Y-%m-%d %H:%M:%S} UTC')
state = collection_state(summary)
if state:
    st.warning(state)

metrics = st.columns(4)
metrics[0].metric('Rows ingested', display(summary.get('rows_ingested')))
metrics[1].metric('Rejected rows', display(summary.get('errors')))
metrics[2].metric('Row error rate', display(error_rate(summary.get('errors'), summary.get('rows_parsed')), '%', 2))
metrics[3].metric('Average processing time', display(summary.get('avg_latency_ms'), ' ms'))

latency_events = number(summary.get('latency_events')) or 0
measured = number(summary.get('measured_samples')) or 0
st.caption(f'Latency measurements: {measured:,.0f} of {latency_events:,.0f} latency events | p95: {display(summary.get("p95_latency_ms"), " ms")}')
if summary.get('latest_event') is not None:
    st.caption(f'Latest returned event: {summary["latest_event"]}')

st.subheader('Rows over time')
volume = results['volume']
if volume.empty:
    st.info('No commit events returned for this selection.')
else:
    st.line_chart(volume.set_index('bucket')[['rows_ingested', 'errors']], height=300)
    st.caption(f'Rows per {scope.bucket_seconds // 60}-minute interval. Gaps mean no returned events, not confirmed zero ingestion.')

left, right = st.columns(2)
with left:
    st.subheader('Top channels by rows')
    if results['channels'].empty:
        st.info('No channel commit totals available.')
    else:
        st.dataframe(results['channels'], hide_index=True)
with right:
    st.subheader('Channel operations')
    if results['lifecycle'].empty:
        st.info('No lifecycle events returned. This does not establish how many channels are active.')
    else:
        st.bar_chart(results['lifecycle'].set_index('bucket')[['opens', 'drops']], height=300)

st.subheader('Recent error events')
if include_messages:
    st.warning('Error messages can contain sensitive data. Do not publish these results.')
if results['errors'].empty:
    st.info('No matching error events returned. Collection may be incomplete; this is not a health check.')
else:
    st.dataframe(results['errors'], hide_index=True)

with st.expander('Metric definitions and limitations'):
    st.markdown('''
- Error rate is rejected rows divided by parsed rows. Zero parsed rows or inconsistent counts produce **Unavailable**.
- Processing time measures server-side ingestion, not source-to-query latency. Missing measurements are excluded.
- Bytes are omitted because request-level bytes can repeat across channel events.
- Row-error events may be representative, not a complete rejected-row list. Use commit counts for totals.
- Object filters exclude events without matching object attributes. This dashboard is not a comprehensive authentication monitor.
- Channel operations count OPEN/DROP events, not distinct active channels. No offset-based recovery decisions are made here.
- Each refresh runs five read-only queries. Event delivery can lag; the final time bucket can be partial.
''')
