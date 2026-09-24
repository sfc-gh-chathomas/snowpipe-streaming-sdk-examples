"""Snowpipe Streaming monitoring example. Read-only, table-scoped telemetry."""

import os
import streamlit as st

from monitoring import EVENT_VIEW, RANGES, collection_state, display, error_rate, make_scope, number, queries


st.set_page_config(page_title='Snowpipe Streaming monitoring', layout='wide')
st.title('Snowpipe Streaming monitoring')

preview_mode = os.getenv('DASHBOARD_PREVIEW') == '1'
if preview_mode:
    st.warning('Synthetic preview data only. No Snowflake connection or queries.')

try:
    local_connection = os.getenv('DASHBOARD_LOCAL_CONNECTION')
    if preview_mode:
        connection = None
    elif local_connection:
        connection = st.connection(local_connection, type='snowflake')
        st.warning('Local development connection active. Do not host this mode for other users.')
    else:
        connection = st.connection('snowflake-callers-rights')
except Exception:
    st.error('Connection unavailable. This example requires a container runtime with restricted caller\'s rights and administrator-configured caller grants. See README. No owner-rights fallback is used.')
    st.stop()

with st.sidebar.form('scope'):
    st.subheader('Event source')
    source = st.text_input('Event table or view', value=EVENT_VIEW)
    st.subheader('Ingestion target')
    database = st.text_input('Database', value='DEMO' if preview_mode else '')
    schema = st.text_input('Schema', value='STREAMING' if preview_mode else '')
    table = st.text_input('Table', value='EVENTS' if preview_mode else '')
    channel = st.text_input('Channel (optional)')
    time_range = st.selectbox('Time range', list(RANGES))
    include_messages = st.checkbox('Show error messages', value=False, help='Messages can contain customer data. Leave hidden when presenting or sharing screenshots.')
    submitted = st.form_submit_button('Load / refresh', icon=':material/refresh:')

if not submitted and not preview_mode:
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
        if preview_mode:
            from preview_data import preview_results
            results = preview_results(scope, include_messages)
        else:
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
