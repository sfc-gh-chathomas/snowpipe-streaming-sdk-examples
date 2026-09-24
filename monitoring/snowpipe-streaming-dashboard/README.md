# Snowpipe Streaming monitoring dashboard example

A starting point for monitoring Snowpipe Streaming event-table records in your own Snowflake account. Includes bounded queries, explicit access handling, and a synthetic local preview. This is example source, not a supported Snowflake product or an availability guarantee.

## What it shows

- Ingested and rejected row counts, and rejected rows divided by parsed rows.
- Average and approximate p95 server-side processing time, with measurement coverage.
- Rows over time, top channels by row count, channel OPEN/DROP operations, and up to 50 recent error events.

Bytes are deliberately omitted: a commit request's bytes can repeat in multiple channel events. Channel operations are not active-channel counts. The app does not interpret offsets or prescribe recovery checkpoints.

## Access model

The hosted app uses `st.connection("snowflake-callers-rights")`. It does not fall back to the app owner's permissions. This requires a **container runtime and Streamlit 1.53.1 or later**. The connection is initialized at the top of each script run as required by the short-lived viewer token.

An administrator must configure restricted caller grants for the app owner and ensure each viewer has the underlying privileges for the intended event view and warehouse. Restricted caller's rights use the viewer's default role, not necessarily the role selected in Snowsight. Follow [Restricted caller's rights in Streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/features/restricted-callers-rights). This sample does not create roles, grants, policies, or integrations.

The default source is `SNOWFLAKE.TELEMETRY.EVENTS_VIEW`. The `SNOWFLAKE.EVENTS_VIEWER` application role provides query access to that view, not unrestricted base-table access. Application privileges and caller grants must be tested for your account. Do not deploy under an administrative role to work around an access error.

**Filters are not access controls.** Users can change the target or source to other objects that their authorized connection can query. If users must only see selected telemetry, an administrator must enforce that through grants and a governed view or row access policy. Hiding error messages by default reduces accidental display; it is not a data-masking policy.

There is no global data/resource cache, shared raw connector, or background query thread. Queries use the supported connection API with `ttl=0`, and results remain local to the current script run. Every refresh rechecks access by querying again. Do not add shared caching without testing viewer isolation.

## Deployment

1. Select an app database/schema, query warehouse, compute pool, and a least-privilege app owner. Configure viewer privileges and caller grants with your administrator.
2. Ensure Snowpipe Streaming events are collected for the target schema. `INFO` collection is needed for commits, latency, and lifecycle; `ERROR` alone does not provide volume totals. See the [monitoring guide](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-event-table-telemetry).
3. Use Snowflake CLI 3.14 or later. Copy `snowflake.yml.example` to `snowflake.yml` and replace every placeholder with your account's actual configuration. The generated configuration is ignored by Git; never publish account-specific configuration or credentials.
4. Configure an approved package source for the included `pyproject.toml`. The manifest shows an external access integration for package installation. Check your account's [dependency-management options](https://docs.snowflake.com/en/developer-guide/streamlit/app-development/dependency-management), including approved artifact repositories. No runtime application calls to external services are required.
5. Deploy from this directory with `snow streamlit deploy`. Deliberately omit `--replace` on the first deployment to avoid overwriting an existing app.
6. Open the app in Snowsight. Enter one target database, schema, and table using exact stored names. Choose a window up to 24 hours and select **Load / refresh**. No event query runs before submission.
7. Complete the sharing checks below before granting app access to additional users.

For Snowsight deployment, select the container runtime, query warehouse, and compute pool, then upload `streamlit_app.py`, `monitoring.py`, `preview_data.py`, and `pyproject.toml`. Configure package access and caller grants before running. No connection secrets file belongs in the deployed artifacts.

The source view currently accepts three unquoted SQL identifiers only; quoted or dotted identifier components are intentionally rejected. Use a simple-named governed view if your source requires quoted identifiers. Target names are bound as values, preserve case, and are not interpolated into SQL. The source name is also bound through `IDENTIFIER`.

## Local development

For an offline visual preview, set `DASHBOARD_PREVIEW=1` and run the app. Preview mode displays synthetic data, makes no Snowflake connection, and shows a persistent warning. It is never selected automatically after an authentication error. Filters change the synthetic scenario, not real data. Keep this mode off for deployment.

```bash
DASHBOARD_PREVIEW=1 uv run streamlit run streamlit_app.py --server.address 127.0.0.1 --browser.gatherUsageStats false
```

Use Python 3.11 and install the declared dependencies in an isolated environment. Select an existing named Snowflake connection through `DASHBOARD_LOCAL_CONNECTION`; do not put credentials in this project. Run `streamlit run streamlit_app.py`.

Local mode uses the selected connection's permissions and is **not appropriate for hosting or sharing with other users**. Do not set `DASHBOARD_LOCAL_CONNECTION` in a hosted deployment. Hosted mode is the default and fails closed when caller's rights are unavailable.

## Limits and costs

- Each refresh issues five SELECT queries sequentially. Time windows are bounded to 24 hours, all queries require one target table, and each query requests a 30-second timeout. Set warehouse/resource policies independently; an output limit does not guarantee a small scan or cost.
- No automatic refresh or unbounded history scan is provided. The default lookback is one hour.
- Object filters exclude error events that lack resolved object names. Empty results do not establish pipeline health.
- Row-error records can be representative rather than exhaustive. Commit counts, not the number of error records, supply the error-rate numerator.
- Latency is server-side processing time. Missing values are excluded; a lack of measurements is displayed as unavailable, not zero.
- Queries share fixed start/end bounds but run separately. Late-arriving telemetry can produce small differences between panels; this is not an atomic ingestion snapshot.
- Error messages can contain source data and are not selected unless explicitly requested. When selected, messages are truncated to 1,000 characters. Table/channel names and error codes may also be sensitive. Do not publish screenshots or exports with customer information.
- Container compute, SQL queries, and telemetry storage can incur charges. This app is not a billing dashboard.

## Test locally

From this directory:

```bash
python -m unittest discover -s tests -v
python -m py_compile monitoring.py streamlit_app.py
python -m unittest discover -s tests -p 'test_render.py' -v
```

The unit suite uses synthetic data and mocks, not account telemetry. It checks error-rate edge cases, missing-data states, bounded/bound queries, opt-in messages, and the application control flow. No credentials are required for these tests.

## Before public publication or customer deployment

- Test a fresh container-runtime deployment and record the exact installed dependency versions. Declared version ranges are not a validated lock file.
- Verify SDK event delivery and all five queries on the target account; local tests are not an end-to-end test.
- Test two viewers with different permissions. Confirm that unauthorized data cannot be queried, that no other viewer's results appear, and that missing caller grants fail without owner-rights fallback.
- Test a schema using `OFF`, one using `ERROR`, empty windows, all-rejected batches, and missing latency measurements.
- Remove private configuration, screenshots, logs, and data. Publish only the example sources, tests, and this documentation.
- Review the repository's [license](../../LICENSE) and ownership conventions. Example availability does not imply product support.

Local Python 3.11 tests and a synthetic Streamlit render have passed. Container deployment, real telemetry queries, and multi-user authorization still require validation. Public source distribution is distinct from public access to a running app or customer telemetry.
