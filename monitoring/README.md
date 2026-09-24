# Snowpipe Streaming monitoring dashboard

Create a Streamlit in Snowflake app to monitor ingestion from your event table. The dashboard shows row counts, error rate, server-side latency, channel activity, and recent errors for a selected target table.

## Create the app in Snowsight

You need only **`streamlit_app.py` and `pyproject.toml`**. The CLI template is optional.

1. Confirm that event collection is enabled for your ingestion target with `LOG_EVENT_LEVEL = INFO`. Snowflake provides a default event table; this app reads its `SNOWFLAKE.TELEMETRY.EVENTS_VIEW` view unless you choose another source. See [Monitor Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-event-table-telemetry).
2. Use an app execution role with permission to query the event source and use the required compute resources. `SNOWFLAKE.EVENTS_VIEWER` gives access to the default view, not a custom event table. For shared deployments, use a dedicated least-privilege owner role, not `ACCOUNTADMIN`.
3. In Snowsight Workspaces, create a Streamlit app with a compute pool and query warehouse. Workspace apps use the container runtime. If your UI offers a runtime choice, select **Run on container**. Use Streamlit **1.53.1 or later** with Python 3.11.
4. Upload `streamlit_app.py` and `pyproject.toml`. Before running, attach an approved package source. Where available, select `snowflake.snowpark.pypi_shared_repository` in the app settings; your role needs access through `SNOWFLAKE.PYPI_REPOSITORY_USER`. Alternatively, attach an administrator-approved PyPI external access integration. Creating an integration without attaching it to the app is not sufficient. See [Dependency management](https://docs.snowflake.com/en/developer-guide/streamlit/app-development/dependency-management).
5. Run the app. Set **Event table or view** to the monitoring destination, not the table receiving streamed rows. Use `SNOWFLAKE.TELEMETRY.EVENTS_VIEW` for the default destination, or your configured custom event table/view. Set **Database**, **Schema**, and **Table** to the ingestion target, using exact stored names. Leave **Channel** blank to include all channels for that table.
6. Send a fresh batch through Snowpipe Streaming, choose a time range that includes it, and select **Load / refresh**. Allow time for telemetry to arrive. Existing target-table rows do not generate new streaming events, and enabling collection does not backfill old events. No event data is queried before submission.

To identify your active event-table destination, check the `EVENT_TABLE` parameter at account and target-database scope as described in [Event table setup](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up). A database override takes precedence over the account setting. Keep an existing destination unless you intentionally want to change routing for other workloads.

## Optional: Deploy with Snowflake CLI

With Snowflake CLI 3.14 or later, copy `snowflake.yml.example` to `snowflake.yml`, replace every placeholder with your account's settings, and run `snow streamlit deploy` from this folder. The template uses an approved external access integration for package installation. Do not commit the filled-in manifest or credentials.

## Access and sharing

The app uses the standard `st.connection("snowflake")` connection. Deployed apps query with [owner's rights](https://docs.snowflake.com/en/developer-guide/streamlit/object-management/owners-rights), not each viewer's table privileges. No caller grants are required by this connection. Queries disable result caching. This example is intended for Streamlit in Snowflake, not a publicly hosted local server.

Sharing the app can expose telemetry that viewers cannot query directly. Because viewers can change the source and target filters, grant the app owner access only to telemetry every intended viewer may see, preferably through a dedicated governed view. Filters and the hidden-by-default error-message option are not security controls. Do not share an `ACCOUNTADMIN`-owned app. Keep privileged Workspace tests private and verify the intended visibility before deployment.

## Troubleshooting

- **Failed to retrieve packages / PyPI DNS error:** Check that the package repository or approved external access integration is attached to this development app. After changing settings, restart the app. Package installation happens before dashboard code runs.
- **No matching events:** Verify the event source, target names, effective `LOG_EVENT_LEVEL`, recent streaming activity, and time range. Run a query from the monitoring guide to distinguish missing telemetry from an app-access issue.
- **Telemetry could not be loaded:** Use the diagnostic line beneath the message to identify the failing query stage, exception type, error code, SQLSTATE, and query ID when available. Inspect the query ID in Snowflake Query History. The app deliberately does not display raw database errors or SQL text.
- **An error mentions restricted caller rights or caller grants:** Check that you are using this version's `st.connection("snowflake")`, not the earlier `snowflake-callers-rights` example. That is a different access model requiring extra grants, even when the viewer has an administrative role. Do not grant access to managed system roles merely to work around the error.

The queries use `?` bind placeholders required by Streamlit's Snowflake connection. If adapting the SQL, do not replace them with `%s` or interpolate user input.

## Interpreting the dashboard

- Error rate is rejected rows divided by parsed rows. Missing data or zero parsed rows displays **Unavailable**, not a healthy zero.
- Latency measures processing inside Snowflake, not total source-to-query time. Missing measurements are excluded.
- Byte totals are omitted because request-level bytes can repeat across channel events.
- OPEN/DROP events are operations, not active-channel counts. Row-error events might not include every rejected row.
- Each refresh runs five read-only queries for one table and at most 24 hours. Late-arriving events can cause differences between panels. SQL, container compute, and telemetry storage can incur costs.
- Event sources accept three unquoted identifiers (`DATABASE.SCHEMA.VIEW`). Use a simple-named view for sources requiring quoted identifiers.

This example has been exercised in a private Workspace against fresh Snowpipe Streaming telemetry using the standard Snowflake connection. Shared deployment and intended viewer visibility still require validation in your account. This is an example, not a supported product or health guarantee. For SDK channel-status examples, see [Python](../python-example/monitoring) or [Java](../java-example/monitoring).
