# Snowpipe Streaming monitoring dashboard

Create a Streamlit in Snowflake app to monitor ingestion from your event table. The dashboard shows row counts, error rate, server-side latency, channel activity, and recent errors for a selected target table.

## Create the app in Snowsight

You need only **`streamlit_app.py` and `pyproject.toml`**. The CLI template is optional.

1. Confirm that event collection is enabled for your ingestion target with `LOG_EVENT_LEVEL = INFO`. Snowflake provides a default event table; this app reads its `SNOWFLAKE.TELEMETRY.EVENTS_VIEW` view unless you choose another source. See [Monitor Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-event-table-telemetry).
2. Use an app execution role with permission to query the event source and use the required compute resources. `SNOWFLAKE.EVENTS_VIEWER` gives access to the default view, not a custom event table. For shared deployments, use a dedicated least-privilege owner role, not `ACCOUNTADMIN`.
3. In Snowsight, create a Streamlit app using **Run on container**, select a compute pool and query warehouse, and use Streamlit **1.53.1 or later** with Python 3.11.
4. Upload `streamlit_app.py` and `pyproject.toml`. Configure an approved package source as described in [Dependency management](https://docs.snowflake.com/en/developer-guide/streamlit/app-development/dependency-management).
5. Run the app. Enter the target database, schema, and table names exactly as stored in Snowflake, choose a time range, and select **Load / refresh**. No event data is queried before submission.

## Optional: Deploy with Snowflake CLI

With Snowflake CLI 3.14 or later, copy `snowflake.yml.example` to `snowflake.yml`, replace every placeholder with your account's settings, and run `snow streamlit deploy` from this folder. The template uses an approved external access integration for package installation. Do not commit the filled-in manifest or credentials.

## Access and sharing

The app uses the standard `st.connection("snowflake")` connection. Deployed apps query with [owner's rights](https://docs.snowflake.com/en/developer-guide/streamlit/object-management/owners-rights), not each viewer's table privileges. No caller grants are required by this connection. Queries disable result caching. This example is intended for Streamlit in Snowflake, not a publicly hosted local server.

Sharing the app can expose telemetry that viewers cannot query directly. Because viewers can change the source and target filters, grant the app owner access only to telemetry every intended viewer may see, preferably through a dedicated governed view. Filters and the hidden-by-default error-message option are not security controls. Do not share an `ACCOUNTADMIN`-owned app. Keep privileged Workspace tests private and verify the intended visibility before deployment.

## Interpreting the dashboard

- Error rate is rejected rows divided by parsed rows. Missing data or zero parsed rows displays **Unavailable**, not a healthy zero.
- Latency measures processing inside Snowflake, not total source-to-query time. Missing measurements are excluded.
- Byte totals are omitted because request-level bytes can repeat across channel events.
- OPEN/DROP events are operations, not active-channel counts. Row-error events might not include every rejected row.
- Each refresh runs five read-only queries for one table and at most 24 hours. Late-arriving events can cause differences between panels. SQL, container compute, and telemetry storage can incur costs.
- Event sources accept three unquoted identifiers (`DATABASE.SCHEMA.VIEW`). Use a simple-named view for sources requiring quoted identifiers.

This is an example, not a supported product or health guarantee. Local validation does not replace testing deployment, real event data, and access controls in your account. For SDK channel-status examples, see [Python](../python-example/monitoring) or [Java](../java-example/monitoring).
