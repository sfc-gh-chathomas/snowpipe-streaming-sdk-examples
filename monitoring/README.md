# Snowpipe Streaming monitoring dashboard

Create a Streamlit in Snowflake app to monitor ingestion from your event table. The dashboard shows row counts, error rate, server-side latency, channel activity, and recent errors for a selected target table.

## Create the app in Snowsight

You need only **`streamlit_app.py` and `pyproject.toml`**. The CLI template is optional.

1. Confirm that event collection is enabled for your ingestion target with `LOG_EVENT_LEVEL = INFO`. Snowflake provides a default event table; this app reads its `SNOWFLAKE.TELEMETRY.EVENTS_VIEW` view unless you choose another source. See [Monitor Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-event-table-telemetry).
2. Have your administrator configure viewer permissions and the app owner's [restricted caller grants](https://docs.snowflake.com/en/developer-guide/streamlit/features/restricted-callers-rights) for the event source and required resources. `SNOWFLAKE.EVENTS_VIEWER` gives access to the default view, not the base table. Caller grants and underlying viewer privileges are both required.
3. In Snowsight, create a Streamlit app using **Run on container**, select a compute pool and query warehouse, and use Streamlit **1.53.1 or later** with Python 3.11.
4. Upload `streamlit_app.py` and `pyproject.toml`. Configure an approved package source as described in [Dependency management](https://docs.snowflake.com/en/developer-guide/streamlit/app-development/dependency-management).
5. Run the app. Enter the target database, schema, and table names exactly as stored in Snowflake, choose a time range, and select **Load / refresh**. No event data is queried before submission.

## Optional: Deploy with Snowflake CLI

With Snowflake CLI 3.14 or later, copy `snowflake.yml.example` to `snowflake.yml`, replace every placeholder with your account's settings, and run `snow streamlit deploy` from this folder. The template uses an approved external access integration for package installation. Do not commit the filled-in manifest or credentials.

## Access and sharing

The app queries with the viewer's restricted caller rights, not the app owner's privileges. It has no owner-rights fallback and uses no shared result cache. The viewer's default role applies, which can differ from the role selected in Snowsight. This example is intended for Streamlit in Snowflake, not a publicly hosted local server.

Filters are not security controls. Restrict access with grants or a governed event view before sharing the app. Error messages are hidden by default because they can contain customer data; names and error codes can also be sensitive. Verify access with two differently privileged viewers before sharing broadly.

## Interpreting the dashboard

- Error rate is rejected rows divided by parsed rows. Missing data or zero parsed rows displays **Unavailable**, not a healthy zero.
- Latency measures processing inside Snowflake, not total source-to-query time. Missing measurements are excluded.
- Byte totals are omitted because request-level bytes can repeat across channel events.
- OPEN/DROP events are operations, not active-channel counts. Row-error events might not include every rejected row.
- Each refresh runs five read-only queries for one table and at most 24 hours. Late-arriving events can cause differences between panels. SQL, container compute, and telemetry storage can incur costs.
- Event sources accept three unquoted identifiers (`DATABASE.SCHEMA.VIEW`). Use a simple-named view for sources requiring quoted identifiers.

This is an example, not a supported product or health guarantee. Local validation does not replace testing deployment, real event data, and access controls in your account. For SDK channel-status examples, see [Python](../python-example/monitoring) or [Java](../java-example/monitoring).
