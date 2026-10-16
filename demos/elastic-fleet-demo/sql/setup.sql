-- Elastic Fleet demo: one-time setup.
--
-- Creates a warehouse, database, schema, telemetry table, and the build-time
-- external access integration the dashboard needs for `npm ci`.
-- Idempotent (IF NOT EXISTS only). Run as a role that can create these objects
-- (see README "Prerequisites"). To use other names, find-and-replace
-- ELASTIC_DEMO_DB, FLEET, ELASTIC_DEMO_WH and ELASTIC_DEMO_NPM_* here, then use
-- the same names in app.yml and the ELASTIC_DEMO_* environment variables.
--
-- Snowpipe Streaming creates the table's default pipe (ROUTER_TELEMETRY-STREAMING)
-- on first use; no CREATE PIPE is needed.

CREATE WAREHOUSE IF NOT EXISTS ELASTIC_DEMO_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Elastic Fleet demo: dashboard and worksheet queries';
USE WAREHOUSE ELASTIC_DEMO_WH;

CREATE DATABASE IF NOT EXISTS ELASTIC_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS ELASTIC_DEMO_DB.FLEET
  COMMENT = 'Elastic Fleet demo';

-- ROW_TIMESTAMP = TRUE exposes METADATA$ROW_LAST_COMMIT_TIME, which the dashboard's
-- "Latency p50" tile uses (commit time minus EVENT_TS).
CREATE TABLE IF NOT EXISTS ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY (
  EVENT_ID      VARCHAR        NOT NULL,  -- run_id:client_id:device_id:sequence
  RUN_ID        VARCHAR        NOT NULL,
  CLIENT_ID     NUMBER(4,0)    NOT NULL,  -- 1..250
  DEVICE_ID     VARCHAR        NOT NULL,  -- RTR-001-01 .. RTR-250-10
  REGION        VARCHAR        NOT NULL,
  SEQUENCE      NUMBER(18,0)   NOT NULL,
  EVENT_TS      TIMESTAMP_NTZ(3) NOT NULL, -- UTC
  TEMPERATURE_C FLOAT          NOT NULL,
  CPU_PCT       FLOAT          NOT NULL,
  TRAFFIC_MBPS  FLOAT          NOT NULL,
  SCENARIO      VARCHAR        NOT NULL   -- normal | heat | recover
)
ROW_TIMESTAMP = TRUE
COMMENT = 'Synthetic router events for the Elastic Fleet demo; EVENT_TS is UTC; EVENT_ID enables at-least-once reconciliation';

-- Build-time egress for the dashboard's `npm ci` (Snowflake App Runtime build step).
CREATE NETWORK RULE IF NOT EXISTS ELASTIC_DEMO_DB.FLEET.ELASTIC_DEMO_NPM_RULE
  MODE = EGRESS TYPE = HOST_PORT
  VALUE_LIST = ('registry.npmjs.org', 'cdn.npmjs.com');
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS ELASTIC_DEMO_NPM_EAI
  ALLOWED_NETWORK_RULES = (ELASTIC_DEMO_DB.FLEET.ELASTIC_DEMO_NPM_RULE)
  ENABLED = TRUE
  COMMENT = 'Elastic Fleet demo: npm registry access during app build';

-- Shape check: expect exactly the 11 columns above, in order.
SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE,
       DATETIME_PRECISION, IS_NULLABLE
FROM ELASTIC_DEMO_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'FLEET' AND TABLE_NAME = 'ROUTER_TELEMETRY'
ORDER BY ORDINAL_POSITION;
