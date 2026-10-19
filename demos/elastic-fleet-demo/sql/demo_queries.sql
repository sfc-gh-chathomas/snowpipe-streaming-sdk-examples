-- Elastic Fleet demo: Snowsight worksheet queries for the presentation.
-- Read-only. No DDL, no DML, no credentials. Every query is scoped to one run
-- through the $run_id session variable.
--
-- EVENT_TS is TIMESTAMP_NTZ holding UTC; SYSDATE() is UTC too, so freshness math
-- is timezone-safe. Duplicate physical rows are allowed (at-least-once delivery):
-- charts and averages deduplicate by EVENT_ID; counts report physical and
-- distinct separately.

USE WAREHOUSE ELASTIC_DEMO_WH;

-- 0. Optional: find the run. The producer banner prints its run_id; this lists
--    the most recent runs if you need to look it up instead.
SELECT RUN_ID,
       MIN(EVENT_TS) AS FIRST_EVENT_TS,
       MAX(EVENT_TS) AS LAST_EVENT_TS,
       COUNT(*)      AS PHYSICAL_ROWS
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE EVENT_TS >= DATEADD(hour, -2, SYSDATE())
GROUP BY RUN_ID
ORDER BY LAST_EVENT_TS DESC
LIMIT 5;

-- Paste the run_id from the producer banner here, then run the queries below.
SET run_id = '00000000-0000-0000-0000-000000000000';

-- 1. Rows landing: running COUNT(*) for the run. Re-run to watch it climb.
SELECT COUNT(*) AS TOTAL_ROWS
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE RUN_ID = $run_id;

-- 2. Newest 12 rows (same shape as the dashboard "Live query" panel).
SELECT EVENT_ID, CLIENT_ID, DEVICE_ID, REGION, EVENT_TS,
       TEMPERATURE_C, CPU_PCT, TRAFFIC_MBPS, SCENARIO
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE RUN_ID = $run_id
ORDER BY EVENT_TS DESC, EVENT_ID DESC
LIMIT 12;

-- 3. Rows per second by EVENT_TS second, last 60 s of the run's data.
--    The newest second may still be filling (visibility trails acknowledgement).
SELECT DATE_TRUNC('second', EVENT_TS) AS EVENT_SECOND,
       COUNT(*)                       AS PHYSICAL_ROWS,
       COUNT(DISTINCT EVENT_ID)       AS DISTINCT_EVENTS,
       COUNT(DISTINCT CLIENT_ID)      AS CLIENTS
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE RUN_ID = $run_id
  AND EVENT_TS >= DATEADD(second, -60, (
        SELECT MAX(EVENT_TS)
        FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
        WHERE RUN_ID = $run_id))
GROUP BY EVENT_SECOND
ORDER BY EVENT_SECOND DESC;

-- 4. Fleet coverage: distinct clients, devices, regions, and freshness.
--    FRESHNESS_MS = now minus newest queried event (not ack latency).
SELECT COUNT(DISTINCT CLIENT_ID)                          AS CLIENTS,
       COUNT(DISTINCT DEVICE_ID)                          AS DEVICES,
       COUNT(DISTINCT REGION)                             AS REGIONS,
       MIN(EVENT_TS)                                      AS FIRST_EVENT_TS,
       MAX(EVENT_TS)                                      AS LAST_EVENT_TS,
       DATEDIFF('millisecond', MAX(EVENT_TS), SYSDATE())  AS FRESHNESS_MS
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE RUN_ID = $run_id;

-- 5a. Reconciliation: physical rows vs distinct events. Compare DISTINCT_EVENTS
--     with the producer's acknowledged_rows after the drain + visibility wait.
SELECT COUNT(*)                            AS PHYSICAL_ROWS,
       COUNT(DISTINCT EVENT_ID)            AS DISTINCT_EVENTS,
       COUNT(*) - COUNT(DISTINCT EVENT_ID) AS DUPLICATE_ROWS
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE RUN_ID = $run_id;

-- 5b. Which events arrived more than once (empty is normal).
SELECT EVENT_ID, COUNT(*) AS COPIES
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
WHERE RUN_ID = $run_id
GROUP BY EVENT_ID
HAVING COUNT(*) > 1
ORDER BY COPIES DESC, EVENT_ID
LIMIT 20;

-- 6a. Heat wave: per-region averages over the last 30 s of the run, deduplicated
--     by EVENT_ID. During a heat wave us-west should sit at the top.
WITH d AS (
  SELECT REGION, SCENARIO, TEMPERATURE_C, CPU_PCT, TRAFFIC_MBPS
  FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
  WHERE RUN_ID = $run_id
    AND EVENT_TS >= DATEADD(second, -30, (
          SELECT MAX(EVENT_TS)
          FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
          WHERE RUN_ID = $run_id))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TS) = 1
)
SELECT REGION,
       LISTAGG(DISTINCT SCENARIO, ',') WITHIN GROUP (ORDER BY SCENARIO) AS SCENARIOS,
       COUNT(*)                     AS EVENTS,
       ROUND(AVG(TEMPERATURE_C), 1) AS AVG_TEMPERATURE_C,
       ROUND(MAX(TEMPERATURE_C), 1) AS MAX_TEMPERATURE_C,
       ROUND(AVG(CPU_PCT), 1)       AS AVG_CPU_PCT,
       ROUND(AVG(TRAFFIC_MBPS), 1)  AS AVG_TRAFFIC_MBPS
FROM d
GROUP BY REGION
ORDER BY AVG_TEMPERATURE_C DESC;

-- 6b. Heat wave over time: average temperature per region in 5-second slices
--     across the whole run, deduplicated. In Snowsight choose Chart -> Line,
--     X = SLICE_START, Y = AVG_TEMPERATURE_C, series = REGION.
WITH d AS (
  SELECT REGION, EVENT_TS, TEMPERATURE_C
  FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY
  WHERE RUN_ID = $run_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TS) = 1
)
SELECT TIME_SLICE(EVENT_TS, 5, 'SECOND') AS SLICE_START,
       REGION,
       ROUND(AVG(TEMPERATURE_C), 1)      AS AVG_TEMPERATURE_C
FROM d
GROUP BY SLICE_START, REGION
ORDER BY SLICE_START, REGION;
