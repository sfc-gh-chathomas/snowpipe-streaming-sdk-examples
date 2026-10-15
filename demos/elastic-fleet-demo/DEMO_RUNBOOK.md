# Elastic Fleet Demo — Presenter Runbook

**Headline:** 2,500 routers. 250 independent producer clients. One table, one
Elastic channel name, no coordination.

The recording has two windows side by side:

- **Producer terminal** (`producer/fleet.py`), the producer side: clients, append
  calls, SDK acknowledgements, request IDs, rate, errors. You drive it with its
  scripted flags (plus optional keys).
- **Dashboard** (your App Runtime URL, or `http://127.0.0.1:3000` locally), the
  Snowflake side: only what SQL reads back from `ROUTER_TELEMETRY`.

The dashboard is read-only. It has no Start/Ramp/Heat/Stop buttons and doesn't
talk to the producer. Every action happens in the terminal, and the dashboard shows
its effect once the rows are queryable.

Setup, authentication, and deployment are in `README.md`. Numbers marked *measured*
come from rehearsal runs of this demo; yours may differ slightly.

---

## 1. What the audience sees

### Producer terminal

| Element | What it shows |
|---|---|
| Banner | SDK version (`snowpipe-streaming 1.8.1`), table and channel, **Run ID**, client ramp, caps, key help |
| Status line every 2 s (~110 chars) | `state`, `clients active/target`, `sent= acked= in-flight= failed=` (in-flight = sent, awaiting ack, not failures), `rows/s` (acknowledged), scenario (`normal`, `heat(us-west)`, `recover(us-west)`); `uncertain= retries= backpressure= errors=` appear only when > 0. Append calls and SDK request IDs are in the end-of-run summary. |
| `>>` event lines | Ramp steps, heat wave, recover, stop, drain, SDK errors (token redacted) |
| Final summary | Clients started, append calls, rows submitted / acknowledged / pending / failed / uncertain, verdict, and the reconciliation SQL for this run |

### Dashboard (`components/fleet-dashboard.tsx`, `components/live-query-panel.tsx`)

| Area | Label on screen | What it shows |
|---|---|---|
| Status bar | Badge **Connecting** / **Awaiting first data** / **Live** / **Stale** / **Error** / **No recent events**. **Run** (short id), **Last snapshot** time | Snapshot state |
| Metric toggle | **Temperature** · **CPU** · **Traffic** | Switches tiles and charts |
| Code button | **Code** | Opens the drawer **Snowpipe Streaming SDK · Elastic channel**, which shows `examples/minimal_elastic.py`. Close with Esc or ✕ |
| KPI strip, **Queried from Snowflake** | **Active clients** (of 250, event in last 10 s) · **Queryable rows / s** · **Queryable rows** · **Distinct events** · **Duplicate rows** (physical − distinct, amber if > 0) · **Reporting devices** (of 2,500) · **Latency p50 · last 30 s** (commit time − `EVENT_TS`, rows committed in the run's last 30 s; run-wide p50 underneath) | Run-scoped SQL |
| **Fleet · 250 SDK clients × 10 routers** | One tile per client, grouped in rows by region (us-west, us-east, eu-central, ap-south, sa-east), colored by the selected metric. Click to chart (max 3). Gray = no data, faded = no event in 10 s | Latest row per router |
| Router charts | `<Metric> (<unit>) · clients 1, 2, 3 · 24 routers`. Last 120 s, 1 s means, deduplicated by `EVENT_ID`, shared scale | Defaults to clients 1 (us-west), 2 (us-east), 3 (eu-central) |
| **Live query: ROUTER_TELEMETRY** | The exact SQL that ran (`RUN_ID = ?` bound), **COUNT(\*)**, and the newest 12 rows. New rows flash. `SCENARIO = heat` is amber | Re-run every 2 s |
| Footer | "Producer-side metrics … are printed in the producer terminal and are not shown here." | |

---

## 2. Three distinctions to make on camera

1. **One logical channel, not one connection.** Each of the 250 clients is its own
   `StreamingIngestClient` with its own network traffic. Each one calls
   `get_elastic_channel()` on the same table, so they all write to the same
   `ELASTIC` channel with nothing coordinating them.
2. **Acknowledged is not the same as queryable.** The terminal counts rows the SDK
   *acknowledged*. The dashboard counts rows a query can *see*. Visibility trails
   acknowledgement by a few seconds, so the dashboard lagging the terminal is
   expected. **Latency p50** is each row's Snowflake commit time
   (`METADATA$ROW_LAST_COMMIT_TIME`) minus its `EVENT_TS`: event to queryable. It's
   anchored on the newest commit, so it doesn't climb between commits or after the run.
3. **At-least-once, unordered.** Elastic delivery can produce duplicate physical
   rows. Every row has a stable `EVENT_ID` (`run_id:client_id:device_id:sequence`).
   The charts deduplicate on it, and **Duplicate rows** reports the difference on
   its own tile.

Also, where it comes up:

- **Append calls are not HTTP requests.** The SDK batches, compresses, and retries.
  `SDK request IDs` in the end-of-run summary comes from SDK callbacks and shows `n/a` when the
  SDK didn't report one. It's never derived from append calls.
- **Demo only.** The acknowledgement ledger lives in memory. This isn't a durable
  replay design.

---

## 3. Recording command and timeline

`scripts/take.sh` pins a new run ID, opens the dashboard on it (with
`ELASTIC_DEMO_DASHBOARD_URL` set), and starts the producer on this timeline:

```bash
scripts/take.sh
```

(Without a pinned run ID, the dashboard follows the newest run with events in the
last 15 minutes, which can briefly show the previous take.)

The flags fix the timeline. Times are seconds since the producer starts, which is
right after the producer has its token (prompt or `ELASTIC_DEMO_PAT`):

| t (s) | Producer | Expected on the dashboard (*measured*) |
|---|---|---|
| 0 | 1 client (10 routers) | First rows a few seconds later. ~**10 rows/s** |
| 30 | 25 clients | ~**250 rows/s** |
| 60 | 100 clients | ~**1,000 rows/s** |
| 90 | 250 clients | ~**2,500 rows/s** steady, **2,500** reporting devices |
| 110 | Heat wave, us-west | us-west tiles warm as a block. us-west average climbs from ~**44 °C** toward ~**67 °C** while other regions stay ~44 °C |
| 150 | Recover | us-west drifts back down (scenario `recover`, then `normal`) |
| 200 | Duration cap → stop intake, drain | Ingest stops. Counts settle after the visibility wait |

Measured in rehearsal: the ramp reached 10 / 250 / 1,000 / 2,500 rows/s by
`EVENT_TS`, with about 250,000 rows, 2,500 devices, and **0 duplicates**. The heat
wave took us-west from about 44 → 67 °C at 2,500 rows/s, with 0 duplicates. These are what
the demo generates (1 reading per router per second), not a throughput ceiling.

Keys still work during the scripted run if you need them: `r` next ramp,
`h` heat, `c` recover, `q` stop + drain. Don't press `h` early: it starts the heat
wave immediately and throws off the timeline.

---

## 4. Talk track (≈4 minutes, follows the timeline)

### Before t=0: Open (≈20 s)

*Dashboard pinned to the new run: badge **Awaiting first data**, tiles gray. Terminal
shows the banner (and the hidden token prompt, if `ELASTIC_DEMO_PAT` isn't set).*

> "This is a fleet of routers sending heartbeat telemetry (temperature, CPU,
> traffic) into one Snowflake table. On the left is the producer: plain Python,
> the Snowpipe Streaming SDK. On the right is a dashboard that does nothing but
> query that table. Nothing on the right is generated in the browser."

*If prompted, enter the token. Nothing is echoed. The producer starts.*

### t=0–30: One client (≈30 s)

> "One client, ten routers. It creates a client from the table, gets the Elastic
> channel, and appends rows. There's no offset token to track and no channel to
> open or close."

*Point at the terminal: `sent=` and `acked=` rising, `failed=0`. Then at the dashboard: the
badge goes to **Live**, the client 1 tile lights up, and rows flash in **Live query**.*

> "Producer side first: appends, then acknowledgements. A few seconds later
> Snowflake can query them. That gap is 'accepted durably' versus 'visible to a
> query'."

### t=30–110: Ramp (≈80 s)

> "Now scale out: 25 clients, then 100, then 250. These are 250 independent SDK
> clients, not one client with 250 threads, and every one writes to the same
> channel on this table. I don't coordinate them."

*Point at **Active clients**, **Queryable rows / s** stepping 250 → 1,000 → 2,500, and
the fleet grid filling region by region.*

> "At full scale that's one reading per router per second: about 2,500 rows a
> second, 2,500 reporting devices. That's what this demo generates, not a
> ceiling."

*Point at **Latency p50** staying in single-digit seconds while the fleet grows (rehearsal:
~4 s at 1 client, ~6–7 s for the last 30 s at 250 clients, ~6 s for the whole run). Read the
value off the screen.*

> "Event to queryable is a few seconds at the median, and it stays there as we go
> from one client to 250. Snowflake commits in batches every few seconds, so that's
> the batch wait plus the commit."

### t=110–150: Heat wave (≈40 s)

*Metric is **Temperature**. Client 1 (us-west) is already charted.*

> "A heat wave hits us-west. Every fifth client is in that region. You can see
> that whole row of tiles warm up, and in the charts client 1's routers climb
> while clients 2 and 3 stay flat."

*Point at `SCENARIO = heat` (amber) rows in **Live query**.*

> "us-west goes from around 44 degrees to the high 60s. Every one of those
> readings came in through the same channel and was queried back out."

### t=150–200: Recover and code (≈50 s)

> "Recovery. Temperatures drift back down."

*Click **Code**. The drawer shows `examples/minimal_elastic.py`.*

> "This is all the code. `from_table`, `get_elastic_channel`, `append_rows`. The
> SDK does batching, compression, and retries. The one thing the app owns is a
> stable event ID, because delivery is at-least-once."

*Press Esc.*

### Optional beat: Snowsight worksheet (≈30 s, during the ramp or recovery)

*Worksheet prepared from `sql/demo_queries.sql`, warehouse `ELASTIC_DEMO_WH`, with
`SET run_id = '<$RUN>';` run off camera.*

- Run **query 1** (`COUNT(*)`) twice a few seconds apart, then **query 2** (newest 12 rows).

  > "Same table, plain SQL. The count keeps climbing. These rows come from routers
  > in all five regions, through that one Elastic channel."

- After the heat wave: **query 6a** (per-region averages, us-west on top). Optionally
  **6b** as a line chart (X = `SLICE_START`, Y = `AVG_TEMPERATURE_C`, series = `REGION`).

This adds about 30 s. Cut it to keep the take under 5 minutes.

### t=200+: Stop and reconcile (≈45 s)

*The duration cap fires. The terminal shows the stop and drain, then the summary.*

> "The run stops taking new readings first, then waits for every pending row to be
> acknowledged. In-flight goes to zero."

*Wait a few seconds, then point at the summary's `rows acknowledged` and the
dashboard's **Distinct events** and **Duplicate rows**. Read the actual values off
the screen.*

> "Reconcile: the producer says this many rows were acknowledged, and Snowflake has
> the same number of distinct events for this run, with this many duplicate rows,
> which at-least-once allows. In rehearsal that was zero duplicates."

> "Read it honestly: distinct can be slightly *above* acknowledged if some
> acknowledgements were still uncertain when the drain finished. If it stayed
> *below* after the visibility wait, I'd call that missing, not hide it."

Optionally run **query 5a** in the worksheet. It's the same reconciliation in SQL.

### Close (≈10 s)

> "2,500 routers, 250 independent producers, one Elastic channel, and every row
> read back and reconciled in Snowflake."

---

## 5. Before you hit record

- [ ] Token valid: `SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER <user>;` shows it
      `ACTIVE` and not expiring before the session; your current IP is allowed by the
      network policy. The Snowsight result that showed the token is closed. Never on camera.
- [ ] About 10 minutes ahead: warehouse and app resumed (`README.md` → Run the demo),
      dashboard open, signed in, and showing data. Keep the tab visible.
- [ ] `RUN` generated, dashboard opened at `/?run_id=$RUN`, badge shows
      **Awaiting first data**, 0 rows.
- [ ] Producer command typed in the terminal and ready. The only thing on camera at
      start is the banner (and the hidden prompt). Clear the terminal scrollback (no
      environment output, no key files).
- [ ] Browser at 1920×1080 (16:9), zoom 100%. Bookmarks bar, extensions, and
      notifications hidden. Terminal font large enough to read the status line.
- [ ] Metric = **Temperature**. Charts show clients 1, 2, 3 (client 1 is us-west).
- [ ] **Code** drawer opens and shows `examples/minimal_elastic.py`.
- [ ] If using the Snowsight beat: one worksheet only, `ELASTIC_DEMO_WH`, `SET run_id` done.
      No other worksheets, query history, or account settings visible.
- [ ] Producer machine's clock synced to network time. Active clients, tile "reporting"
      and latency p50 compare producer-generated `EVENT_TS` with Snowflake time; more
      than ~10 s of skew shows 0 active clients.
- [ ] Do Not Disturb on. Recorder audio levels checked.

---

## 6. Resetting between takes: new run, not truncation

- Each take is a new producer run with a new run ID. Generate a new `RUN`, reopen the
  dashboard with it, and start the producer with `--run-id "$RUN"`. All dashboard
  queries are scoped to that run.
- **Don't `TRUNCATE`** `ROUTER_TELEMETRY` or any other table. Old runs stay and are
  out of view.
- Don't reuse a run ID. Event IDs would restart inside a run.
- Don't measure a take with `getChannelStatus` row counters. They're cumulative
  across runs. Use the run-scoped dashboard numbers or `sql/demo_queries.sql`.

---

## 7. Operator recovery during a take

| What you see | What it means | What to do |
|---|---|---|
| **Awaiting first data** for a few seconds after start | Rows acknowledged but not yet queryable | Normal. Explain the ack/visibility gap. |
| **Stale** badge, charts frozen at last good data | Snapshot query failing or slow for more than about 6 s, often a warehouse cold start | Keep going if it clears. Warm up the warehouse before presenting to avoid it. |
| **Error** badge with a query message | Snapshot SQL failed (auth, warehouse) | Stop the take. Fix off camera. |
| Terminal `fail`/`err` rising, or `>>` error lines | SDK or producer error | Press `q`. Retake with a new run ID. |
| Terminal shows `backpressure=` rising, `in-flight=` near the cap | Producer hit its pending-row/byte bound and is skipping ticks | Expected protection. Mention it. Don't raise caps mid-take. |
| HTTP 401/403 or auth error right after start | Token wrong or expired, role restriction mismatch, or your IP isn't allowed by the network policy | Off camera: check the token, `SNOWFLAKE_ROLE`, and network policy (`README.md` → step 2), then retake. |
| Drain ends with `in-flight`/`uncertain` > 0 | Drain timed out with uncertain acknowledgements | Report the count as-is ("unknown, not lost"). Wait, then reconcile. |
| Pressed a key at the wrong time | e.g. `h` early | Finish the take or `q`, then retake with a new run ID. |

---

## 8. Cost-safe shutdown

Close the dashboard tab when you finish: it queries every ~2 s, which keeps the
warehouse from auto-suspending.

- The producer stops itself at `--duration` (200 s in the recording command,
  1,800 s hard maximum), even if you walk away.
- After the last take, confirm the summary shows `rows pending 0` (or write down the
  uncertain count).
- Close the browser tab (and stop a local dev server) so the 2 s polling stops. The
  warehouse then auto-suspends (5 minutes with `sql/setup.sql` defaults).
- If you won't present again soon, suspend the app:
  `ALTER APPLICATION SERVICE ELASTIC_DEMO_DB.FLEET.ELASTIC_FLEET_DEMO SUSPEND;`
- Remove the token when you're done with the demo (`README.md` → Cleanup).

---

## 9. Setup and run commands (summary)

Full detail is in `README.md`.

```bash
# once
snow sql -f sql/setup.sql -c <connection>          # objects (idempotent)
# sql/auth_setup.sql: edit <PRODUCER_PUBLIC_IP>, run it in a Snowsight worksheet
#   (the token is shown once; keep it out of terminal scrollback)
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp app.yml.example app.yml                         # fill in placeholders
snow app deploy -c <connection> && snow app open -c <connection>

# each session
export SNOWFLAKE_ACCOUNT=<org-account> SNOWFLAKE_USER=ELASTIC_DEMO_SVC SNOWFLAKE_ROLE=ELASTIC_DEMO_PRODUCER
export ELASTIC_DEMO_DASHBOARD_URL=<url from snow app open>
read -rs ELASTIC_DEMO_PAT && export ELASTIC_DEMO_PAT   # optional; else the producer prompts

# each take
scripts/take.sh
```

---

## References

- [Elastic channels: getting started (SDK)](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Elastic channels: best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming SDK 2026 release notes (1.8.1)](https://docs.snowflake.com/en/release-notes/clients-drivers/snowpipe-streaming-sdk-2026)
- In this repo: `README.md` (setup, authentication, deploy), `sql/demo_queries.sql`
  (Snowsight worksheet, scoped by `$run_id`)
