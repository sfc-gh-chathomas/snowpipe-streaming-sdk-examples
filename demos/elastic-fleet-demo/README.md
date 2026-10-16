# Elastic Fleet Demo: Snowpipe Streaming Elastic Channels

A presentable demo of **Snowpipe Streaming Elastic Channels**. A simulated fleet of
2,500 routers (250 independent SDK clients × 10 routers) streams heartbeat telemetry
(temperature, CPU, traffic) into one Snowflake table, and a read-only dashboard
hosted on **Snowflake App Runtime** queries it back live.

**The story:** 250 independent producers, one table, every client writing to that
table's `ELASTIC` channel. No channel open/close, no offset tokens, no coordination
between producers.

Everything on the dashboard is queried from Snowflake. The sensor readings are
synthetic, but they're ingested by the real SDK and read back with SQL.

> **Sample code.** Provided as-is for demonstration and learning. It isn't an
> officially supported Snowflake product, and it isn't designed for production use.
> See [Notes and limits](#notes-and-limits).

---

## Contents

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  1. [Create the Snowflake objects](#1-create-the-snowflake-objects)
  2. [Set up producer authentication](#2-set-up-producer-authentication)
  3. [Install the producer](#3-install-the-producer)
  4. [Configure your environment](#4-configure-your-environment)
  5. [Run the dashboard locally (optional)](#5-run-the-dashboard-locally-optional)
  6. [Deploy the dashboard to Snowflake App Runtime](#6-deploy-the-dashboard-to-snowflake-app-runtime)
- [Run the demo](#run-the-demo)
- [Reconcile a run](#reconcile-a-run)
- [Cleanup](#cleanup)
- [Notes and limits](#notes-and-limits)
- [Security notes](#security-notes)

---

## How it works

```
 your laptop                                        Snowflake
 ┌──────────────────────────────────┐             ┌──────────────────────────────────────┐
 │ Python producer (producer/)      │  SDK, PAT   │ ELASTIC_DEMO_DB.FLEET                │
 │  N × StreamingIngestClient       │ ──────────► │   ROUTER_TELEMETRY                   │
 │  each → get_elastic_channel()    │ append_rows │   (default pipe, created on first    │
 │  terminal = producer metrics     │             │    use)                              │
 └──────────────────────────────────┘             │                                      │
                                                  │ App Runtime dashboard (Next.js)      │
 browser ◄──────── read-only SQL every ~2 s ───── │   warehouse ELASTIC_DEMO_WH          │
                                                  └──────────────────────────────────────┘
```

| Piece | Runs where | Does | Shows |
|---|---|---|---|
| **Producer** (`producer/fleet.py`) | Your terminal | Ramps 1 → 25 → 100 → 250 SDK clients, generates synthetic signals, a regional heat wave and recovery, then stops and drains | One status line: clients, `sent= acked= in-flight= failed=`, rows/s, scenario. Full counters in the end-of-run summary |
| **Dashboard** (`app/`, `components/`, `lib/`) | Snowflake App Runtime (or locally) | Read-only SQL against `ROUTER_TELEMETRY` | Active clients, queryable rows/s, rows, distinct events, duplicate rows, reporting devices, latency p50, 250 client tiles, router charts, a live latest-rows panel, and a code drawer |

The dashboard has **no control API**: it never starts, stops, or talks to the
producer. Producer counters ("acknowledged by the SDK") stay in the terminal and
queried numbers ("visible to a query") stay on the dashboard, so the two are never
mixed.

Every row carries a stable `EVENT_ID` (`run_id:client_id:device_id:sequence`) and a
`RUN_ID`. Elastic delivery is **at-least-once and unordered**, so the dashboard
deduplicates by `EVENT_ID` and reports `duplicate rows = physical rows − distinct
events` separately. Each producer run gets a new `RUN_ID`; don't truncate the table
between runs.

### Repository layout

```
├── producer/      fleet.py (CLI), ledger.py (acknowledgement ledger), signals.py (synthetic signals)
├── examples/      minimal_elastic.py: the smallest SDK producer; shown in the dashboard's Code drawer
├── scripts/       take.sh: one demo run (new run ID, open dashboard, start producer)
├── sql/           setup.sql (objects), auth_setup.sql (producer auth), demo_queries.sql (worksheet)
├── app/           Next.js pages and API routes: snapshot, latest-rows, code
├── components/    dashboard UI
├── lib/           query layer and dashboard logic
├── app.yml.example  App Runtime manifest template
└── DEMO_RUNBOOK.md  presenter talk track, timeline and checklist
```

---

## Prerequisites

- A Snowflake account with **Snowpipe Streaming high-performance architecture** and
  **Snowflake App Runtime** available.
- A role that can create a warehouse, database, schema, table, network rule and
  external access integration (`sql/setup.sql`), and roles, users and network policies
  (`sql/auth_setup.sql`). `ACCOUNTADMIN` works; least privilege is better.
- **Python 3.9+** (tested with 3.14) for the producer.
- **Node.js 20.9+** (tested with 22) and npm for the dashboard.
- **[Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)**
  with `snow app` commands (tested with 3.28) to deploy the dashboard.
- macOS or Linux shell for `scripts/take.sh`.

Tested with `snowpipe-streaming==1.8.1`.

---

## Setup

### 1. Create the Snowflake objects

Run `sql/setup.sql` once in a Snowsight worksheet (or `snow sql -f sql/setup.sql -c <connection>`).
It's idempotent (`IF NOT EXISTS` only) and creates:

| Object | Name | Notes |
|---|---|---|
| Warehouse | `ELASTIC_DEMO_WH` | X-Small, auto-suspend 5 min |
| Database / schema | `ELASTIC_DEMO_DB.FLEET` | |
| Table | `ROUTER_TELEMETRY` | 11 columns, `ROW_TIMESTAMP = TRUE` (powers the latency tile) |
| Network rule + integration | `ELASTIC_DEMO_NPM_RULE`, `ELASTIC_DEMO_NPM_EAI` | npm registry egress for the dashboard **build** only |

The final `SELECT` should list the 11 columns. Snowpipe Streaming creates the
table's default pipe (`ROUTER_TELEMETRY-STREAMING`) on first use, so there's no pipe DDL.

**Using other names?** Find-and-replace them in `sql/*.sql`, then use the same names
in `app.yml` and the `ELASTIC_DEMO_*` variables below.

### 2. Set up producer authentication

The producer authenticates to Snowpipe Streaming with a
[programmatic access token (PAT)](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens).
You create the token in Snowflake and give it to the producer at runtime. It's never
written to a file.

#### Recommended: a dedicated service user

`sql/auth_setup.sql` creates a user that can do exactly one thing: insert into the
demo table.

| Object | Name | Purpose |
|---|---|---|
| Role | `ELASTIC_DEMO_PRODUCER` | `USAGE` on the database and schema, `INSERT` on `ROUTER_TELEMETRY` (all Snowpipe Streaming needs for a default pipe) |
| User | `ELASTIC_DEMO_SVC` | `TYPE = SERVICE`: no password and no interactive login |
| Network policy | `ELASTIC_DEMO_PRODUCER_POLICY` | Service users can only use a PAT while subject to a network policy; this allows the address the producer runs from |
| Token | `ELASTIC_DEMO_PRODUCER_PAT` | Restricted to `ELASTIC_DEMO_PRODUCER`, expires in 7 days |

Steps:

1. Find the public IP address of the machine that will run the producer:
   ```bash
   curl -s https://checkip.amazonaws.com
   ```
2. Open `sql/auth_setup.sql`, replace `<PRODUCER_PUBLIC_IP>`, and run it as a role that
   can create roles, users and network policies. If your account already applies a
   network policy to all users, make sure it allows that address and skip the
   policy section.
3. The `ALTER USER ... ADD PROGRAMMATIC ACCESS TOKEN` result shows the token secret
   **once**. Copy it directly into step 4 or the producer's prompt. Don't save it in a
   file, a chat, a ticket, or a screenshot.
4. Confirm the token: `SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER ELASTIC_DEMO_SVC;`
   shows its role restriction, expiry and status.

If your network changes (a new Wi-Fi network or VPN), the policy blocks the token
until you add the new address to `ELASTIC_DEMO_PRODUCER_IPS`.

#### Alternative: a token on your own (human) user

You can also add a PAT to your own user. It needs a role restricted to what the
producer needs, and your user must be subject to a network policy that allows your
address when the token is used. Snowflake also lets a human user without a network
policy temporarily bypass that requirement (`MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT`),
but [Snowflake doesn't recommend it](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens);
prefer the service user.

```sql
ALTER USER <YOUR_USER> ADD PROGRAMMATIC ACCESS TOKEN ELASTIC_DEMO_PAT
  ROLE_RESTRICTION = 'ELASTIC_DEMO_PRODUCER'
  DAYS_TO_EXPIRY = 1
  COMMENT = 'Elastic Fleet demo producer';
```

Grant `ELASTIC_DEMO_PRODUCER` to your user first, then set `SNOWFLAKE_USER` to your
user name in step 4.

### 3. Install the producer

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt      # snowpipe-streaming==1.8.1
```

### 4. Configure your environment

The producer and the SDK example read these variables:

| Variable | Required | Example | Meaning |
|---|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | `myorg-myaccount` | Account identifier ([format](https://docs.snowflake.com/en/user-guide/admin-account-identifier)) |
| `SNOWFLAKE_USER` | yes | `ELASTIC_DEMO_SVC` | User that owns the token |
| `SNOWFLAKE_ROLE` | recommended | `ELASTIC_DEMO_PRODUCER` | Must match the token's role restriction |
| `SNOWFLAKE_URL` | no | `https://myorg-myaccount.snowflakecomputing.com` | Defaults to `https://<account>.snowflakecomputing.com` |
| `ELASTIC_DEMO_DATABASE` / `_SCHEMA` / `_TABLE` | no | `ELASTIC_DEMO_DB` / `FLEET` / `ROUTER_TELEMETRY` | Target table; defaults match `sql/setup.sql` |
| `ELASTIC_DEMO_DASHBOARD_URL` | no | `https://<id>.snowflakecomputing.app` | Your deployed dashboard; `take.sh` opens it per run |
| `ELASTIC_DEMO_PAT` | no | (never written down) | The token. If unset, the producer prompts with hidden input |

```bash
export SNOWFLAKE_ACCOUNT=myorg-myaccount
export SNOWFLAKE_USER=ELASTIC_DEMO_SVC
export SNOWFLAKE_ROLE=ELASTIC_DEMO_PRODUCER
```

**The token:** the simplest option is to let the producer prompt for it (input
hidden). To avoid pasting it on every run, read it into the current terminal session
without echo or shell history:

```bash
# bash / zsh
read -rs ELASTIC_DEMO_PAT && export ELASTIC_DEMO_PAT    # paste, Enter
```
```fish
# fish
read --silent --global --export ELASTIC_DEMO_PAT
```

The producer removes `ELASTIC_DEMO_PAT` from its own environment as soon as it reads
it. Clear it from your shell when you're done (`unset ELASTIC_DEMO_PAT`, or
`set -e ELASTIC_DEMO_PAT` in fish). Never `export ELASTIC_DEMO_PAT=<value>`: that
saves the token in shell history.

### 5. Run the dashboard locally (optional)

Locally, the dashboard queries with a connection from your
[`~/.snowflake/connections.toml`](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections),
so any authenticator configured there works (browser SSO, OAuth, key pair, and so
on). That connection's role needs `USAGE` on `ELASTIC_DEMO_WH`, `ELASTIC_DEMO_DB` and
`FLEET`, and `SELECT` on `ROUTER_TELEMETRY`.

```bash
npm install
SNOWFLAKE_CONNECTION_NAME=<your-connection> npx next dev -H 127.0.0.1 -p 3000
```

Open http://127.0.0.1:3000. The page follows the newest run that had events in the
last 15 minutes; add `?run_id=<uuid>` to pin one run.

### 6. Deploy the dashboard to Snowflake App Runtime

Only the read-only dashboard is deployed. `app.yml`'s `ignore:` list keeps the
producer, scripts, SQL, virtual environments and any key files out of the upload.

1. Create your manifest from the template and fill in every `<PLACEHOLDER>`:
   ```bash
   cp app.yml.example app.yml
   ```
   With the default names: `database: ELASTIC_DEMO_DB`, `schema: FLEET`,
   `query_warehouse: ELASTIC_DEMO_WH`, `build_eai: ELASTIC_DEMO_NPM_EAI`, and the same
   values in `environment_variables:`. `app.yml` is git-ignored so your values stay local.
2. Deploy with a role that can create an application service in that schema
   (see [App Runtime privileges](https://docs.snowflake.com/en/developer-guide/snowflake-app-runtime/privileges)):
   ```bash
   snow app validate -c <your-connection>
   snow app deploy   -c <your-connection>
   snow app open     -c <your-connection>     # prints and opens the https://...snowflakecomputing.app URL
   ```
3. The dashboard queries as the app service's owner (owner's rights). The owning role
   needs `USAGE` on `ELASTIC_DEMO_WH`, `ELASTIC_DEMO_DB` and `FLEET`, and `SELECT` on
   `ROUTER_TELEMETRY`.
4. To let others open it, grant access to their role:
   ```sql
   GRANT USAGE ON APPLICATION SERVICE ELASTIC_DEMO_DB.FLEET.ELASTIC_FLEET_DEMO TO ROLE <viewer_role>;
   ```
   Viewers sign in with their own Snowflake credentials.

Set `ELASTIC_DEMO_DASHBOARD_URL` to the URL from `snow app open` so `take.sh` can
open the dashboard for each run.

---

## Run the demo

**Warm up first.** The warehouse and the app suspend when idle and can take a minute
or more to start. About 10 minutes before presenting:

```sql
ALTER WAREHOUSE ELASTIC_DEMO_WH RESUME IF SUSPENDED;
ALTER APPLICATION SERVICE ELASTIC_DEMO_DB.FLEET.ELASTIC_FLEET_DEMO RESUME;
```

Then open the dashboard, sign in, and keep the tab visible. Its 2-second refresh
keeps both awake.

**One run:**

```bash
scripts/take.sh
```

It prints a new `run_id`, opens the dashboard on that run, and starts the
producer on a fixed timeline (seconds since start): 1 client at 0, 25 at 30, 100 at
60, 250 at 90, a us-west heat wave at 110, recovery at 150, then stop and drain at
200. The heat wave and recovery are automatic; don't press keys during a recording.

Variations (extra flags override the defaults):

```bash
scripts/take.sh --clients 250                                          # start at full scale
scripts/take.sh --clients 250 --heat-at 40 --recover-at 80 --duration 120
.venv/bin/python -m producer.fleet --help                              # all flags
```

Keys while it runs: `r` next ramp step, `h` heat wave, `c` recover, `q` stop and
drain (`Ctrl-C` = `q`; a second `Ctrl-C` exits immediately and marks pending rows as
uncertain).

The terminal status line, every 2 seconds:

```
21:25:26Z running | clients 250/250 | sent=248,030 acked=245,530 in-flight=2,500 failed=0 | 2,500 rows/s | heat(us-west)
```

`in-flight` means sent but not yet acknowledged, not failed. `uncertain=`,
`retries=`, `backpressure=` and `errors=` appear only when non-zero.

`DEMO_RUNBOOK.md` has the full talk track and checklist, and `sql/demo_queries.sql`
has Snowsight worksheet queries for showing rows landing live.

### The SDK code

`examples/minimal_elastic.py` is the smallest complete producer, and it's what the
dashboard's **Code** drawer shows:

```python
client = StreamingIngestClient.from_table("elastic-minimal", DATABASE, SCHEMA, "ROUTER_TELEMETRY", properties=props)
channel = client.get_elastic_channel()          # always named ELASTIC
channel.set_success_handler(on_success)
channel.set_error_handler(on_error)
channel.append_rows([row], append_token=seq)    # fire-and-forget
channel.wait_for_flush(timeout_seconds=60)
client.close(wait_for_flush=True, timeout_seconds=60)
```

Run it with the same environment variables: `.venv/bin/python examples/minimal_elastic.py`.

---

## Reconcile a run

After the producer stops, wait a few seconds for visibility, then compare the
producer's `rows acknowledged` with Snowflake:

```sql
SET run_id = '<run_id from the producer banner>';
SELECT COUNT(*) AS physical_rows, COUNT(DISTINCT EVENT_ID) AS distinct_events,
       COUNT(*) - COUNT(DISTINCT EVENT_ID) AS duplicate_rows
FROM ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY WHERE RUN_ID = $run_id;
```

Distinct events can come in slightly above acknowledged if some acknowledgements were
still uncertain when the drain finished. That's an unconfirmed acknowledgement, not
corruption. If distinct stays below acknowledged after the visibility wait, treat it
as missing.

---

## Cleanup

1. Stop the producer (`q`) and let it drain. Close the dashboard tab.
2. Remove the token:
   ```sql
   ALTER USER ELASTIC_DEMO_SVC REMOVE PROGRAMMATIC ACCESS TOKEN ELASTIC_DEMO_PRODUCER_PAT;
   ```
   Clear `ELASTIC_DEMO_PAT` from your shell.
3. Suspend or remove the dashboard:
   `ALTER APPLICATION SERVICE ELASTIC_DEMO_DB.FLEET.ELASTIC_FLEET_DEMO SUSPEND;` or
   `snow app teardown --force -c <your-connection>`.
4. To remove everything, drop the application service, `ELASTIC_DEMO_DB`,
   `ELASTIC_DEMO_WH`, `ELASTIC_DEMO_NPM_EAI`, `ELASTIC_DEMO_PRODUCER_POLICY`,
   `ELASTIC_DEMO_SVC` and `ELASTIC_DEMO_PRODUCER`.

The warehouse and app auto-suspend when idle (warehouse 5 min from `setup.sql`, app
2 hours from `app.yml`), so an idle demo costs little, but suspend the app if you
won't use it for a while.

---

## Notes and limits

- **One channel name, many clients.** Each client is its own `StreamingIngestClient`,
  and each calls `get_elastic_channel()` on the same table. "One channel" means one
  logical ingestion target, not one connection.
- **Acknowledged ≠ queryable.** An acknowledgement means Snowflake has durably accepted
  the data; rows become queryable a few seconds later. **Latency p50** on the dashboard
  is each row's commit time (`METADATA$ROW_LAST_COMMIT_TIME`) minus its `EVENT_TS`: event
  to queryable, including client buffering. It uses the producer machine's clock for
  `EVENT_TS`, so keep that clock synced.
- **`wait_for_flush`** waits until Snowflake has acknowledged everything sent before the
  call. It doesn't mean every row succeeded (failures go to the error handler) or that
  rows are queryable yet.
- **Throughput.** The demo generates 1 reading per router per second (2,500 rows/s at 250
  clients). That's what the demo produces, not a limit of the service.
- **Demo only.** The acknowledgement ledger lives in memory and nothing is replayed after
  a crash. A production producer needs a durable source or buffer for unacknowledged
  data; see the Elastic Channels
  [best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices).
- `lib/snowflake.ts` and `next.config.mjs` come from the Snowflake App Runtime Next.js
  template.

## Security notes

- No credentials are stored in this repository. The producer receives the token at
  runtime (hidden prompt or `ELASTIC_DEMO_PAT`), keeps it in memory, and redacts it
  from any error text it prints.
- The deployed dashboard is read-only, uses no secrets, and authenticates with the
  app service's own identity.
- `.gitignore` excludes `app.yml`, `.env*`, virtual environments, and key files
  (`*.p8`, `*.pem`, `*.key`). Keep it that way when you fork.
- Restrict tokens with `ROLE_RESTRICTION`, keep their expiry short, and remove them
  after the demo.

## References

- [Elastic Channels: getting started (SDK)](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Elastic Channels: best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming access control](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-access-control)
- [Programmatic access tokens](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens)
- [Snowflake App Runtime](https://docs.snowflake.com/en/developer-guide/snowflake-app-runtime/about-snowflake-app-runtime)

## License

This demo is part of this repository and is licensed under the repository's
[CC BY 4.0 license](../../LICENSE).

It's sample code provided as-is, without warranty of any kind, for demonstration
and learning. It isn't an officially supported Snowflake product.

See also [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
