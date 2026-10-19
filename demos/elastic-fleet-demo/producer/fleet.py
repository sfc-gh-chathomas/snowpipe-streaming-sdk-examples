"""Elastic Fleet producer: N independent Snowpipe Streaming clients, 10 routers each.

Run it yourself in a terminal. Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER (see README); it
prompts for a programmatic access token with hidden input unless ELASTIC_DEMO_PAT is set:

    .venv/bin/python -m producer.fleet --ramp 1,25,100,250 --ramp-interval 30

Keys while running: r = ramp to next target, h = heat wave (us-west), c = recover,
q = stop, drain and reconcile. Ctrl-C = q; a second Ctrl-C exits immediately.

Every client is its own StreamingIngestClient with its own Elastic channel on the same
table. Appends are fire-and-forget; acknowledgements arrive on the SDK's success/error
handlers and are counted by AckLedger. Nothing is ever replayed by this program: the SDK
does its own batching and retries.
"""

from __future__ import annotations

import argparse
import getpass
import json
import math
import os
import select
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait as wait_futures
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, TextIO

from producer import CONTRACT_VERSION, DB, SCHEMA, TABLE
from producer.ledger import CLOSED_CODES, AckLedger, error_code_name
from producer.signals import (DEFAULT_HEAT_REGION, DEVICES_PER_CLIENT, REGIONS, Scenario,
                              client_rows)

MAX_CLIENTS = 250
MAX_DURATION_S = 1800
DEFAULT_RAMP = (1, 25, 100, 250)


@dataclass
class FleetConfig:
    account: str = ""
    url: str = ""
    user: str = ""
    role: Optional[str] = None
    targets: List[int] = field(default_factory=lambda: [1, 25, 100, 250])
    ramp_interval_s: Optional[float] = None
    rate_hz: float = 1.0
    duration_s: float = 300.0
    heat_at_s: Optional[float] = None
    recover_at_s: Optional[float] = None
    heat_region: str = DEFAULT_HEAT_REGION
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    max_pending_rows: int = 50_000
    max_pending_bytes: int = 32 * 1024 * 1024
    drain_timeout_s: float = 60.0
    create_parallelism: int = 16
    status_json: Optional[str] = None


PAT_ENV_VAR = "ELASTIC_DEMO_PAT"  # optional; otherwise prompt with hidden input

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: Optional[datetime]) -> Optional[str]:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if ts else None


@dataclass
class _Slot:
    client_id: int
    client: object
    channel: object
    sequence: int = 0
    closed: bool = False


ClientFactory = Callable[[int, str], object]


class FleetProducer:
    """Owns the clients, the 1 Hz ticker, the scenario and the drain."""

    def __init__(self, config: FleetConfig, client_factory: ClientFactory,
                 on_event: Callable[[str], None] = lambda msg: None,
                 redact: Callable[[str], str] = lambda s: s,
                 clock: Callable[[], float] = time.monotonic,
                 now: Callable[[], datetime] = utc_now):
        self.config = config
        self._factory = client_factory
        self._emit = on_event
        self._redact = redact
        self._clock = clock
        self._now = now
        self.ledger = AckLedger(config.max_pending_rows, config.max_pending_bytes,
                                redact=redact, clock=clock)
        self._lock = threading.RLock()
        self._scenario = Scenario(heat_region=config.heat_region)
        self._slots: Dict[int, _Slot] = {}
        self._requested: set = set()
        self._target_index = 0
        self._stop = threading.Event()
        self._ticker: Optional[threading.Thread] = None
        self._creator = ThreadPoolExecutor(max_workers=config.create_parallelism,
                                           thread_name_prefix="fleet-create")
        self._creations: list = []
        self._drain_lock = threading.Lock()
        self._summary: Optional[dict] = None
        self.state = "idle"
        self.started_at: Optional[datetime] = None
        self.stopped_at: Optional[datetime] = None
        self._t0: Optional[float] = None
        self.failed_client_creations = 0
        self.late_ticks = 0
        self.stop_reason: Optional[str] = None

    # -- lifecycle ----------------------------------------------------------------------

    @property
    def target_clients(self) -> int:
        return self.config.targets[self._target_index]

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def start(self, ticker: bool = True) -> bool:
        """Begin creating clients and (unless ticker=False, for tests) the 1 Hz ticker."""
        with self._lock:
            if self.state != "idle":
                return False
            self.state = "starting"
            self.started_at = self._now()
            self._t0 = self._clock()
        self._emit(f"start: run_id={self.config.run_id} target={self.target_clients} clients "
                   f"x {DEVICES_PER_CLIENT} routers @ {self.config.rate_hz:g} Hz")
        self._ensure_clients()
        if ticker:
            self._ticker = threading.Thread(target=self._tick_loop, name="fleet-ticker",
                                            daemon=True)
            self._ticker.start()
        return True

    def wait_for_clients(self, timeout: Optional[float] = None) -> bool:
        """Wait for every requested client creation to finish (successfully or not)."""
        with self._lock:
            pending = list(self._creations)
        done, not_done = wait_futures(pending, timeout=timeout)
        return not not_done

    def request_stop(self, reason: str = "stop requested") -> bool:
        with self._lock:
            if self._stop.is_set():
                return False
            self._stop.set()
            self.stop_reason = reason
        self._emit(f"stop: {reason}; intake stopped, draining")
        return True

    def wait_stopped(self, timeout: Optional[float] = None) -> bool:
        return self._stop.wait(timeout)

    # -- controls -----------------------------------------------------------------------

    def ramp_next(self) -> bool:
        with self._lock:
            if self.stopping or self._target_index + 1 >= len(self.config.targets):
                return False
            self._target_index += 1
            target = self.target_clients
        self._emit(f"ramp: target {target} clients ({target * DEVICES_PER_CLIENT} routers)")
        self._ensure_clients()
        return True

    def heat(self, region: Optional[str] = None) -> bool:
        with self._lock:
            changed = not self.stopping and self._scenario.start_heat(region or self.config.heat_region)
            region = self._scenario.heat_region
        if changed:
            self._emit(f"heat wave: {region}")
        return changed

    def recover(self) -> bool:
        with self._lock:
            changed = not self.stopping and self._scenario.recover()
            region = self._scenario.heat_region
        if changed:
            self._emit(f"recover: {region} cooling")
        return changed

    # -- clients ------------------------------------------------------------------------

    def _ensure_clients(self) -> None:
        with self._lock:
            if self.stopping:
                return
            missing = [c for c in range(1, self.target_clients + 1) if c not in self._requested]
            self._requested.update(missing)
            for client_id in missing:
                self._creations.append(self._creator.submit(self._create_client, client_id))

    def _create_client(self, client_id: int) -> None:
        if self.stopping:
            return
        name = f"elastic-fleet-{self.config.run_id[:8]}-c{client_id:03d}"
        try:
            client = self._factory(client_id, name)
            channel = client.get_elastic_channel()
            channel.set_success_handler(self.ledger.on_success)
            channel.set_error_handler(self.ledger.on_error)
        except Exception as exc:  # noqa: BLE001 - reported, never fatal to other clients
            with self._lock:
                self.failed_client_creations += 1
                first_failure = not self._slots
            self.ledger.note_error(f"client {client_id}: {exc}")
            if first_failure and client_id == 1:
                with self._lock:
                    self.state = "error"
                self._emit(f"error: client 1 could not start: {self._redact(str(exc))[:200]}")
                self.request_stop("client 1 failed to start")
            return
        with self._lock:
            self._slots[client_id] = _Slot(client_id, client, channel)
            if self.state == "starting":
                self.state = "running"
            count = len(self._slots)
        if count in self.config.targets:
            self._emit(f"clients ready: {count}")

    # -- ticker -------------------------------------------------------------------------

    def elapsed(self) -> float:
        return 0.0 if self._t0 is None else self._clock() - self._t0

    def _scripted(self, elapsed: float, last_ramp: float) -> float:
        cfg = self.config
        if elapsed >= cfg.duration_s:
            self.request_stop(f"duration cap {cfg.duration_s:g}s reached")
            return last_ramp
        if cfg.ramp_interval_s and elapsed - last_ramp >= cfg.ramp_interval_s:
            if self.ramp_next():
                last_ramp = elapsed
        if cfg.heat_at_s is not None and elapsed >= cfg.heat_at_s:
            cfg.heat_at_s = None
            self.heat()
        if cfg.recover_at_s is not None and elapsed >= cfg.recover_at_s:
            cfg.recover_at_s = None
            self.recover()
        return last_ramp

    def _tick_loop(self) -> None:
        dt = 1.0 / self.config.rate_hz
        next_t = self._clock()
        last_ramp = 0.0
        while not self._stop.is_set():
            last_ramp = self._scripted(self.elapsed(), last_ramp)
            if self._stop.is_set():
                break
            self.tick(dt)
            next_t += dt
            delay = next_t - self._clock()
            if delay < -dt:
                # Fell behind by more than a tick: skip ahead instead of bursting to catch up.
                self.late_ticks += 1
                next_t = self._clock()
                delay = 0.0
            self._stop.wait(max(0.0, delay))

    def tick(self, dt: float) -> None:
        """Advance the scenario and submit one batch per active client."""
        with self._lock:
            recovered = self._scenario.step(dt)
            scenario = Scenario(self._scenario.heat_region, self._scenario.mode,
                                dict(self._scenario.heat_level))
            slots = [self._slots[c] for c in sorted(self._slots) if not self._slots[c].closed]
        if recovered:
            self._emit(f"recovered: {scenario.heat_region} back to normal")
        ts = self._now()
        for slot in slots:
            if self._stop.is_set():
                return
            self._submit(slot, scenario, ts)

    def _submit(self, slot: _Slot, scenario: Scenario, ts: datetime) -> None:
        rows = client_rows(self.config.run_id, slot.client_id, slot.sequence,
                           self.config.rate_hz, scenario, ts)
        nbytes = len(json.dumps(rows, separators=(",", ":")))
        token = (slot.client_id, slot.sequence)
        if not self.ledger.reserve(token, len(rows), nbytes):
            return  # over the pending bound: skip this tick; sequence is not consumed
        try:
            slot.channel.append_rows(rows, token)
        except Exception as exc:  # noqa: BLE001 - synchronous raise: nothing was enqueued
            self.ledger.release(token, exc)
            if error_code_name(exc) in CLOSED_CODES:
                slot.closed = True
            return
        slot.sequence += 1

    # -- drain --------------------------------------------------------------------------

    def drain(self) -> dict:
        """Stop intake, flush every channel, close every client. Idempotent."""
        with self._drain_lock:
            if self._summary is not None:
                return self._summary
            self.request_stop("drain")
            if self._ticker is not None:
                self._ticker.join()
            with self._lock:
                if self.state != "error":
                    self.state = "draining"
            self._creator.shutdown(wait=True, cancel_futures=True)
            with self._lock:
                slots = list(self._slots.values())
            deadline = self._clock() + self.config.drain_timeout_s
            flush_failures = self._parallel(slots, lambda s: s.channel.wait_for_flush(
                timeout_seconds=self._remaining(deadline)))
            close_failures = self._parallel(slots, lambda s: s.client.close(
                wait_for_flush=True, timeout_seconds=self._remaining(deadline)))
            with self._lock:
                if self.state != "error":
                    self.state = "stopped"
                self.stopped_at = self._now()
            counters = self.ledger.counters()
            reconciled_by_acks = (counters["pending_rows"] == 0 and counters["failed_rows"] == 0
                                  and counters["uncertain_rows"] == 0)
            self._summary = {
                **counters,
                "run_id": self.config.run_id,
                "state": self.state,
                "stop_reason": self.stop_reason,
                "clients_started": len(slots),
                "failed_client_creations": self.failed_client_creations,
                "flush_failures": flush_failures,
                "close_failures": close_failures,
                "late_ticks": self.late_ticks,
                "all_acknowledged": reconciled_by_acks,
                "verify_sql": verify_sql(self.config.run_id),
            }
            return self._summary

    def _remaining(self, deadline: float) -> int:
        return max(0, int(math.ceil(deadline - self._clock())))

    def _parallel(self, slots: List[_Slot], fn: Callable[[_Slot], None]) -> int:
        if not slots:
            return 0
        failures = 0
        with ThreadPoolExecutor(max_workers=min(32, len(slots)),
                                thread_name_prefix="fleet-drain") as pool:
            futures = [pool.submit(fn, s) for s in slots]
            wait_futures(futures)
            for slot, fut in zip(slots, futures):
                exc = fut.exception()
                if exc is not None:
                    failures += 1
                    self.ledger.note_error(f"client {slot.client_id} drain: {exc}")
        return failures

    # -- status -------------------------------------------------------------------------

    def status(self) -> dict:
        c = self.ledger.counters()
        with self._lock:
            active = sum(1 for s in self._slots.values() if not s.closed)
            if self.state in ("stopped", "error"):
                active = 0
            scenario, heat_region = self._scenario.mode, self._scenario.active_heat_region
            state, target = self.state, self.target_clients
        return {
            "contract_version": CONTRACT_VERSION,
            "run_id": self.config.run_id,
            "state": state,
            "scenario": scenario,
            "heat_region": heat_region,
            "started_at": iso(self.started_at),
            "stopped_at": iso(self.stopped_at),
            "updated_at": iso(self._now()),
            "target_clients": target,
            "active_clients": active,
            "devices_per_client": DEVICES_PER_CLIENT,
            "rate_hz": self.config.rate_hz,
            "append_calls": c["append_calls"],
            "submitted_rows": c["submitted_rows"],
            "acknowledged_rows": c["acknowledged_rows"],
            "pending_rows": c["pending_rows"],
            "pending_bytes": c["pending_bytes"],
            "failed_rows": c["failed_rows"],
            "uncertain_rows": c["uncertain_rows"],
            "errors": c["errors"],
            "sdk_observed_request_ids": c["sdk_observed_request_ids"],
            "sdk_retries": c["sdk_retries"],
            "acknowledged_rows_per_second": round(self.ledger.ack_rate(), 1),
            "backpressure_events": c["backpressure_events"],
            "limits": {
                "max_clients": MAX_CLIENTS,
                "max_duration_seconds": self.config.duration_s,
                "max_pending_rows": self.config.max_pending_rows,
                "max_pending_bytes": self.config.max_pending_bytes,
            },
            "last_error": c["last_error"],
        }


# -- SDK wiring --------------------------------------------------------------------------

def sdk_client_factory(config: FleetConfig, pat: str) -> ClientFactory:
    """Build real StreamingIngestClients. The PAT lives only in this closure's memory."""
    # The SDK logs INFO to stdout, which would interleave with the recorded status lines.
    # SS_LOG_LEVEL / SS_LOG_TARGET are the SDK's own knobs; an explicit user setting wins.
    os.environ.setdefault("SS_LOG_LEVEL", "warn")
    from snowflake.ingest.streaming import StreamingIngestClient

    def factory(client_id: int, client_name: str):
        props = {
            "account": config.account,
            "user": config.user,
            "url": config.url,
            "authorization_type": "PAT",
            "personal_access_token": pat,
        }
        if config.role:
            props["role"] = config.role
        return StreamingIngestClient.from_table(client_name, DB, SCHEMA, TABLE, properties=props)

    return factory


def sdk_version() -> str:
    try:
        from importlib.metadata import version
        return version("snowpipe-streaming")
    except Exception:  # noqa: BLE001
        return "unknown"


def verify_sql(run_id: str) -> str:
    return (f"SELECT COUNT(*) AS physical_rows, COUNT(DISTINCT EVENT_ID) AS distinct_events, "
            f"COUNT(*) - COUNT(DISTINCT EVENT_ID) AS duplicate_rows "
            f"FROM {DB}.{SCHEMA}.{TABLE} WHERE RUN_ID = '{run_id}';")


# -- terminal output ---------------------------------------------------------------------

def _kb(n: int) -> str:
    return f"{n / 1024:,.0f} KB" if n < 10 * 1024 * 1024 else f"{n / 1048576:,.1f} MB"


def _opt(n: Optional[int]) -> str:
    return "n/a" if n is None else f"{n:,}"


def format_status_line(s: dict) -> str:
    """One terminal line (~105 chars). Counters that are usually zero appear only when non-zero."""
    scen = s["scenario"] + (f"({s['heat_region']})" if s["heat_region"] else "")
    extras = [
        (label, n) for label, n in (
            ("uncertain", s["uncertain_rows"]),
            ("retries", s["sdk_retries"]),
            ("backpressure", s["backpressure_events"]),
            ("errors", s["errors"]),
        ) if n
    ]
    line = (f"{s['updated_at'][11:19]}Z {s['state']} | "
            f"clients {s['active_clients']}/{s['target_clients']} | "
            f"sent={s['submitted_rows']:,} acked={s['acknowledged_rows']:,} "
            f"in-flight={s['pending_rows']:,} failed={s['failed_rows']:,} | "
            f"{s['acknowledged_rows_per_second']:,.0f} rows/s | {scen}")
    if extras:
        line += " | " + " ".join(f"{label}={n:,}" for label, n in extras)
    return line


def format_banner(cfg: FleetConfig) -> str:
    return "\n".join([
        "=" * 78,
        " Elastic Fleet producer  (producer-side metrics; the dashboard shows queried rows)",
        f"   SDK            snowpipe-streaming {sdk_version()}",
        f"   Table          {DB}.{SCHEMA}.{TABLE}   channel ELASTIC (one per client)",
        f"   Run ID         {cfg.run_id}",
        f"   Clients        {' -> '.join(str(t) for t in cfg.targets)}  x {DEVICES_PER_CLIENT} "
        f"routers @ {cfg.rate_hz:g} Hz",
        f"   Caps           {cfg.duration_s:g}s duration, {cfg.max_pending_rows:,} pending rows, "
        f"{_kb(cfg.max_pending_bytes)} pending",
        "   Keys           r ramp   h heat wave   c recover   q stop+drain   (Ctrl-C = q)",
        "   Status line    in-flight = sent, awaiting SDK ack (not failures). Shown only when > 0:",
        "                  uncertain, retries, backpressure (skipped ticks), errors",
        "=" * 78,
    ])


def format_summary(summary: dict) -> str:
    verdict = ("all submitted rows acknowledged" if summary["all_acknowledged"] else
               "NOT fully acknowledged: pending/uncertain rows are unknown, not lost")
    return "\n".join([
        "-" * 78,
        f" Run {summary['run_id']} {summary['state']} ({summary['stop_reason']})",
        f"   clients started      {summary['clients_started']:,}"
        f"   (failed to start {summary['failed_client_creations']:,})",
        f"   append calls         {summary['append_calls']:,}",
        f"   rows submitted       {summary['submitted_rows']:,}",
        f"   rows acknowledged    {summary['acknowledged_rows']:,}",
        f"   rows pending         {summary['pending_rows']:,}",
        f"   rows failed          {summary['failed_rows']:,}   (server-reported)",
        f"   rows uncertain       {summary['uncertain_rows']:,}   (SDK-local failure; may have landed)",
        f"   SDK request IDs      {_opt(summary['sdk_observed_request_ids'])}"
        f"   retries {_opt(summary['sdk_retries'])} (retried rows may appear twice)",
        f"   backpressure/errors  {summary['backpressure_events']:,} / {summary['errors']:,}"
        f"   flush/close failures {summary['flush_failures']}/{summary['close_failures']}",
        f"   last error           {summary['last_error'] or '-'}",
        f"   verdict              {verdict}",
        "",
        " Reconcile after a few seconds (visibility lags acknowledgement):",
        f"   {summary['verify_sql']}",
        "-" * 78,
    ])


def write_status_json(path: str, status: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        json.dump(status, fh, indent=2)
    os.replace(tmp, path)


# -- CLI ---------------------------------------------------------------------------------

def _client_count(value: str) -> int:
    n = int(value)
    if not 1 <= n <= MAX_CLIENTS:
        raise argparse.ArgumentTypeError(f"client count must be 1..{MAX_CLIENTS}")
    return n


def _ramp(value: str) -> List[int]:
    steps = [_client_count(v.strip()) for v in value.split(",") if v.strip()]
    if not steps or steps != sorted(set(steps)):
        raise argparse.ArgumentTypeError("ramp must be strictly increasing, e.g. 1,25,100,250")
    return steps


def _run_id(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("run id must be a UUID") from exc


def _bounded(lo: float, hi: float):
    def parse(value: str) -> float:
        x = float(value)
        if not lo <= x <= hi:
            raise argparse.ArgumentTypeError(f"must be between {lo:g} and {hi:g}")
        return x
    return parse


def parse_args(argv: Optional[List[str]] = None) -> FleetConfig:
    ap = argparse.ArgumentParser(prog="producer.fleet", description=__doc__.split("\n\n")[0])
    ap.add_argument("--account", default=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                    help="account identifier, e.g. myorg-myaccount (env SNOWFLAKE_ACCOUNT)")
    ap.add_argument("--url", default=os.environ.get("SNOWFLAKE_URL", ""),
                    help="account URL (env SNOWFLAKE_URL); default https://<account>.snowflakecomputing.com")
    ap.add_argument("--user", default=os.environ.get("SNOWFLAKE_USER", ""),
                    help="Snowflake user that owns the token (env SNOWFLAKE_USER)")
    ap.add_argument("--role", default=os.environ.get("SNOWFLAKE_ROLE") or None,
                    help="role to use; must match the token's ROLE_RESTRICTION (env SNOWFLAKE_ROLE)")
    ap.add_argument("--clients", type=_client_count, default=1, help="starting client count")
    ap.add_argument("--ramp", type=_ramp, default=list(DEFAULT_RAMP),
                    help="client targets, e.g. 1,25,100,250 (r advances)")
    ap.add_argument("--ramp-interval", type=_bounded(1, MAX_DURATION_S), default=None,
                    help="seconds between automatic ramp steps")
    ap.add_argument("--rate-hz", type=_bounded(0.1, 10), default=1.0)
    ap.add_argument("--duration", type=_bounded(1, MAX_DURATION_S), default=300.0,
                    help=f"hard cap in seconds (max {MAX_DURATION_S})")
    ap.add_argument("--heat-at", type=_bounded(0, MAX_DURATION_S), default=None)
    ap.add_argument("--recover-at", type=_bounded(0, MAX_DURATION_S), default=None)
    ap.add_argument("--heat-region", choices=REGIONS, default=DEFAULT_HEAT_REGION)
    ap.add_argument("--run-id", type=_run_id, default=None)
    ap.add_argument("--max-pending-rows", type=int, default=50_000)
    ap.add_argument("--max-pending-mb", type=_bounded(1, 1024), default=32.0)
    ap.add_argument("--drain-timeout", type=_bounded(1, 600), default=60.0)
    ap.add_argument("--status-json", default=None, help="also write ProducerStatus JSON here")
    args = ap.parse_args(argv)
    targets = [args.clients] + [t for t in args.ramp if t > args.clients]
    account = args.account.strip()
    url = args.url.strip() or (f"https://{account.lower()}.snowflakecomputing.com" if account else "")
    cfg = FleetConfig(
        account=account, url=url, user=args.user.strip(), role=args.role,
        targets=targets, ramp_interval_s=args.ramp_interval, rate_hz=args.rate_hz,
        duration_s=args.duration, heat_at_s=args.heat_at, recover_at_s=args.recover_at,
        heat_region=args.heat_region, run_id=args.run_id or str(uuid.uuid4()),
        max_pending_rows=max(1, args.max_pending_rows),
        max_pending_bytes=int(args.max_pending_mb * 1024 * 1024),
        drain_timeout_s=args.drain_timeout, status_json=args.status_json,
    )
    return cfg


class _Keys:
    """Single-keypress input on a TTY; line input otherwise."""

    def __init__(self, stream: TextIO):
        self.stream = stream
        self._saved = None
        self.tty = stream.isatty()
        self.eof = False

    def __enter__(self):
        if self.tty:
            import termios
            import tty
            fd = self.stream.fileno()
            self._saved = termios.tcgetattr(fd)
            tty.setcbreak(fd)
        return self

    def __exit__(self, *exc):
        if self._saved is not None:
            import termios
            termios.tcsetattr(self.stream.fileno(), termios.TCSADRAIN, self._saved)

    def read(self, timeout: float) -> Optional[str]:
        if self.eof:
            # No more input (e.g. stdin is /dev/null): scripted flags and the duration cap drive the run.
            time.sleep(timeout)
            return None
        ready, _, _ = select.select([self.stream], [], [], timeout)
        if not ready:
            return None
        data = self.stream.read(1) if self.tty else self.stream.readline()
        if not data:
            self.eof = True
            return None
        return data.strip().lower()[:1]


def dispatch_key(producer: FleetProducer, key: Optional[str], say: Callable[[str], None]) -> None:
    if key == "r":
        if not producer.ramp_next():
            say("ramp: already at the last target")
    elif key == "h":
        producer.heat()
    elif key == "c":
        if not producer.recover():
            say("recover: no heat wave in progress")
    elif key == "q":
        producer.request_stop("q pressed")
    elif key == "?":
        say("keys: r ramp  h heat wave  c recover  q stop+drain")


def main(argv: Optional[List[str]] = None) -> int:
    cfg = parse_args(argv)
    out_lock = threading.RLock()  # re-entrant: the SIGINT handler may print mid-print

    def say(msg: str) -> None:
        with out_lock:
            print(msg, flush=True)

    say(format_banner(cfg))
    if not cfg.account or not cfg.user:
        say("set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER (or pass --account/--user); nothing started")
        return 2
    # Removed from the environment on read so nothing this process starts inherits it.
    pat = os.environ.pop(PAT_ENV_VAR, "").strip()
    if pat:
        say(f"token: from ${PAT_ENV_VAR}")
    else:
        try:
            pat = getpass.getpass("Snowflake programmatic access token (input hidden): ").strip()
        except (KeyboardInterrupt, EOFError):
            say("\ncancelled; nothing started")
            return 130
    if not pat:
        say("no token entered; nothing started")
        return 2

    def redact(text: str) -> str:
        return text.replace(pat, "***")

    def event(msg: str) -> None:
        say(f"{utc_now().strftime('%H:%M:%S')}Z >> {redact(msg)}")

    producer = FleetProducer(cfg, sdk_client_factory(cfg, pat), on_event=event, redact=redact)
    interrupts = {"n": 0}

    def on_sigint(_sig, _frame):
        interrupts["n"] += 1
        if interrupts["n"] == 1 and not producer.stopping:
            producer.request_stop("Ctrl-C")
            return
        s = producer.status()
        say(f"\nsecond Ctrl-C: exiting without drain. {s['pending_rows']:,} pending rows are "
            f"UNCERTAIN (may or may not land). Check: {verify_sql(cfg.run_id)}")
        os._exit(130)

    signal.signal(signal.SIGINT, on_sigint)
    status_path = cfg.status_json

    def show_status() -> None:
        s = producer.status()
        say(format_status_line(s))
        if status_path:
            write_status_json(status_path, s)

    producer.start()
    next_status = time.monotonic()
    with _Keys(sys.stdin) as keys:
        while not producer.stopping:
            dispatch_key(producer, keys.read(0.2), say)
            if time.monotonic() >= next_status:
                show_status()
                next_status += 2.0
    result: dict = {}
    drainer = threading.Thread(target=lambda: result.update(producer.drain()), daemon=True)
    drainer.start()
    while drainer.is_alive():
        drainer.join(2.0)
        show_status()
    say(format_summary(result))
    if status_path:
        write_status_json(status_path, producer.status())
    return 1 if result.get("state") == "error" else 0


if __name__ == "__main__":
    sys.exit(main())
