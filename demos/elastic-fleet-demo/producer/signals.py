"""Deterministic, evolving router telemetry and the regional heat-wave scenario.

Every reading is a pure function of (device, sequence, rate, regional heat level), so the
same run produces the same shape every time and tests can assert exact values. Wall-clock
time only affects EVENT_TS, never the measurements.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

REGIONS = ("us-west", "us-east", "eu-central", "ap-south", "sa-east")
DEVICES_PER_CLIENT = 10
DEFAULT_HEAT_REGION = "us-west"

WAVE_PERIOD_S = 120.0      # one visible load cycle per 2-minute dashboard window
HEAT_RISE_TAU_S = 15.0     # heat wave reaches ~63% in 15 s
HEAT_DECAY_TAU_S = 10.0    # recovery decays ~63% in 10 s
RECOVERED_BELOW = 0.03     # heat level at which "recover" becomes "normal" again


def region_for(client_id: int) -> str:
    return REGIONS[(client_id - 1) % len(REGIONS)]


def device_id(client_id: int, device: int) -> str:
    return f"RTR-{client_id:03d}-{device:02d}"


@dataclass(frozen=True)
class RouterProfile:
    """Static per-router characteristics, stable across runs."""

    base_temp_c: float
    base_cpu_pct: float
    base_traffic_mbps: float
    phase: float

    @classmethod
    def for_device(cls, dev_id: str) -> "RouterProfile":
        rng = random.Random(f"profile:{dev_id}")
        return cls(
            base_temp_c=38.0 + 8.0 * rng.random(),
            base_cpu_pct=15.0 + 25.0 * rng.random(),
            base_traffic_mbps=50.0 + 400.0 * rng.random(),
            phase=2.0 * math.pi * rng.random(),
        )


@dataclass
class Scenario:
    """Fleet-wide scenario: normal, a heat wave in one region, then recovery."""

    heat_region: str = DEFAULT_HEAT_REGION
    mode: str = "normal"  # normal | heat | recover
    heat_level: Dict[str, float] = field(default_factory=lambda: {r: 0.0 for r in REGIONS})

    def start_heat(self, region: str = DEFAULT_HEAT_REGION) -> bool:
        if region not in REGIONS:
            raise ValueError(f"unknown region {region!r}")
        if self.mode == "heat" and self.heat_region == region:
            return False
        self.heat_region = region
        self.mode = "heat"
        return True

    def recover(self) -> bool:
        if self.mode != "heat":
            return False
        self.mode = "recover"
        return True

    def step(self, dt: float) -> bool:
        """Advance heat levels by dt seconds. Returns True when recovery completes."""
        for region in REGIONS:
            level = self.heat_level[region]
            if self.mode == "heat" and region == self.heat_region:
                level += (1.0 - level) * min(1.0, dt / HEAT_RISE_TAU_S)
            else:
                level -= level * min(1.0, dt / HEAT_DECAY_TAU_S)
            self.heat_level[region] = level
        if self.mode == "recover" and self.heat_level[self.heat_region] < RECOVERED_BELOW:
            self.mode = "normal"
            return True
        return False

    def label_for(self, region: str) -> str:
        """SCENARIO column value: only the affected region is labelled heat/recover."""
        if self.mode != "normal" and region == self.heat_region:
            return self.mode
        return "normal"

    @property
    def active_heat_region(self):
        return self.heat_region if self.mode != "normal" else None


def reading(dev_id: str, sequence: int, rate_hz: float, heat: float) -> Dict[str, float]:
    """Measurements for one router at one sequence number."""
    p = RouterProfile.for_device(dev_id)
    t = sequence / rate_hz
    wave = math.sin(2.0 * math.pi * t / WAVE_PERIOD_S + p.phase)
    noise = random.Random(f"noise:{dev_id}:{sequence}")

    traffic = p.base_traffic_mbps * (1.0 + 0.35 * wave) * (1.0 - 0.25 * heat)
    traffic += noise.gauss(0.0, 0.03 * p.base_traffic_mbps)
    cpu = p.base_cpu_pct + 12.0 * (wave + 1.0) + 25.0 * heat + noise.gauss(0.0, 1.5)
    temp = p.base_temp_c + 2.5 * wave + 0.05 * cpu + 22.0 * heat + noise.gauss(0.0, 0.3)
    return {
        "TEMPERATURE_C": round(temp, 2),
        "CPU_PCT": round(min(100.0, max(0.0, cpu)), 2),
        "TRAFFIC_MBPS": round(max(0.0, traffic), 2),
    }


def format_event_ts(ts: datetime) -> str:
    """UTC, millisecond precision, no zone suffix (column is TIMESTAMP_NTZ holding UTC)."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def client_rows(run_id: str, client_id: int, sequence: int, rate_hz: float,
                scenario: Scenario, ts: datetime) -> List[dict]:
    """One tick of rows for a client's 10 routers, keyed by the contract's uppercase columns."""
    region = region_for(client_id)
    heat = scenario.heat_level[region]
    label = scenario.label_for(region)
    event_ts = format_event_ts(ts)
    rows = []
    for device in range(1, DEVICES_PER_CLIENT + 1):
        dev = device_id(client_id, device)
        row = {
            "EVENT_ID": f"{run_id}:{client_id}:{dev}:{sequence}",
            "RUN_ID": run_id,
            "CLIENT_ID": client_id,
            "DEVICE_ID": dev,
            "REGION": region,
            "SEQUENCE": sequence,
            "EVENT_TS": event_ts,
            "SCENARIO": label,
        }
        row.update(reading(dev, sequence, rate_hz, heat))
        rows.append(row)
    return rows
