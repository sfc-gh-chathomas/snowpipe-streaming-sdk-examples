"""Acknowledgement bookkeeping shared by all clients' SDK success/error handlers.

The SDK runs handlers on its shared acknowledgement worker pool, so every handler here does
constant-time dictionary and counter work under one short lock: no I/O, no SDK calls.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable, Dict, Hashable, Optional, Tuple

BACKPRESSURE_CODES = frozenset({
    "ReceiverSaturated",
    "MemoryThresholdExceeded",
    "MemoryThresholdExceededInContainer",
})
CLOSED_CODES = frozenset({
    "ClosedChannelError",
    "ClosedElasticChannelError",
    "ClosedClientError",
    "InvalidChannelError",
    "InvalidClientError",
})


def error_code_name(exc: BaseException) -> Optional[str]:
    """StreamingIngestError.error_code is an enum whose value is the code name."""
    code = getattr(exc, "error_code", None)
    return getattr(code, "value", code) if code is not None else None


class AckLedger:
    """Tracks every append by its small token until the SDK reports it acknowledged or failed."""

    def __init__(self, max_pending_rows: int, max_pending_bytes: int,
                 redact: Callable[[str], str] = lambda s: s,
                 rate_window_s: float = 10.0, clock: Callable[[], float] = time.monotonic):
        self.max_pending_rows = max_pending_rows
        self.max_pending_bytes = max_pending_bytes
        self._redact = redact
        self._window = rate_window_s
        self._clock = clock
        self._lock = threading.Lock()
        self._pending: Dict[Hashable, Tuple[int, int]] = {}
        self._acks: deque = deque()
        self._request_ids: set = set()
        self._started = clock()
        self.append_calls = 0
        self.submitted_rows = 0
        self.acknowledged_rows = 0
        self.pending_rows = 0
        self.pending_bytes = 0
        self.failed_rows = 0
        self.uncertain_rows = 0
        self.errors = 0
        self.sdk_retries = 0
        self.backpressure_events = 0
        self.last_error: Optional[str] = None

    # -- caller side (producer thread) --------------------------------------------------

    def reserve(self, token: Hashable, rows: int, nbytes: int) -> bool:
        """Register an append before handing it to the SDK; False when over the pending bound."""
        with self._lock:
            if (self.pending_rows + rows > self.max_pending_rows
                    or self.pending_bytes + nbytes > self.max_pending_bytes):
                self.backpressure_events += 1
                return False
            self._pending[token] = (rows, nbytes)
            self.pending_rows += rows
            self.pending_bytes += nbytes
            self.append_calls += 1
            self.submitted_rows += rows
            return True

    def release(self, token: Hashable, exc: BaseException) -> None:
        """The append raised synchronously: nothing was enqueued, so undo the reservation."""
        with self._lock:
            entry = self._pending.pop(token, None)
            if entry is not None:
                rows, nbytes = entry
                self.pending_rows -= rows
                self.pending_bytes -= nbytes
                self.append_calls -= 1
                self.submitted_rows -= rows
            if error_code_name(exc) in BACKPRESSURE_CODES:
                self.backpressure_events += 1
            else:
                self.errors += 1
            self.last_error = self._redact(str(exc))[:240]

    def note_error(self, message: str) -> None:
        with self._lock:
            self.errors += 1
            self.last_error = self._redact(message)[:240]

    # -- SDK handler side (acknowledgement worker pool) ---------------------------------

    def _settle(self, tokens) -> int:
        settled = 0
        for token in tokens or ():
            entry = self._pending.pop(token, None)
            if entry is None:
                continue
            rows, nbytes = entry
            self.pending_rows -= rows
            self.pending_bytes -= nbytes
            settled += rows
        return settled

    def _observe_request(self, request_id: Optional[str], retry_count: int) -> None:
        if request_id is not None:
            self._request_ids.add(request_id)
            self.sdk_retries += int(retry_count or 0)

    def on_success(self, detail) -> None:
        with self._lock:
            rows = self._settle(detail.append_tokens)
            self.acknowledged_rows += rows
            self._observe_request(detail.request_id, detail.retry_count)
            now = self._clock()
            self._acks.append((now, rows))
            while self._acks and self._acks[0][0] < now - self._window:
                self._acks.popleft()

    def on_error(self, detail) -> None:
        with self._lock:
            rows = self._settle(detail.append_tokens)
            if detail.request_id is None:
                # SDK-local failure (close/invalidation): the rows may or may not have landed.
                self.uncertain_rows += rows
            else:
                self.failed_rows += rows
            self._observe_request(detail.request_id, detail.retry_count)
            self.errors += 1
            self.last_error = self._redact(str(detail.error))[:240]

    # -- reporting ----------------------------------------------------------------------

    def ack_rate(self) -> float:
        with self._lock:
            now = self._clock()
            while self._acks and self._acks[0][0] < now - self._window:
                self._acks.popleft()
            span = max(1.0, min(self._window, now - self._started))  # no startup spike
            return sum(n for _, n in self._acks) / span

    def counters(self) -> dict:
        with self._lock:
            return {
                "append_calls": self.append_calls,
                "submitted_rows": self.submitted_rows,
                "acknowledged_rows": self.acknowledged_rows,
                "pending_rows": self.pending_rows,
                "pending_bytes": self.pending_bytes,
                "failed_rows": self.failed_rows,
                "uncertain_rows": self.uncertain_rows,
                "errors": self.errors,
                "sdk_observed_request_ids": len(self._request_ids),
                "sdk_retries": self.sdk_retries,
                "backpressure_events": self.backpressure_events,
                "last_error": self.last_error,
            }
