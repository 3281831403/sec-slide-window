"""Sliding-window request counting helpers."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

MAX_TIMESTAMP = 253_402_300_799_999
MIN_TIMESTAMP = 0


def _new_state() -> dict[str, Any]:
    return {"events": {}, "seq": 0}


def _require_state(state: Any) -> None:
    if not isinstance(state, dict):
        raise TypeError("state must be a dict")
    events = state.get("events", {})
    if not isinstance(events, dict):
        raise TypeError("state events must be a dict")


def _require_key(key: Any) -> None:
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")


def _require_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _require_now(now: Any) -> int:
    now = _require_int(now, "now")
    if now < MIN_TIMESTAMP:
        raise ValueError("now must be non-negative")
    return now


def _require_window(window: Any) -> int:
    window = _require_int(window, "window")
    if window <= 0:
        raise ValueError("window must be positive")
    return window


def _require_add_timestamp(ts: Any) -> int:
    ts = _require_int(ts, "timestamp")
    if ts < MIN_TIMESTAMP or ts > MAX_TIMESTAMP:
        raise ValueError("timestamp is out of range")
    return ts


def _event_ts(event: Any) -> int:
    if isinstance(event, dict):
        if "ts" not in event:
            raise ValueError("event is missing a timestamp")
        return _require_int(event["ts"], "event timestamp")
    if isinstance(event, (tuple, list)):
        if not event:
            raise ValueError("event is empty")
        return _require_int(event[0], "event timestamp")
    return _require_int(event, "event timestamp")


def _copy_event(event: Any) -> Any:
    if isinstance(event, dict):
        return dict(event)
    if isinstance(event, list):
        return list(event)
    if isinstance(event, tuple):
        return tuple(event)
    return event


def _event_identity(event: Any) -> Any:
    if isinstance(event, dict):
        return tuple(sorted((str(name), repr(value)) for name, value in event.items()))
    if isinstance(event, (list, tuple)):
        return tuple(repr(value) for value in event)
    return repr(event)


def add_event(state: dict[str, Any], key: str, ts: int) -> dict[str, Any]:
    """Add one event timestamp under key and return the same state."""
    _require_state(state)
    _require_key(key)
    _require_add_timestamp(ts)

    events = state.setdefault("events", {})
    bucket = events.setdefault(key, [])

    seq = state.get("seq", 0)
    if isinstance(seq, bool) or not isinstance(seq, int):
        seq = 0
    bucket.append({"ts": ts, "seq": seq})
    state["seq"] = seq + 1
    return state


def evict(state: dict[str, Any], key: str, now: int, window: int) -> dict[str, Any]:
    """Remove events that have fallen out of the current window."""
    _require_state(state)
    _require_key(key)
    now = _require_now(now)
    window = _require_window(window)

    lower = now - window
    events = state.get("events", {})
    bucket = events.get(key, [])
    if not bucket:
        return state
    events[key] = [event for event in bucket if _event_ts(event) > lower]
    return state


def count(state: dict[str, Any], key: str, now: int, window: int) -> int:
    """Return the number of events for key in the current window."""
    _require_state(state)
    _require_key(key)
    now = _require_now(now)
    window = _require_window(window)

    lower = now - window
    upper = now
    bucket = state.get("events", {}).get(key, [])
    return sum(1 for event in bucket if lower < _event_ts(event) <= upper)


def window_bounds(now: int, window: int) -> tuple[int, int]:
    """Return the left-open, right-closed interval represented by now and window."""
    now = _require_now(now)
    window = _require_window(window)
    return now - window, now


def merge_states(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Merge two states, keeping events whose timestamp and sequence both match."""
    _require_state(a)
    _require_state(b)

    merged = _new_state()
    keys = list(dict.fromkeys([*a.get("events", {}).keys(), *b.get("events", {}).keys()]))
    for key in keys:
        seen: set[Any] = set()
        bucket: list[Any] = []
        for source in (a, b):
            for event in source.get("events", {}).get(key, []):
                stamp = _event_ts(event)
                marker = (stamp, _event_identity(event))
                if marker in seen:
                    continue
                seen.add(marker)
                bucket.append(_copy_event(event))
        bucket.sort(key=_event_ts)
        merged["events"][key] = bucket

    maximum_seq = 0
    for source in (a, b):
        seq = source.get("seq", 0)
        if isinstance(seq, int) and not isinstance(seq, bool):
            maximum_seq = max(maximum_seq, seq)
    merged["seq"] = maximum_seq
    return merged


def validate_state(state: dict[str, Any]) -> list[str]:
    """Return a list of problems found in state."""
    _require_state(state)

    problems: list[str] = []
    for key, bucket in state.get("events", {}).items():
        seen: set[Any] = set()
        for event in bucket:
            try:
                stamp = _event_ts(event)
            except (TypeError, ValueError):
                problems.append(f"{key}: invalid event")
                continue

            if stamp < MIN_TIMESTAMP or stamp > MAX_TIMESTAMP:
                problems.append(f"{key}: timestamp {stamp} is out of range")

            if isinstance(event, dict) and "seq" in event:
                marker = (stamp, event.get("seq"))
                if marker in seen:
                    problems.append(f"{key}: duplicate event at {stamp}")
                seen.add(marker)
    return problems


def _percentile_value(sorted_values: list[int], p: float) -> float:
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (len(sorted_values) - 1) * p
    lower_index = int(math.floor(rank))
    upper_index = int(math.ceil(rank))
    if lower_index == upper_index:
        return float(sorted_values[lower_index])
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return float(lower_value + (upper_value - lower_value) * (rank - lower_index))


def percentile(
    state: dict[str, Any], key: str, now: int, window: int, p: float
) -> float:
    """Return the p-th percentile of event timestamps in the current window."""
    _require_state(state)
    _require_key(key)
    now = _require_now(now)
    window = _require_window(window)
    if isinstance(p, bool) or not isinstance(p, (int, float)):
        raise TypeError("p must be a number")
    p = float(p)
    if p < 0.0 or p > 1.0:
        raise ValueError("p must be between zero and one")

    lower = now - window
    upper = now
    bucket = state.get("events", {}).get(key, [])
    values = [
        _event_ts(event)
        for event in bucket
        if lower < _event_ts(event) <= upper
    ]
    if not values:
        return 0.0
    values.sort()
    return _percentile_value(values, p)


def slide_batch(requests: list[Mapping[str, Any]]) -> dict[str, int]:
    """Run several independent window queries and return counts by key."""
    counts: dict[str, int] = {}

    for request in requests:
        if not isinstance(request, Mapping):
            raise TypeError("each request must be a mapping")
        key = request["key"]
        now = request["now"]
        window = request["window"]
        _require_key(key)
        _require_now(now)
        _require_window(window)

        state = _new_state()
        for ts in request.get("events", []):
            _require_add_timestamp(ts)
            add_event(state, key, ts)

        counts[key] = count(state, key, now, window)
    return counts