from __future__ import annotations

import copy

from sliding import add_event, count, evict, slide_batch, window_bounds


def _new_state() -> dict:
    return {"events": {}, "seq": 0}


def test_add_two_events_counts_two() -> None:
    state = _new_state()
    add_event(state, "alpha", 1500)
    add_event(state, "alpha", 1500)

    assert count(state, "alpha", 3000, 2000) == 2


def test_expired_event_is_removed() -> None:
    state = _new_state()
    add_event(state, "alpha", 1000)

    evict(state, "alpha", 2000, 1000)

    assert state["events"].get("alpha", []) == []


def test_count_does_not_modify_state() -> None:
    state = _new_state()
    add_event(state, "alpha", 300)
    add_event(state, "alpha", 1200)
    add_event(state, "alpha", 2500)
    before = copy.deepcopy(state)

    assert count(state, "alpha", 3000, 2000) == 2
    assert state == before


def test_window_bounds_are_left_open_right_closed() -> None:
    state = _new_state()
    add_event(state, "alpha", 4000)

    assert window_bounds(5000, 1000) == (4000, 5000)
    assert count(state, "alpha", 5000, 1000) == 0


def test_batch_queries_are_independent() -> None:
    requests = [
        {"key": "alpha", "events": [1500, 2500], "now": 3000, "window": 2000},
        {"key": "beta", "events": [2600], "now": 3000, "window": 2000},
    ]

    assert slide_batch(requests) == {"alpha": 2, "beta": 1}