from types import SimpleNamespace

from app.carlo_gavazzi.em540_master import DailyExtrema, _local_day_start


def _make_data(power=0.0, current_sys=0.0, vln_sys=0.0, vll_sys=0.0, phases=None):
    """Build a minimal MeterData-like object for the extrema tracker.

    ``phases`` is a list of dicts with keys power/current/line_neutral_voltage/
    line_line_voltage; defaults to three zeroed phases.
    """
    if phases is None:
        phases = [{} for _ in range(3)]

    def _phase(p):
        return SimpleNamespace(
            power=p.get("power", 0.0),
            current=p.get("current", 0.0),
            line_neutral_voltage=p.get("line_neutral_voltage", 0.0),
            line_line_voltage=p.get("line_line_voltage", 0.0),
        )

    system = SimpleNamespace(
        power=power,
        An=current_sys,
        line_neutral_voltage=vln_sys,
        line_line_voltage=vll_sys,
    )
    return SimpleNamespace(system=system, phases=[_phase(p) for p in phases])


# A fixed wall-clock inside a single local day; +10s stays same day.
_T0 = _local_day_start(1_700_000_000.0) + 3600.0


def test_extrema_start_unset_and_seed_from_first_frame():
    tracker = DailyExtrema()
    tracker.update(_make_data(power=1234.0), _T0)

    snap = tracker.snapshot()
    # First frame seeds both min and max to the same observed value.
    assert snap["power_min"] == 1234.0
    assert snap["power_max"] == 1234.0
    # Untouched quantities from a zeroed frame are 0.0 (they were observed as 0).
    assert snap["current_min"] == 0.0


def test_extrema_track_min_and_max_across_frames_including_negatives():
    tracker = DailyExtrema()
    tracker.update(_make_data(power=100.0), _T0)
    tracker.update(_make_data(power=-500.0), _T0 + 1)
    tracker.update(_make_data(power=800.0), _T0 + 2)

    snap = tracker.snapshot()
    assert snap["power_min"] == -500.0
    assert snap["power_max"] == 800.0


def test_per_phase_and_per_quantity_scopes_are_tracked():
    tracker = DailyExtrema()
    tracker.update(
        _make_data(
            current_sys=6.0,
            vln_sys=230.0,
            vll_sys=400.0,
            phases=[
                {"power": 10.0, "current": 1.0, "line_neutral_voltage": 229.0, "line_line_voltage": 399.0},
                {"power": 20.0, "current": 2.0, "line_neutral_voltage": 230.0, "line_line_voltage": 400.0},
                {"power": 30.0, "current": 3.0, "line_neutral_voltage": 231.0, "line_line_voltage": 401.0},
            ],
        ),
        _T0,
    )

    snap = tracker.snapshot()
    assert snap["current_max"] == 6.0
    assert snap["voltage_ln_max"] == 230.0
    assert snap["voltage_ll_max"] == 400.0
    assert snap["power_l3_max"] == 30.0
    assert snap["current_l1_min"] == 1.0
    assert snap["voltage_ln_l3_max"] == 231.0
    assert snap["voltage_ll_l1_min"] == 399.0


def test_snapshot_exposes_all_32_keys():
    tracker = DailyExtrema()
    snap = tracker.snapshot()
    assert len(snap) == 32
    # Unset before any frame.
    assert all(v is None for v in snap.values())


def test_daily_rollover_resets_and_reseeds():
    tracker = DailyExtrema()
    tracker.update(_make_data(power=1000.0), _T0)
    tracker.update(_make_data(power=-200.0), _T0 + 1)
    assert tracker.snapshot()["power_min"] == -200.0

    # Cross into the next local day.
    next_day = _T0 + 86400.0
    tracker.update(_make_data(power=50.0), next_day)

    snap = tracker.snapshot()
    # Re-seeded from the first post-rollover frame, not carried over.
    assert snap["power_min"] == 50.0
    assert snap["power_max"] == 50.0


def test_backwards_clock_jump_reanchors_window():
    tracker = DailyExtrema()
    tracker.update(_make_data(power=1000.0), _T0)

    # A backwards jump before the current day's start re-anchors and re-seeds.
    earlier = _T0 - 86400.0
    tracker.update(_make_data(power=7.0), earlier)

    snap = tracker.snapshot()
    assert snap["power_min"] == 7.0
    assert snap["power_max"] == 7.0
