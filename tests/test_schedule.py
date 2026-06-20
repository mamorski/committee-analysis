from datetime import datetime, timezone

from committee_sim import run_simulations
from committee_sim.run_simulations import (
    ISRAEL_TZ,
    MIN_SLACK_MIN,
    enough_slack,
    in_window,
    next_window_start_epoch,
    seconds_to_window_end,
    sleep_until,
    window_end_epoch,
)
from committee_sim.telegram_notifier import StopFlags


def israel_epoch(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=ISRAEL_TZ).timestamp()


class TestWindowEndEpoch:
    def test_winter_is_08_local_utc_plus_2(self):
        now = israel_epoch(2026, 1, 15, 2)  # winter, UTC+2
        end = window_end_epoch(now)
        utc = datetime.fromtimestamp(end, timezone.utc)
        assert (utc.hour, utc.minute) == (6, 0)  # 08:00 IST == 06:00 UTC

    def test_summer_is_08_local_utc_plus_3(self):
        now = israel_epoch(2026, 7, 15, 2)  # summer (DST), UTC+3
        end = window_end_epoch(now)
        utc = datetime.fromtimestamp(end, timezone.utc)
        assert (utc.hour, utc.minute) == (5, 0)  # 08:00 IDT == 05:00 UTC

    def test_matches_local_eight_oclock(self):
        now = israel_epoch(2026, 7, 15, 2)
        end_local = datetime.fromtimestamp(window_end_epoch(now), ISRAEL_TZ)
        assert (end_local.hour, end_local.minute) == (8, 0)


class TestInWindow:
    def test_inside(self):
        assert in_window(israel_epoch(2026, 1, 15, 2))

    def test_after(self):
        assert not in_window(israel_epoch(2026, 1, 15, 9))

    def test_late_night(self):
        assert not in_window(israel_epoch(2026, 1, 15, 23))


class TestSlack:
    def test_seconds_to_end_positive_inside(self):
        now = israel_epoch(2026, 1, 15, 2)
        assert seconds_to_window_end(now) == 6 * 3600

    def test_no_slack_near_edge(self):
        now = israel_epoch(2026, 1, 15, 7, 45)  # 15 min before 08:00
        assert not enough_slack(now)
        assert seconds_to_window_end(now) < MIN_SLACK_MIN * 60

    def test_plenty_slack(self):
        assert enough_slack(israel_epoch(2026, 1, 15, 2))


class TestNextWindowStart:
    def test_is_next_day_midnight_local(self):
        # Inside tonight's window (02:00) -> next start is tomorrow 00:00 local.
        now = israel_epoch(2026, 1, 15, 2)
        start_local = datetime.fromtimestamp(next_window_start_epoch(now), ISRAEL_TZ)
        assert (start_local.year, start_local.month, start_local.day) == (2026, 1, 16)
        assert (start_local.hour, start_local.minute) == (0, 0)

    def test_after_window_same_day_points_to_tomorrow(self):
        now = israel_epoch(2026, 1, 15, 9)  # after 08:00, still the 15th
        start_local = datetime.fromtimestamp(next_window_start_epoch(now), ISRAEL_TZ)
        assert start_local.day == 16
        assert (start_local.hour, start_local.minute) == (0, 0)

    def test_strictly_in_future(self):
        now = israel_epoch(2026, 1, 15, 23)
        assert next_window_start_epoch(now) > now

    def test_summer_dst_midnight(self):
        now = israel_epoch(2026, 7, 15, 2)  # DST, UTC+3
        utc = datetime.fromtimestamp(next_window_start_epoch(now), timezone.utc)
        assert (utc.hour, utc.minute) == (21, 0)  # 00:00 IDT == 21:00 UTC prev day


class TestSleepUntil:
    def test_returns_true_when_deadline_reached(self, monkeypatch):
        monkeypatch.setattr(run_simulations.time, "sleep", lambda *_: None)
        flags = StopFlags()
        # Deadline already in the past -> returns immediately True, no stop.
        assert sleep_until(run_simulations.time.time() - 1, flags) is True

    def test_returns_false_when_stopped(self, monkeypatch):
        monkeypatch.setattr(run_simulations.time, "sleep", lambda *_: None)
        flags = StopFlags()
        flags.stop_requested.set()
        # Far-future deadline, but a stop is already set -> early False.
        assert sleep_until(run_simulations.time.time() + 10_000, flags) is False

    def test_stop_set_midway_breaks(self, monkeypatch):
        flags = StopFlags()
        calls = {"n": 0}

        def fake_sleep(_secs):
            calls["n"] += 1
            if calls["n"] >= 2:
                flags.stop_requested.set()

        monkeypatch.setattr(run_simulations.time, "sleep", fake_sleep)
        assert sleep_until(run_simulations.time.time() + 10_000, flags) is False
        assert calls["n"] >= 2
