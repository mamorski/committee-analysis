from datetime import datetime, timezone

from committee_sim import run_simulations
from committee_sim.run_simulations import (
    ISRAEL_TZ,
    MIN_SLACK_MIN,
    enough_slack,
    in_window,
    seconds_to_window_end,
    window_end_epoch,
)


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
