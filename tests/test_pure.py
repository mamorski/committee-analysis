import resource

import pytest

from committee_sim import run_simulations
from committee_sim.run_simulations import (
    _ensure_duration,
    _swallow,
    parse_duration,
    percent_to_count,
    raise_fd_limit,
    validate_args,
)


class TestRaiseFdLimit:
    def test_raises_soft_to_target_within_hard(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(resource, "getrlimit", lambda _r: (1024, 1048576))
        monkeypatch.setattr(resource, "setrlimit", lambda _r, v: captured.update(v=v))
        soft, ok = raise_fd_limit(65536)
        assert soft == 65536 and ok is True
        assert captured["v"] == (65536, 1048576)

    def test_caps_at_hard_when_target_exceeds_it(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(resource, "getrlimit", lambda _r: (1024, 4096))
        monkeypatch.setattr(resource, "setrlimit", lambda _r, v: captured.update(v=v))
        soft, ok = raise_fd_limit(65536)
        assert soft == 4096 and ok is False  # shortfall reported
        assert captured["v"] == (4096, 4096)

    def test_no_setrlimit_when_already_high(self, monkeypatch):
        called = {"n": 0}
        monkeypatch.setattr(resource, "getrlimit", lambda _r: (65536, 1048576))
        monkeypatch.setattr(resource, "setrlimit", lambda *_a: called.__setitem__("n", called["n"] + 1))
        soft, ok = raise_fd_limit(65536)
        assert soft == 65536 and ok is True
        assert called["n"] == 0  # soft already met -> no syscall

    def test_infinite_hard_allows_target(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(resource, "getrlimit", lambda _r: (1024, resource.RLIM_INFINITY))
        monkeypatch.setattr(resource, "setrlimit", lambda _r, v: captured.update(v=v))
        soft, ok = raise_fd_limit(65536)
        assert soft == 65536 and ok is True
        assert captured["v"] == (65536, resource.RLIM_INFINITY)


class TestParseDuration:
    def test_seconds(self):
        assert parse_duration("30s") == 30

    def test_compound_minutes_seconds(self):
        assert parse_duration("2m30s") == 150

    def test_compound_hours_minutes(self):
        assert parse_duration("1h5m") == 3900

    def test_bare_integer(self):
        assert parse_duration("45") == 45

    def test_whitespace_stripped(self):
        assert parse_duration("  10s ") == 10

    def test_milliseconds_rejected(self):
        with pytest.raises(ValueError):
            parse_duration("500ms")


class TestPercentToCount:
    def test_zero_percent(self):
        assert percent_to_count(100, 0) == 0

    def test_negative_percent(self):
        assert percent_to_count(100, -5) == 0

    def test_zero_total(self):
        assert percent_to_count(0, 50) == 0

    def test_rounds_up_and_floors_at_one(self):
        assert percent_to_count(100, 0.5) == 1

    def test_ceil(self):
        assert percent_to_count(200, 10) == 20

    def test_ceil_non_integer(self):
        # 33% of 10 = 3.3 -> ceil -> 4
        assert percent_to_count(10, 33) == 4


class TestEnsureDuration:
    def test_none(self):
        assert _ensure_duration(None) == "0s"

    def test_empty(self):
        assert _ensure_duration("") == "0s"

    def test_passthrough(self):
        assert _ensure_duration("5s") == "5s"


class TestSwallow:
    def test_swallows_exception(self):
        def boom():
            raise RuntimeError("nope")

        # Must not raise.
        _swallow(boom)

    def test_runs_non_raising(self):
        calls = []
        _swallow(calls.append, 42)
        assert calls == [42]


class TestValidateArgs:
    def _valid(self, **overrides):
        kwargs = dict(
            num_nodes=10,
            max_outbound_degree=3,
            diameter=2,
            log_level="info",
            drop_on_send_percent=0.0,
            drop_on_send_probability=0.1,
            node_drop_percent=0.0,
            peer_drop_percent=0.0,
        )
        kwargs.update(overrides)
        return kwargs

    def test_valid_returns_none(self):
        assert validate_args(**self._valid()) is None

    @pytest.mark.parametrize(
        "overrides",
        [
            {"num_nodes": 1},
            {"max_outbound_degree": 0},
            {"diameter": 1},
            {"log_level": "trace"},
            {"drop_on_send_percent": 150},
            {"drop_on_send_percent": -1},
            {"drop_on_send_probability": 1.5},
            {"drop_on_send_probability": -0.1},
            {"node_drop_percent": 101},
            {"peer_drop_percent": 200},
        ],
    )
    def test_invalid_exits(self, overrides):
        with pytest.raises(SystemExit):
            validate_args(**self._valid(**overrides))
