import pytest

from committee_sim.run_simulations import _expand_runs, _expand_sweep, parse_run


class TestExpandSweep:
    def test_basic_expansion(self):
        sweep = {
            "num_nodes": {"from": 10, "to": 30, "step": 10},
            "max_outbound_degree": 3,
            "diameter": 2,
        }
        runs = _expand_sweep(sweep)
        assert [r["number_of_nodes"] for r in runs] == [10, 20, 30]
        assert all(r["max_outbound_degree"] == 3 for r in runs)
        assert all(r["diameter"] == 2 for r in runs)

    def test_repetitions(self):
        sweep = {
            "num_nodes": {"from": 10, "to": 10, "step": 10},
            "max_outbound_degree": 3,
            "diameter": 2,
            "repetitions": 3,
        }
        runs = _expand_sweep(sweep)
        assert len(runs) == 3
        names = [r["name"] for r in runs]
        assert names == ["sweep-10n-rep-01", "sweep-10n-rep-02", "sweep-10n-rep-03"]

    @pytest.mark.parametrize(
        "sweep",
        [
            {"num_nodes": {"from": 1}, "max_outbound_degree": 3, "diameter": 2},
            {"num_nodes": {"from": 1, "to": 5, "step": 0}, "max_outbound_degree": 3, "diameter": 2},
            {"num_nodes": {"from": 1, "to": 5, "step": 1}, "max_outbound_degree": 3, "diameter": 2, "repetitions": 0},
            {"num_nodes": {"from": 1, "to": 5, "step": 1}, "diameter": 2},
            {"num_nodes": {"from": 1, "to": 5, "step": 1}, "max_outbound_degree": 3},
        ],
    )
    def test_invalid_exits(self, sweep):
        with pytest.raises(SystemExit):
            _expand_sweep(sweep)


class TestExpandRuns:
    def test_inline_runs_passthrough(self):
        cfg = {"runs": [{"name": "a", "number_of_nodes": 5, "max_outbound_degree": 3, "diameter": 2}]}
        runs = _expand_runs(cfg)
        assert len(runs) == 1
        assert runs[0]["name"] == "a"

    def test_repetitions_fan_out(self):
        cfg = {
            "runs": [
                {"name": "base", "number_of_nodes": 5, "max_outbound_degree": 3, "diameter": 2, "repetitions": 2}
            ]
        }
        runs = _expand_runs(cfg)
        assert [r["name"] for r in runs] == ["base-rep-01", "base-rep-02"]
        assert all("repetitions" not in r for r in runs)

    def test_routes_to_sweep(self):
        cfg = {
            "sweep": {
                "num_nodes": {"from": 10, "to": 20, "step": 10},
                "max_outbound_degree": 3,
                "diameter": 2,
            }
        }
        runs = _expand_runs(cfg)
        assert [r["number_of_nodes"] for r in runs] == [10, 20]

    def test_sweep_and_runs_combined(self):
        # Both blocks present: sweep entries first, then runs entries (fanned out).
        cfg = {
            "sweep": {
                "num_nodes": {"from": 10, "to": 20, "step": 10},
                "max_outbound_degree": 3,
                "diameter": 2,
            },
            "runs": [
                {"name": "drop", "number_of_nodes": 30, "max_outbound_degree": 3,
                 "diameter": 2, "node_drop_percent": 5, "repetitions": 2},
            ],
        }
        runs = _expand_runs(cfg)
        names = [r["name"] for r in runs]
        # 2 sweep points + 2 fanned-out drop reps = 4 total.
        assert len(runs) == 4
        assert names[:2] == ["sweep-10n-rep-01", "sweep-20n-rep-01"]
        assert names[2:] == ["drop-rep-01", "drop-rep-02"]
        assert all("repetitions" not in r for r in runs)

    def test_runs_not_a_list_exits(self):
        with pytest.raises(SystemExit):
            _expand_runs({"runs": "notalist"})

    @pytest.mark.parametrize("cfg", [{}, {"runs": []}, {"runs": "notalist"}])
    def test_missing_runs_exits(self, cfg):
        with pytest.raises(SystemExit):
            _expand_runs(cfg)

    def test_reps_less_than_one_exits(self):
        cfg = {"runs": [{"name": "a", "number_of_nodes": 5, "max_outbound_degree": 3, "diameter": 2, "repetitions": 0}]}
        with pytest.raises(SystemExit):
            _expand_runs(cfg)


class TestParseRun:
    def test_canonical_fields(self, default_args, tmp_path):
        run = {"name": "s", "number_of_nodes": 12, "max_outbound_degree": 4, "diameter": 3}
        cfg = parse_run(run, default_args, 1, tmp_path)
        assert cfg.num_nodes == 12
        assert cfg.max_outbound_degree == 4
        assert cfg.diameter == 3
        assert cfg.scenario_name == "s"
        assert cfg.run_label == "run-01-12n-4m-d3"

    def test_field_aliases(self, default_args, tmp_path):
        run = {
            "num_nodes": 8,
            "max_degree": 2,
            "diameter": 2,
            "building_rounds": 6,
            "building_graph_timeout": "3m",
        }
        cfg = parse_run(run, default_args, 2, tmp_path)
        assert cfg.num_nodes == 8
        assert cfg.max_outbound_degree == 2
        assert cfg.graph_building_rounds == 6
        assert cfg.graph_building_round_timeout == "3m"

    def test_cli_defaults_fallback(self, default_args, tmp_path):
        run = {"number_of_nodes": 5, "max_outbound_degree": 3, "diameter": 2}
        cfg = parse_run(run, default_args, 3, tmp_path)
        assert cfg.log_level == "info"
        assert cfg.graph_building_rounds == 4
        assert cfg.graph_discovery_timeout == "30s"
        assert cfg.graph_building_round_timeout == "2m"

    def test_empty_timeout_falls_through(self, default_args, tmp_path):
        # Empty string is falsy -> falls back to CLI default (truthiness quirk).
        run = {
            "number_of_nodes": 5,
            "max_outbound_degree": 3,
            "diameter": 2,
            "graph_discovery_timeout": "",
        }
        cfg = parse_run(run, default_args, 4, tmp_path)
        assert cfg.graph_discovery_timeout == "30s"

    def test_zero_building_rounds_preserved(self, default_args, tmp_path):
        # 0 is falsy but the code uses `is None`, so a configured 0 is kept.
        run = {
            "number_of_nodes": 5,
            "max_outbound_degree": 3,
            "diameter": 2,
            "graph_building_rounds": 0,
        }
        cfg = parse_run(run, default_args, 5, tmp_path)
        assert cfg.graph_building_rounds == 0

    @pytest.mark.parametrize(
        "run",
        [
            {"max_outbound_degree": 3, "diameter": 2},
            {"number_of_nodes": 5, "diameter": 2},
            {"number_of_nodes": 5, "max_outbound_degree": 3},
        ],
    )
    def test_missing_required_exits(self, run, default_args, tmp_path):
        with pytest.raises(SystemExit):
            parse_run(run, default_args, 6, tmp_path)
