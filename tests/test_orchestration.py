import json
import signal
import threading

import pytest

from committee_sim import run_simulations
from committee_sim.run_simulations import (
    Processes,
    RunConfig,
    cleanup,
    drop_nodes,
    read_bootstrap_address,
    run_one_simulation,
    start_bootstrap_server,
    start_node,
)
from committee_sim.telegram_notifier import StopFlags
from conftest import FakeProc


class TestStartNode:
    def test_writes_log_and_pid(self, base_paths, monkeypatch, tmp_path):
        monkeypatch.setattr(run_simulations, "Popen", lambda *a, **k: FakeProc())
        base_paths.pids_file.write_text("")
        cfg_file = tmp_path / "conf.json"
        cfg_file.write_text("{}")
        proc = start_node(base_paths, 7, cfg_file)
        assert (base_paths.logs_dir / "node-7.log").is_file()
        assert str(proc.pid) in base_paths.pids_file.read_text()


class TestStartBootstrapServer:
    def test_writes_pid_file(self, base_paths, monkeypatch):
        monkeypatch.setattr(run_simulations, "Popen", lambda *a, **k: FakeProc())
        monkeypatch.setattr(run_simulations.time, "sleep", lambda *_: None)
        proc = start_bootstrap_server(base_paths)
        assert base_paths.bootstrap_pid_file.read_text().strip() == str(proc.pid)


class TestReadBootstrapAddress:
    def test_returns_stripped_address(self, base_paths):
        base_paths.bootstrap_address_file.write_text("  /ip4/addr\n")
        assert read_bootstrap_address(base_paths) == "/ip4/addr"

    def test_missing_exits(self, base_paths):
        with pytest.raises(SystemExit):
            read_bootstrap_address(base_paths)


class TestDropNodes:
    def _call(self, node_id_map, node_drop_count, log, stop_event=None, deadline=0.0):
        drop_nodes(
            node_id_map=node_id_map,
            stop_event=stop_event or threading.Event(),
            graph_phase_deadline=deadline,
            node_drop_count=node_drop_count,
            node_drop_interval_sec=0,
            node_drop_log=log,
            session_id="sess",
            run_label="run-01",
            num_nodes=len(node_id_map),
            node_drop_percent=50.0,
            logs_dir=log.parent,
        )

    def test_zero_count_noop(self, tmp_path):
        log = tmp_path / "drops.log"
        self._call({1: FakeProc(), 2: FakeProc()}, 0, log)
        assert not log.exists()

    def test_happy_path_terminates_and_logs_summary(self, tmp_path):
        log = tmp_path / "drops.log"
        procs = {i: FakeProc() for i in range(1, 5)}
        self._call(procs, 2, log)
        terminated = sum(p.terminate_calls for p in procs.values())
        assert terminated == 2
        records = [json.loads(l) for l in log.read_text().splitlines()]
        summary = [r for r in records if r.get("summary")]
        assert len(summary) == 1
        assert summary[0]["node_drop_count_actual"] == 2

    def test_early_stop_no_terminate(self, tmp_path):
        log = tmp_path / "drops.log"
        procs = {i: FakeProc() for i in range(1, 5)}
        ev = threading.Event()
        ev.set()
        # Deadline in the future so stop_event.wait returns True immediately.
        self._call(procs, 2, log, stop_event=ev, deadline=9_999_999_999.0)
        assert sum(p.terminate_calls for p in procs.values()) == 0
        records = [json.loads(l) for l in log.read_text().splitlines()]
        assert any(r.get("summary") for r in records)

    def test_already_exited_skipped(self, tmp_path):
        log = tmp_path / "drops.log"
        procs = {1: FakeProc(stopped=True), 2: FakeProc(stopped=True)}
        self._call(procs, 2, log)
        assert sum(p.terminate_calls for p in procs.values()) == 0


class TestCleanup:
    def test_terminates_and_cleans(self, base_paths, monkeypatch):
        running = FakeProc()
        stopped = FakeProc(stopped=True)
        procs = Processes(server_proc=None, node_procs=[running, stopped])
        base_paths.bootstrap_pid_file.write_text("4242")
        (base_paths.logs_dir / "node-1.log").write_text("x")

        kills = []
        monkeypatch.setattr(run_simulations.os, "kill", lambda pid, sig: kills.append((pid, sig)))

        cleanup(base_paths, procs, "sess-1")

        assert running.terminate_calls == 1
        assert stopped.terminate_calls == 0
        assert kills == [(4242, signal.SIGTERM)]
        assert (base_paths.base_dir / "sess-1.tar.gz").is_file()
        assert not base_paths.logs_dir.exists()
        assert not base_paths.configs_dir.exists()


class TestHardKillCleanup:
    def test_force_kills_and_discards_partial_archive(self, base_paths, monkeypatch):
        running = FakeProc()
        procs = Processes(server_proc=None, node_procs=[running])
        base_paths.bootstrap_pid_file.write_text("4242")
        (base_paths.logs_dir / "node-1.log").write_text("x")
        # A partial archive left over from the interrupted run.
        partial = base_paths.base_dir / "sess-1.tar.gz"
        partial.write_text("partial")

        kills = []
        monkeypatch.setattr(run_simulations.os, "kill", lambda pid, sig: kills.append((pid, sig)))

        result = cleanup(base_paths, procs, "sess-1", archive=False, force=True)

        assert result is None
        assert running.kill_calls == 1
        assert running.terminate_calls == 0
        assert kills == [(4242, signal.SIGKILL)]
        assert not partial.exists()                       # partial archive removed
        assert not base_paths.logs_dir.exists()
        # No new archive created on a hard kill.
        assert not (base_paths.base_dir / "sess-1.tar.gz").exists()


class _FakePbar:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def update(self, *a, **k):
        pass

    def set_postfix(self, *a, **k):
        pass


def _smoke_cfg(tmp_path, run_label="run-01"):
    return RunConfig(
        num_nodes=4,
        max_outbound_degree=2,
        diameter=2,
        log_level="info",
        run_label=run_label,
        drop_on_send_percent=0.0,
        drop_on_send_probability=0.0,
        node_drop_percent=0.0,
        node_drop_interval_sec=0,
        node_drop_log=tmp_path / "node_drops.log",
        verify_timeout="5s",
        committee_size=2,
        graph_building_rounds=2,
        graph_discovery_timeout="5s",
        graph_building_round_timeout="5s",
        scenario_name="smoke",
        peer_drop_percent=0.0,
    )


def _smoke_base_paths(tmp_path):
    from committee_sim.run_simulations import Paths

    bin_dir = tmp_path / "bin"
    configs_dir = tmp_path / "configs"
    bin_dir.mkdir()
    configs_dir.mkdir()
    (bin_dir / "server").write_text("")
    (bin_dir / "committee-sampling").write_text("")
    return Paths(
        base_dir=tmp_path,
        bin_dir=bin_dir,
        configs_dir=configs_dir,
        bootstrap_address_file=tmp_path / "bootstrap_address.txt",
    )


def _patch_boundaries(monkeypatch, node_stopped=True):
    monkeypatch.setattr(run_simulations, "start_bootstrap_server", lambda p: FakeProc())
    monkeypatch.setattr(run_simulations, "read_bootstrap_address", lambda p: "/ip4/addr")
    monkeypatch.setattr(run_simulations, "start_node", lambda p, i, c: FakeProc(stopped=node_stopped))
    monkeypatch.setattr(run_simulations.time, "sleep", lambda *_: None)
    monkeypatch.setattr(run_simulations.signal, "signal", lambda *a, **k: None)
    monkeypatch.setattr(run_simulations, "tqdm", lambda *a, **k: _FakePbar())


class TestRunOneSimulation:
    def test_integration_smoke_completes_and_records(self, tmp_path, monkeypatch):
        base_paths = _smoke_base_paths(tmp_path)
        cfg = _smoke_cfg(tmp_path)
        _patch_boundaries(monkeypatch, node_stopped=True)

        result = run_one_simulation(base_paths, cfg)

        # cleanup removes configs dir at the end -> proof the run reached the end.
        assert not base_paths.configs_dir.exists()
        assert result.outcome == "completed"
        assert result.run_label == "run-01"
        assert result.archive and result.archive.endswith(".tar.gz")

    def test_deadline_mid_run_hard_kills(self, tmp_path, monkeypatch):
        base_paths = _smoke_base_paths(tmp_path)
        cfg = _smoke_cfg(tmp_path)
        # Nodes never stop -> loop relies on the deadline to break.
        _patch_boundaries(monkeypatch, node_stopped=False)

        result = run_one_simulation(
            base_paths, cfg, deadline_epoch=run_simulations.time.time()
        )
        assert result.outcome == "killed"
        assert result.archive is None
        assert not base_paths.configs_dir.exists()

    def test_stop_now_mid_run_hard_kills(self, tmp_path, monkeypatch):
        base_paths = _smoke_base_paths(tmp_path)
        cfg = _smoke_cfg(tmp_path)
        _patch_boundaries(monkeypatch, node_stopped=False)
        flags = StopFlags()
        flags.stop_immediate.set()

        result = run_one_simulation(base_paths, cfg, stop_flags=flags)
        assert result.outcome == "killed"
