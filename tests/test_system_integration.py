"""End-to-end exercise of main() over the real filesystem + real state log.

Only the process/network/clock boundaries are mocked (same style as the
run_one_simulation smoke test): bootstrap + node launches return FakeProc, sleeps
are no-ops, disk is plentiful, and Telegram is a recording fake. base_dir is
redirected to tmp_path by patching run_simulations.__file__ so archives, logs and
the state log all land in the temp dir.
"""

import json

import pytest

from committee_sim import run_simulations
from conftest import FakeProc
from committee_sim.telegram_notifier import StopFlags


class _FakePbar:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def update(self, *a, **k):
        pass

    def set_postfix(self, *a, **k):
        pass


class FakeNotifier:
    instances = []

    def __init__(self, stop_flags, base_dir="."):
        self.stop_flags = stop_flags
        self.base_dir = base_dir
        self.messages = []
        self.started = False
        self.stopped = False
        FakeNotifier.instances.append(self)

    def start(self):
        self.started = True

    def notify(self, text):
        self.messages.append(text)

    def stop(self):
        self.stopped = True


def _write_batch(tmp_path):
    batch = {
        "runs": [
            {"name": "r1", "num_nodes": 4, "max_degree": 2, "diameter": 2, "committee_size": 2},
            {"name": "r2", "num_nodes": 6, "max_degree": 2, "diameter": 2, "committee_size": 2},
        ]
    }
    p = tmp_path / "batch.json"
    p.write_text(json.dumps(batch))
    return p


def _run_main(
    tmp_path,
    monkeypatch,
    extra_argv=None,
    node_stopped=True,
    free=50 * 1024 ** 3,
):
    """Drive main() to completion (it always sys.exit()s). Returns the FakeNotifier."""
    FakeNotifier.instances = []
    batch = _write_batch(tmp_path)
    state_log = tmp_path / "run_state.log"

    # Redirect the project root into tmp_path; outputs land in tmp_path/results.
    # Provide the binaries run_one_simulation checks for under tmp_path/bin.
    monkeypatch.setenv("COMMITTEE_SIM_ROOT", str(tmp_path))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "server").write_text("")
    (bin_dir / "committee-sampling").write_text("")

    argv = ["run_simulations.py", str(batch), "--state-log", str(state_log)]
    argv += extra_argv or []
    monkeypatch.setattr("sys.argv", argv)

    # Boundaries.
    monkeypatch.setattr(run_simulations, "start_bootstrap_server", lambda p: FakeProc())
    monkeypatch.setattr(run_simulations, "read_bootstrap_address", lambda p: "/ip4/addr")
    monkeypatch.setattr(run_simulations, "start_node", lambda p, i, c: FakeProc(stopped=node_stopped))
    monkeypatch.setattr(run_simulations.time, "sleep", lambda *_: None)
    monkeypatch.setattr(run_simulations.signal, "signal", lambda *a, **k: None)
    monkeypatch.setattr(run_simulations, "tqdm", lambda *a, **k: _FakePbar())
    monkeypatch.setattr(run_simulations, "free_bytes", lambda _p: free)
    monkeypatch.setattr(run_simulations, "Notifier", FakeNotifier)

    with pytest.raises(SystemExit) as exc:
        run_simulations.main()
    # Runtime outputs (archives, logs, generated configs) live under results/.
    results_dir = tmp_path / "results"
    return FakeNotifier.instances[-1], state_log, results_dir, exc.value.code


class TestHappyPath:
    def test_runs_all_and_records(self, tmp_path, monkeypatch):
        notifier, state_log, base, code = _run_main(
            tmp_path, monkeypatch, extra_argv=["--ignore-window"]
        )
        assert code == 0
        labels = run_simulations.load_finished_labels(state_log)
        assert labels == {"run-01-4n-2m-d2", "run-02-6n-2m-d2"}
        assert len(list(base.glob("*.tar.gz"))) == 2
        assert not (base / "_generated_configs").exists()
        # Per-run log dirs are archived + removed (the logs/ parent may remain empty).
        assert list((base / "logs").glob("run-*")) == []
        starts = [m for m in notifier.messages if m.startswith("🚀")]
        summaries = [m for m in notifier.messages if m.startswith("✅ run-")]
        assert len(starts) == 2
        assert len(summaries) == 2
        assert notifier.stopped


class TestResume:
    def test_skips_already_finished(self, tmp_path, monkeypatch):
        state_log = tmp_path / "run_state.log"
        run_simulations.record_finished(state_log, "run-01-4n-2m-d2", "sess-old", "old.tar.gz")

        notifier, state_log, base, code = _run_main(
            tmp_path, monkeypatch, extra_argv=["--ignore-window"]
        )
        # Only run-02 should have produced a new archive.
        assert len(list(base.glob("*.tar.gz"))) == 1
        starts = [m for m in notifier.messages if m.startswith("🚀")]
        assert len(starts) == 1
        assert "run-02-6n-2m-d2" in starts[0]
        assert run_simulations.load_finished_labels(state_log) == {
            "run-01-4n-2m-d2",
            "run-02-6n-2m-d2",
        }


class TestWindowClosed:
    def test_exits_without_running(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_simulations, "in_window", lambda *a: False)
        notifier, state_log, base, code = _run_main(tmp_path, monkeypatch)
        assert code == 0
        assert list(base.glob("*.tar.gz")) == []
        assert not state_log.exists() or run_simulations.load_finished_labels(state_log) == set()
        assert not any(m.startswith("🚀") for m in notifier.messages)


class TestInsufficientSlack:
    def test_exits_before_first_sim(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_simulations, "in_window", lambda *a: True)
        monkeypatch.setattr(run_simulations, "enough_slack", lambda *a: False)
        notifier, state_log, base, code = _run_main(tmp_path, monkeypatch)
        assert list(base.glob("*.tar.gz")) == []
        assert run_simulations.load_finished_labels(state_log) == set()
        assert not any(m.startswith("🚀") for m in notifier.messages)


class TestDeadlineDuringRun:
    def test_first_sim_hard_killed_not_recorded(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_simulations, "in_window", lambda *a: True)
        monkeypatch.setattr(run_simulations, "enough_slack", lambda *a: True)
        # Deadline already passed -> sim is hard-killed on the first poll.
        monkeypatch.setattr(run_simulations, "window_end_epoch", lambda *a: run_simulations.time.time())
        notifier, state_log, base, code = _run_main(
            tmp_path, monkeypatch, node_stopped=False
        )
        assert run_simulations.load_finished_labels(state_log) == set()
        assert list(base.glob("*.tar.gz")) == []
        assert any("killed" in m for m in notifier.messages)


class TestGracefulStop:
    def test_stop_after_first_sim(self, tmp_path, monkeypatch):
        real = run_simulations.run_one_simulation

        def wrapper(base_paths, cfg, **kwargs):
            result = real(base_paths, cfg, **kwargs)
            # Simulate a /stop arriving during the first sim.
            FakeNotifier.instances[-1].stop_flags.stop_requested.set()
            return result

        monkeypatch.setattr(run_simulations, "run_one_simulation", wrapper)
        notifier, state_log, base, code = _run_main(
            tmp_path, monkeypatch, extra_argv=["--ignore-window"]
        )
        # First sim recorded, second skipped due to graceful stop.
        assert run_simulations.load_finished_labels(state_log) == {"run-01-4n-2m-d2"}
        assert len(list(base.glob("*.tar.gz"))) == 1
