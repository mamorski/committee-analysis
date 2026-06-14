import argparse

import pytest

import run_simulations
from run_simulations import Paths


class FakeProc:
    """Minimal stand-in for subprocess.Popen used by the harness.

    poll() returns None ("running") until the process is marked stopped, either
    by calling terminate() or by constructing with stopped=True.
    """

    _next_pid = 1000

    def __init__(self, stopped: bool = False):
        FakeProc._next_pid += 1
        self.pid = FakeProc._next_pid
        self._returncode = 0 if stopped else None
        self.terminate_calls = 0

    def poll(self):
        return self._returncode

    def terminate(self):
        self.terminate_calls += 1
        self._returncode = 0


@pytest.fixture
def base_paths(tmp_path):
    """A fully-populated per-run Paths rooted in tmp_path, with dirs created."""
    logs_dir = tmp_path / "logs"
    configs_dir = tmp_path / "configs"
    bin_dir = tmp_path / "bin"
    for d in (logs_dir, configs_dir, bin_dir):
        d.mkdir(parents=True, exist_ok=True)
    return Paths(
        base_dir=tmp_path,
        bin_dir=bin_dir,
        configs_dir=configs_dir,
        logs_dir=logs_dir,
        pids_file=tmp_path / "node_pids.txt",
        bootstrap_log=logs_dir / "bootstrap.log",
        bootstrap_pid_file=tmp_path / "bootstrap_pid.txt",
        bootstrap_address_file=tmp_path / "bootstrap_address.txt",
    )


@pytest.fixture
def default_args():
    """argparse.Namespace mirroring the fields main() sets, for parse_run tests."""
    return argparse.Namespace(
        default_log_level="info",
        drop_on_send_percent=0.0,
        drop_on_send_probability=0.1,
        graph_discovery_timeout="30s",
        graph_building_round_timeout="2m",
        graph_building_rounds=4,
        node_drop_percent=0.0,
        node_drop_interval_sec=0,
        node_drop_log="",
    )
