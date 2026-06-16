import json
import tarfile

import pytest

from committee_sim.run_simulations import (
    append_node_drop_log,
    backup_logs,
    ensure_files,
    mkdirs,
    write_committee_config,
)


class TestWriteCommitteeConfig:
    def _write(self, base_paths, **overrides):
        kwargs = dict(
            paths=base_paths,
            session_id="sess-1",
            max_outbound_degree=4,
            diameter=3,
            graph_building_rounds=4,
            num_nodes=20,
            bootstrap_address="/ip4/1.2.3.4/tcp/4001/p2p/Qm",
            log_level="info",
            verify_timeout="30s",
            graph_discovery_timeout="30s",
            graph_building_round_timeout="2m",
            start_time=1000,
        )
        kwargs.update(overrides)
        path = write_committee_config(**kwargs)
        return path, json.loads(path.read_text())

    def test_nested_structure(self, base_paths):
        _, cfg = self._write(
            base_paths,
            committee_size=15,
            drop_on_send_enabled=True,
            drop_on_send_probability=0.2,
            peer_drop_enabled=True,
        )
        assert cfg["network"]["max_outbound_degree"] == 4
        assert cfg["network"]["drop_on_send"] is True
        assert cfg["network"]["drop_on_send_probability"] == 0.2
        assert cfg["network"]["peer_drop_enabled"] is True
        assert cfg["committee"]["committee_size"] == 15
        assert cfg["committee"]["total_weight"] == 20
        assert cfg["synchronization"]["start_time"] == 1000

    def test_ensure_duration_applied(self, base_paths):
        _, cfg = self._write(
            base_paths,
            graph_discovery_timeout="",
            graph_building_round_timeout=None,
        )
        assert cfg["synchronization"]["graph_discovery_timeout"] == "0s"
        assert cfg["synchronization"]["graph_building_round_timeout"] == "0s"

    def test_config_file_path_override(self, base_paths, tmp_path):
        target = tmp_path / "custom.json"
        path, _ = self._write(base_paths, config_file_path=target)
        assert path == target
        assert target.is_file()

    def test_default_path(self, base_paths):
        path, _ = self._write(base_paths)
        assert path == base_paths.configs_dir / "committee-sampling-conf.json"


class TestAppendNodeDropLog:
    def test_appends_one_json_per_line(self, tmp_path):
        log = tmp_path / "sub" / "drops.log"
        append_node_drop_log(log, {"a": 1})
        append_node_drop_log(log, {"b": 2})
        lines = log.read_text().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"a": 1}
        assert json.loads(lines[1]) == {"b": 2}

    def test_creates_parent_dir(self, tmp_path):
        log = tmp_path / "deep" / "nested" / "drops.log"
        append_node_drop_log(log, {"x": 1})
        assert log.is_file()


class TestBackupLogs:
    def test_archives_only_node_logs(self, base_paths):
        (base_paths.logs_dir / "node-1.log").write_text("a")
        (base_paths.logs_dir / "node-2.log").write_text("b")
        (base_paths.logs_dir / "bootstrap.log").write_text("boot")
        archive = backup_logs(base_paths, "sess-1")
        assert archive.is_file()
        with tarfile.open(archive) as tar:
            names = sorted(tar.getnames())
        assert names == ["sess-1/node-1.log", "sess-1/node-2.log"]

    def test_missing_logs_dir_returns_path(self, base_paths):
        import shutil

        shutil.rmtree(base_paths.logs_dir)
        archive = backup_logs(base_paths, "sess-1")
        assert archive == base_paths.base_dir / "sess-1.tar.gz"
        assert not archive.exists()


class TestMkdirs:
    def test_creates_dirs_and_clears_pids(self, base_paths):
        base_paths.pids_file.write_text("stale\n")
        mkdirs(base_paths)
        assert base_paths.logs_dir.is_dir()
        assert base_paths.configs_dir.is_dir()
        assert base_paths.pids_file.read_text() == ""


class TestEnsureFiles:
    def test_missing_binaries_exits(self, base_paths):
        with pytest.raises(SystemExit):
            ensure_files(base_paths)

    def test_creates_default_server_config(self, base_paths):
        (base_paths.bin_dir / "server").write_text("")
        (base_paths.bin_dir / "committee-sampling").write_text("")
        ensure_files(base_paths)
        dev_cfg = base_paths.configs_dir / "dev-server.json"
        assert dev_cfg.is_file()
        data = json.loads(dev_cfg.read_text())
        assert data["network"]["listen_port"] == 4001
