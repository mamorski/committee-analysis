import argparse
import json
import math
import os
import random
import re
import signal
import sys
import tarfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import rmtree
from subprocess import Popen, STDOUT
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm


@dataclass
class Paths:
    base_dir: Path
    bin_dir: Path
    logs_dir: Path
    configs_dir: Path
    pids_file: Path
    bootstrap_log: Path
    bootstrap_pid_file: Path
    bootstrap_address_file: Path


@dataclass
class Processes:
    server_proc: Optional[Popen]
    node_procs: List[Popen]


def percent_to_count(total: int, percent: float) -> int:
    if percent <= 0 or total <= 0:
        return 0
    return max(1, math.ceil(total * (percent / 100.0)))


PROTOCOL_START_OFFSET_SEC = 60


def _ensure_duration(value: Optional[str]) -> str:
    if not value:
        return "0s"
    return str(value)


def parse_duration(duration: str) -> int:
    """Parse a Go duration string into seconds. Handles compound forms like '2m30s', '1h5m'.

    Note: 'ms' is matched before 'm' so '500ms' is not misread as 500 minutes.
    """
    duration = duration.strip()
    matches = re.findall(r'(\d+)(ms|h|m|s)', duration)
    if not matches:
        return int(duration)
    total = 0.0
    for value, unit in matches:
        if unit == 'h':
            total += int(value) * 3600
        elif unit == 'm':
            total += int(value) * 60
        elif unit == 's':
            total += int(value)
        elif unit == 'ms':
            total += int(value) / 1000.0
    return int(total)


def append_node_drop_log(node_drop_log: Path, record: dict) -> None:
    with node_drop_log.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def drop_nodes(
        node_id_map: Dict[int, Popen],
        stop_event: threading.Event,
        graph_phase_deadline: float,
        node_drop_count: int,
        node_drop_interval_sec: int,
        node_drop_log: Path,
        session_id: str,
        run_label: str,
        num_nodes: int,
        node_drop_percent: float,
        logs_dir: Path,
) -> None:
    if node_drop_count == 0:
        return

    # Wait until graph generation completes before dropping anything. The deadline
    # is anchored to the protocol start_time (an absolute epoch shared with the node
    # configs), not to this thread's start, so the drop instant does not drift with
    # however long node launch takes.
    wait_sec = graph_phase_deadline - time.time()
    if wait_sec > 0 and stop_event.wait(timeout=wait_sec):
        return

    candidates = list(node_id_map.items())
    random.shuffle(candidates)
    dropped_so_far = 0

    for node_id, proc in candidates:
        if stop_event.is_set() or dropped_so_far >= node_drop_count:
            break

        if proc.poll() is None:
            proc.terminate()
            dropped_so_far += 1
            timestamp = datetime.now(timezone.utc).isoformat()
            record = {
                "timestamp": timestamp,
                "session_id": session_id,
                "run_label": run_label,
                "node_id": node_id,
                "node_log": str(logs_dir / f"node-{node_id}.log"),
                "dropped_so_far": dropped_so_far,
                "node_drop_count": node_drop_count,
                "num_nodes": num_nodes,
                "node_drop_percent": node_drop_percent,
            }
            append_node_drop_log(node_drop_log, record)
            print(f"[{timestamp}] Dropped node-{node_id} ({dropped_so_far}/{node_drop_count})")

            if dropped_so_far < node_drop_count:
                if stop_event.wait(timeout=node_drop_interval_sec):
                    break

    append_node_drop_log(node_drop_log, {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "run_label": run_label,
        "summary": True,
        "node_drop_count_configured": node_drop_count,
        "node_drop_count_actual": dropped_so_far,
        "num_nodes": num_nodes,
        "node_drop_percent": node_drop_percent,
    })


def validate_args(
        num_nodes: int,
        max_outbound_degree: int,
        diameter: int,
        log_level: str,
        drop_on_send_percent: float,
        drop_on_send_probability: float,
        node_drop_percent: float,
        peer_drop_percent: float = 0.0,
) -> None:
    if num_nodes < 2:
        sys.exit("Error: Number of nodes must be a positive integer >= 2")
    if max_outbound_degree < 1:
        sys.exit("Error: Max outbound degree must be a positive integer >= 1")
    if diameter < 2:
        sys.exit("Error: Diameter must be a positive integer >= 2")
    if log_level not in {"debug", "info", "warn", "error"}:
        sys.exit("Error: Log level must be one of: debug, info, warn, error")
    if not 0 <= drop_on_send_percent <= 100:
        sys.exit("Error: drop-on-send percent must be within [0, 100]")
    if not 0.0 <= drop_on_send_probability <= 1.0:
        sys.exit("Error: drop-on-send probability must be within [0.0, 1.0]")
    if not 0 <= node_drop_percent <= 100:
        sys.exit("Error: node-drop percent must be within [0, 100]")
    if not 0 <= peer_drop_percent <= 100:
        sys.exit("Error: peer-drop percent must be within [0, 100]")


def ensure_files(paths: Paths) -> None:
    if not paths.bin_dir.joinpath("server").is_file():
        sys.exit(f"Error: Server binary not found at {paths.bin_dir / 'server'}")
    if not paths.bin_dir.joinpath("committee-sampling").is_file():
        sys.exit(
            f"Error: Committee-sampling binary not found at {paths.bin_dir / 'committee-sampling'}"
        )

    # Ensure configs directory exists
    paths.configs_dir.mkdir(parents=True, exist_ok=True)

    # Create a default dev-server.json if it does not exist
    dev_server_cfg = paths.configs_dir / "dev-server.json"
    if not dev_server_cfg.is_file():
        default_server_cfg = {
            "network": {"listen_address": "0.0.0.0", "listen_port": 4001},
            "logging": {"level": "INFO"},
        }
        with dev_server_cfg.open("w", encoding="utf-8") as f:
            json.dump(default_server_cfg, f, indent=2)
        print(f"Created default bootstrap server config at: {dev_server_cfg}")


def mkdirs(paths: Paths) -> None:
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    paths.configs_dir.mkdir(parents=True, exist_ok=True)
    # Clear previous PIDs file
    paths.pids_file.write_text("")


def write_committee_config(
        paths: Paths,
        session_id: str,
        max_outbound_degree: int,
        diameter: int,
        graph_building_rounds: int,
        num_nodes: int,
        bootstrap_address: str,
        log_level: str,
        verify_timeout: str,
        graph_discovery_timeout: str,
        graph_building_round_timeout: str,
        start_time: int,
        config_file_path: Optional[Path] = None,
        metrics_enabled: bool = False,
        pushgateway_enabled: bool = False,
        drop_on_send_enabled: bool = False,
        drop_on_send_probability: float = 0.0,
        committee_size: int = 30,
        peer_drop_enabled: bool = False,
) -> Path:
    config_file = config_file_path or (
            paths.configs_dir / "committee-sampling-conf.json"
    )

    config = {
        "network": {
            "listen_port": 0,
            "max_outbound_degree": max_outbound_degree,
            "discovery_config": {
                "protocol_id": "/committee-sampling/1.0.0",
                "interval": "5s",
                "bootstrap_peers": [bootstrap_address],
            },
            "drop_on_send": drop_on_send_enabled,
            "drop_on_send_probability": drop_on_send_probability,
            "peer_drop_enabled": peer_drop_enabled,
        },
        "graph": {
            "diameter": diameter,
            "grading_levels": 5,
            "building_rounds": graph_building_rounds,
        },
        "committee": {
            "session_id": session_id,
            "lambda": 256,
            "weight": 1,
            "delta_w": 10,
            "committee_size": committee_size,
            "delay": 20,
            "total_weight": num_nodes,
        },
        "synchronization": {
            "type": 0,
            "ex_ante_round_timeout": verify_timeout,
            "ex_post_round_timeout": verify_timeout,
            "mdag_round_timeout": "10s",
            "start_time": start_time,
            "graph_discovery_timeout": _ensure_duration(graph_discovery_timeout),
            "graph_building_round_timeout": _ensure_duration(graph_building_round_timeout),
            "time_server": "time.google.com",
        },
        "logger": {"level": log_level},
        "metrics": {
            "enabled": metrics_enabled,
            "push_gateway": {
                "enabled": pushgateway_enabled,
                "url": "http://localhost:9091",
            },
            "http_server": {"enabled": False, "port": 0, "path": "/metrics"},
            "push_interval": "30s",
            "job_name": "committee-sampling-simulation",
        },
    }

    with config_file.open("w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    return config_file


def start_bootstrap_server(paths: Paths) -> Popen:
    print("Starting DHT bootstrap server...")
    server_bin = str(paths.bin_dir / "server")
    server_cfg = str(paths.configs_dir / "dev-server.json")
    # Ensure parent dir exists for bootstrap log (it may live inside logs_dir per run)
    paths.bootstrap_log.parent.mkdir(parents=True, exist_ok=True)
    # Open, spawn, then close our handle to avoid FD leaks in the parent
    with paths.bootstrap_log.open("w") as bootstrap_log_fh:
        proc = Popen(
            [server_bin, "-config", server_cfg], stdout=bootstrap_log_fh, stderr=STDOUT
        )
    paths.bootstrap_pid_file.write_text(str(proc.pid))
    print(f"Bootstrap server started (PID: {proc.pid})")
    print("Waiting 10 seconds for bootstrap server to be ready...")
    time.sleep(10)
    return proc


def read_bootstrap_address(paths: Paths) -> str:
    if not paths.bootstrap_address_file.is_file():
        print("Failed to read bootstrap address from file")
        print(
            "Check bootstrap.log for the bootstrap address and update the script manually"
        )
        sys.exit(1)
    address = paths.bootstrap_address_file.read_text().strip()
    print(f"Bootstrap address: {address}")
    return address


def start_node(paths: Paths, node_id: int, config_file: Path) -> Popen:
    log_file = paths.logs_dir / f"node-{node_id}.log"
    cmd = [str(paths.bin_dir / "committee-sampling"), "-config", str(config_file)]
    # Open, spawn, then close our handle to avoid FD leaks in the parent
    with log_file.open("w") as log_fh:
        proc = Popen(cmd, stdout=log_fh, stderr=STDOUT)
    with paths.pids_file.open("a") as pf:
        pf.write(f"{proc.pid}\n")

    return proc


def backup_logs(paths: Paths, session_id: str) -> Path:
    archive_path = paths.base_dir / f"{session_id}.tar.gz"
    if not paths.logs_dir.exists():
        return archive_path
    print(f"Archiving logs to: {archive_path}")
    with tarfile.open(archive_path, "w:gz") as tar:
        # Add only node logs, exclude bootstrap.log
        for log_file in paths.logs_dir.glob("node-*.log"):
            tar.add(log_file, arcname=f"{session_id}/{log_file.name}")
    return archive_path


# noinspection PyBroadException
def cleanup(paths: Paths, procs: Processes, session_id: str) -> None:
    print("")
    print("Stopping all nodes...")

    # Stop node processes
    for p in procs.node_procs:
        if p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass

    # Stop bootstrap server
    if paths.bootstrap_pid_file.is_file():
        try:
            boot_pid_str = paths.bootstrap_pid_file.read_text().strip()
            if boot_pid_str:
                os.kill(int(boot_pid_str), signal.SIGTERM)
        except Exception:
            pass
        try:
            paths.bootstrap_pid_file.unlink(missing_ok=True)
        except Exception:
            pass

    # Backup logs only
    archive_path = backup_logs(paths, session_id)

    # Clean up temporary logs (but keep configs as requested in bash script)
    print("Cleaning up temporary logs...")
    try:
        rmtree(paths.logs_dir, ignore_errors=True)
    except Exception:
        pass
    try:
        paths.pids_file.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        paths.bootstrap_address_file.unlink(missing_ok=True)
    except Exception:
        pass

    print("Cleaning up temporary configs...")
    try:
        rmtree(paths.configs_dir, ignore_errors=True)
    except Exception:
        pass

    print("All nodes and bootstrap server stopped")
    print(f"Logs archived: {archive_path}")
    print("Configuration files removed")


def run_one_simulation(
        base_paths: Paths,
        num_nodes: int,
        max_outbound_degree: int,
        diameter: int,
        log_level: str,
        run_label: str,
        drop_on_send_percent: float,
        drop_on_send_probability: float,
        node_drop_percent: float,
        node_drop_interval_sec: int,
        node_drop_log: Path,
        verify_timeout: str = "30s",
        committee_size: int = 30,
        graph_building_rounds: int = 3,
        graph_discovery_timeout: str = "30s",
        graph_building_round_timeout: str = "2m",
        scenario_name: str = "",
        peer_drop_percent: float = 0.0,
) -> None:
    validate_args(
        num_nodes,
        max_outbound_degree,
        diameter,
        log_level,
        drop_on_send_percent,
        drop_on_send_probability,
        node_drop_percent,
        peer_drop_percent,
    )

    # Create per-run paths (logs in a dedicated folder; bootstrap log inside logs folder)
    logs_dir = base_paths.base_dir / "logs" / run_label
    paths = Paths(
        base_dir=base_paths.base_dir,
        bin_dir=base_paths.bin_dir,
        logs_dir=logs_dir,
        configs_dir=base_paths.configs_dir,
        pids_file=base_paths.base_dir / f"node_pids_{run_label}.txt",
        bootstrap_log=logs_dir / "bootstrap.log",
        bootstrap_pid_file=base_paths.base_dir / f"bootstrap_pid_{run_label}.txt",
        bootstrap_address_file=base_paths.bootstrap_address_file,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    session_id = f"{scenario_name}-{timestamp}" if scenario_name else f"simulation-{timestamp}-n{num_nodes}-m{max_outbound_degree}-d{diameter}-c{committee_size}"

    print(f"🚀 Starting {num_nodes} committee-sampling simulation nodes...")
    print(f"Session ID: {session_id}")
    print(f"Logs directory: {paths.logs_dir}")
    print(f"Configs directory: {paths.configs_dir}")
    print("")

    mkdirs(paths)
    ensure_files(paths)

    # Trap signals to clean up
    procs = Processes(server_proc=None, node_procs=[])
    stop_event = threading.Event()

    drop_count = percent_to_count(num_nodes, drop_on_send_percent)
    base_count = num_nodes - drop_count
    if base_count < 0:
        sys.exit("Error: configuration assigns more specialised nodes than available")

    peer_drop_count = percent_to_count(num_nodes, peer_drop_percent)
    # Space dropper indices evenly: every x-th node (1-indexed), not a contiguous block.
    if peer_drop_count > 0:
        x = max(1, num_nodes // peer_drop_count)
        peer_drop_indices = {i * x + 1 for i in range(peer_drop_count)}
    else:
        peer_drop_indices: set = set()

    node_drop_count = percent_to_count(num_nodes, node_drop_percent)
    if num_nodes - node_drop_count < committee_size:
        sys.exit(f"Error: node-drop percent leaves fewer than committee_size ({committee_size}) nodes alive")

    # Total graph-generation pre-phase duration (protocol start offset + graph
    # discovery + graph building rounds). Used for display; the absolute drop
    # deadline is anchored to the protocol start_time once it is fixed below.
    graph_phase_sec = (
        PROTOCOL_START_OFFSET_SEC
        + parse_duration(graph_discovery_timeout)
        + graph_building_rounds * parse_duration(graph_building_round_timeout)
    )
    # interval 0 = drop all configured nodes at once (batch) right after the graph
    # phase, so the configured node_drop_percent is actually realized even on short
    # runs; a positive interval spaces drops out for gradual-churn experiments.
    effective_interval = max(0, node_drop_interval_sec)
    drop_mode_desc = "all at once (batch)" if effective_interval == 0 else f"every {effective_interval}s"

    node_id_map: Dict[int, Popen] = {}

    def _handle_signal(_, __):
        stop_event.set()
        cleanup(paths, procs, session_id)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Determine log level status
    log_level_status = f"{log_level} (default)"

    print("Network parameters:")
    print(f"- Number of nodes: {num_nodes}")
    print(f"- Max outbound degree: {max_outbound_degree}")
    print(f"- Diameter: {diameter}")
    print(f"- Graph building rounds: {graph_building_rounds}")
    print(f"- Graph discovery timeout: {graph_discovery_timeout}")
    print(f"- Graph building round timeout: {graph_building_round_timeout}")
    print(f"- Log level: {log_level_status}")
    print(f"- Drop-on-send nodes: {drop_count} ({drop_on_send_percent:.2f}%)")
    print(f"- Node-drop count: {node_drop_count} ({node_drop_percent:.2f}%), starting after graph phase ({graph_phase_sec}s), dropping {drop_mode_desc}")
    print("")

    # Start bootstrap server
    server_proc = start_bootstrap_server(paths)
    procs.server_proc = server_proc

    # Read bootstrap address
    bootstrap_address = read_bootstrap_address(paths)

    # Fix one protocol start time, shared by every node's config (so all nodes run
    # on a single clock) and by the drop thread. +OFFSET gives nodes time to boot
    # and discover before the protocol begins.
    protocol_start_time = int(time.time()) + PROTOCOL_START_OFFSET_SEC
    # Absolute epoch at which graph generation ends, anchored to protocol_start_time
    # so the drop fires at the same protocol-relative instant regardless of how long
    # node launch takes (matters at 500-1000 nodes, where launch is many seconds).
    graph_phase_deadline = (
        protocol_start_time
        + parse_duration(graph_discovery_timeout)
        + graph_building_rounds * parse_duration(graph_building_round_timeout)
    )

    drop_thread = threading.Thread(
        target=drop_nodes,
        args=(
            node_id_map,
            stop_event,
            graph_phase_deadline,
            node_drop_count,
            effective_interval,
            node_drop_log,
            session_id,
            run_label,
            num_nodes,
            node_drop_percent,
            logs_dir,
        ),
        daemon=True,
    )

    # Create configuration(s) only for distinct (drop_on_send, peer_drop) flag combinations
    # and reuse them across nodes that share the same settings.
    print("")
    print("Starting nodes...")

    config_cache: Dict[Tuple[bool, bool], Path] = {}

    def ensure_config(drop_on_send_flag: bool, peer_drop_flag: bool) -> Path:
        key = (drop_on_send_flag, peer_drop_flag)
        if key in config_cache:
            return config_cache[key]
        parts = []
        if drop_on_send_flag:
            parts.append("msgsend")
        if peer_drop_flag:
            parts.append("peerdrop")
        suffix = "-".join(parts) if parts else "base"
        conf_path = paths.configs_dir / f"committee-sampling-conf-{run_label}-{suffix}.json"
        write_committee_config(
            paths,
            session_id=session_id,
            max_outbound_degree=max_outbound_degree,
            diameter=diameter,
            graph_building_rounds=graph_building_rounds,
            num_nodes=num_nodes,
            bootstrap_address=bootstrap_address,
            log_level=log_level,
            config_file_path=conf_path,
            metrics_enabled=False,
            pushgateway_enabled=False,
            drop_on_send_enabled=drop_on_send_flag,
            drop_on_send_probability=drop_on_send_probability if drop_on_send_flag else 0.0,
            verify_timeout=verify_timeout,
            graph_discovery_timeout=graph_discovery_timeout,
            graph_building_round_timeout=graph_building_round_timeout,
            start_time=protocol_start_time,
            committee_size=committee_size,
            peer_drop_enabled=peer_drop_flag,
        )
        config_cache[key] = conf_path
        return conf_path

    # Assign configs by iterating every node index so peer-drop nodes are spaced
    # evenly (every x-th index) rather than grouped at the front.
    # drop_on_send is applied to the first drop_count nodes (existing behaviour).
    drop_on_send_indices = set(range(1, drop_count + 1))
    config_sequence: List[Path] = []
    for idx in range(1, num_nodes + 1):
        msg_drop = idx in drop_on_send_indices
        peer_drop = idx in peer_drop_indices
        config_sequence.append(ensure_config(msg_drop, peer_drop))

    if len(config_sequence) != num_nodes:
        sys.exit("Error: internal configuration mismatch while assigning node configs")

    # Log peer-drop dropper indices before starting nodes
    if peer_drop_count > 0:
        peer_drop_log = paths.logs_dir / "peer_drops.log"
        paths.logs_dir.mkdir(parents=True, exist_ok=True)
        append_node_drop_log(peer_drop_log, {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
            "run_label": run_label,
            "peer_drop_count": peer_drop_count,
            "peer_drop_percent": peer_drop_percent,
            "num_nodes": num_nodes,
            "dropper_indices": sorted(peer_drop_indices),
        })

    for idx, cfg_path in enumerate(config_sequence, start=1):
        proc = start_node(paths, idx, cfg_path)
        procs.node_procs.append(proc)
        node_id_map[idx] = proc

    drop_thread.start()

    print("")
    print(f"All {num_nodes} nodes started successfully!")
    print("")
    print("Simulation Status:")
    print(f"- Nodes: {num_nodes}")
    print(f"- Max outbound degree: {max_outbound_degree} (provided)")
    print(f"- Diameter: {diameter} (provided)")
    print(f"- Log level: {log_level_status}")
    print("- Ports: Auto-assigned by system (port 0 configured)")
    print(f"- Session ID: {session_id}")
    print(f"- Drop-on-send nodes: {drop_count} ({drop_on_send_percent:.2f}%)")
    print(f"- Peer-drop nodes: {peer_drop_count} ({peer_drop_percent:.2f}%)")
    print(f"- Node-drop count: {node_drop_count} ({node_drop_percent:.2f}%), after {graph_phase_sec}s, dropping {drop_mode_desc}")
    print("- Discovery: DHT with bootstrap server")
    print(f"- Log files: {paths.logs_dir}/node-*.log")
    print(f"- Bootstrap log: {paths.bootstrap_log}")
    unique_cfgs = sorted(set(config_cache.values()), key=lambda p: str(p))
    if unique_cfgs:
        print("- Committee configs:")
        for cfg in unique_cfgs:
            print(f"  - {cfg}")
    else:
        print("- Committee configs: none")
    print(f"- Server config: {paths.configs_dir / 'dev-server.json'}")
    print("")

    # Monitor nodes
    print(
        "Monitoring nodes (press Ctrl+C to stop all, or wait for automatic completion)..."
    )
    print(
        "Nodes will automatically stop when the committee-sampling simulation completes."
    )
    print("")

    completed = 0
    total = len(procs.node_procs)
    with tqdm(total=total, desc="Completed nodes") as pbar:
        while True:
            running_count = sum(1 for p in procs.node_procs if p.poll() is None)

            if total - running_count > completed:
                pbar.update(total - running_count - completed)
                completed = total - running_count
                pbar.set_postfix(completed=completed, running=running_count)

            if running_count == 0:
                print(
                    time.strftime("%H:%M:%S"),
                    "- All node processes have stopped. Waiting 30 seconds to confirm completion...",
                )
                print("")
                if node_drop_count > 0:
                    print(f"🎉 Simulation finished (up to {node_drop_count} of {num_nodes} nodes dropped by the adversarial model).")
                else:
                    print("🎉 All committee-sampling nodes have completed successfully!")
                print(f"Simulation finished at: {datetime.now()}")
                print("")
                break
            time.sleep(30)

    stop_event.set()
    drop_thread.join(timeout=5)

    print("Cleaning up and creating log archive...")
    cleanup(paths, procs, session_id)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run committee-sampling simulations from a JSON config file"
    )
    parser.add_argument(
        "batch_config",
        help="Path to a JSON file describing multiple simulations to run sequentially",
    )
    parser.add_argument(
        "--default-log-level",
        dest="default_log_level",
        default="info",
        choices=["debug", "info", "warn", "error"],
        help="Default log level used when a run omits 'log_level' (default: info)",
    )
    parser.add_argument(
        "--drop-on-send-percent",
        dest="drop_on_send_percent",
        type=float,
        default=0.0,
        help="Percentage [0-100] of nodes per run that enable simulated drop-on-send (default: 0)",
    )
    parser.add_argument(
        "--drop-on-send-probability",
        dest="drop_on_send_probability",
        type=float,
        default=0.1,
        help="Probability [0-1] used when drop-on-send is enabled for a node (default: 0.1)",
    )
    parser.add_argument(
        "--graph-discovery-timeout",
        dest="graph_discovery_timeout",
        default="30s",
        help="Duration to continue graph discovery before starting building rounds (default: 30s)",
    )
    parser.add_argument(
        "--graph-building-round-timeout",
        dest="graph_building_round_timeout",
        default="2m",
        help="Duration allocated per graph building round (default: 2m)",
    )
    parser.add_argument(
        "--graph-building-rounds",
        dest="graph_building_rounds",
        type=int,
        default=4,
        help="Number of graph building rounds to execute (must be even, default: 4)",
    )
    parser.add_argument(
        "--node-drop-percent",
        dest="node_drop_percent",
        type=float,
        default=0.0,
        help="Percentage [0-100] of nodes to terminate after graph generation (default: 0 = disabled)",
    )
    parser.add_argument(
        "--node-drop-interval-sec",
        dest="node_drop_interval_sec",
        type=int,
        default=0,
        help="Seconds between successive node drops; 0 = drop all configured nodes at once right after graph generation (default: 0)",
    )
    parser.add_argument(
        "--node-drop-log",
        dest="node_drop_log",
        type=str,
        default="",
        help="Path to the persistent node-drop log file (default: <base_dir>/node_drops.log)",
    )

    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    base_paths = Paths(
        base_dir=base_dir,
        bin_dir=base_dir / "bin",
        logs_dir=base_dir / "logs",  # base logs dir; per-run subdir created later
        configs_dir=base_dir / "configs",
        pids_file=base_dir
                  / "node_pids.txt",  # unused in batch; per-run file created later
        bootstrap_log=base_dir
                      / "bootstrap.log",  # unused in batch; per-run file created later
        bootstrap_pid_file=base_dir
                           / "bootstrap_pid.txt",  # unused in batch; per-run file created later
        bootstrap_address_file=base_dir / "bootstrap_address.txt",
    )

    cfg_path = Path(args.batch_config)
    if not cfg_path.is_file():
        sys.exit(f"Error: batch config file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        try:
            batch_cfg = json.load(f)
        except json.JSONDecodeError as e:
            sys.exit(f"Error: failed to parse JSON: {e}")

    runs = batch_cfg.get("runs")
    if not isinstance(runs, list) or not runs:
        sys.exit("Error: batch config must contain a non-empty 'runs' array")
    sleep_between = int(batch_cfg.get("sleep_between_runs_sec", 0) or 0)

    for idx, run in enumerate(runs, start=1):
        # Flexible field names
        num_nodes = run.get("number_of_nodes", run.get("num_nodes"))
        max_deg = run.get("max_outbound_degree", run.get("max_degree"))
        diameter = run.get("diameter")
        log_level = run.get("log_level", args.default_log_level)
        drop_on_send_percent = float(
            run.get("drop_on_send_percent", args.drop_on_send_percent)
        )
        drop_on_send_probability = float(
            run.get("drop_on_send_probability", args.drop_on_send_probability)
        )
        node_drop_percent = float(run.get("node_drop_percent", args.node_drop_percent))
        node_drop_interval_sec = int(run.get("node_drop_interval_sec", args.node_drop_interval_sec))
        verify_timeout = str(run.get("verify_timeout", "30s"))
        committee_size = int(run.get("committee_size", 30))
        graph_discovery_timeout = run.get("graph_discovery_timeout")
        if not graph_discovery_timeout:
            graph_discovery_timeout = args.graph_discovery_timeout
        graph_discovery_timeout = str(graph_discovery_timeout)

        graph_building_round_timeout = run.get("graph_building_round_timeout")
        if not graph_building_round_timeout:
            graph_building_round_timeout = run.get("building_graph_timeout")
        if not graph_building_round_timeout:
            graph_building_round_timeout = args.graph_building_round_timeout
        graph_building_round_timeout = str(graph_building_round_timeout)

        graph_building_rounds = run.get("graph_building_rounds")
        if graph_building_rounds is None:
            graph_building_rounds = run.get("building_rounds")
        if graph_building_rounds is None:
            graph_building_rounds = args.graph_building_rounds
        graph_building_rounds = int(graph_building_rounds)

        if num_nodes is None or max_deg is None or diameter is None:
            sys.exit(
                f"Error: run #{idx} must include 'number_of_nodes' (or 'num_nodes'), 'max_outbound_degree' (or 'max_degree'), and 'diameter'"
            )

        scenario_name = run.get("name", "")
        
        node_drop_log = Path(args.node_drop_log) if args.node_drop_log else base_dir / "node_drops.log"

        run_label = f"run-{idx:02d}-{num_nodes}n-{max_deg}m-d{diameter}"
        run_one_simulation(
            base_paths,
            num_nodes=int(num_nodes),
            max_outbound_degree=int(max_deg),
            diameter=int(diameter),
            log_level=str(log_level),
            run_label=run_label,
            drop_on_send_percent=drop_on_send_percent,
            drop_on_send_probability=drop_on_send_probability,
            node_drop_percent=node_drop_percent,
            node_drop_interval_sec=node_drop_interval_sec,
            node_drop_log=node_drop_log,
            verify_timeout=verify_timeout,
            committee_size=committee_size,
            graph_building_rounds=graph_building_rounds,
            graph_discovery_timeout=graph_discovery_timeout,
            graph_building_round_timeout=graph_building_round_timeout,
            scenario_name=scenario_name,
        )

        if idx < len(runs) and sleep_between > 0:
            print(f"Sleeping {sleep_between}s before the next run...")
            time.sleep(sleep_between)


if __name__ == "__main__":
    main()
