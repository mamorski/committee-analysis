package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"os"
	"os/exec"
	osSignal "os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Paths struct {
	BaseDir              string
	BinDir               string
	LogsDir              string
	ConfigsDir           string
	PidsFile             string
	BootstrapLog         string
	BootstrapPidFile     string
	BootstrapAddressFile string
}

type Proc struct {
	Cmd *exec.Cmd
}

type Processes struct {
	Server *Proc
	Nodes  []*Proc
}

type BatchConfig struct {
	SleepBetweenRunsSec int              `json:"sleep_between_runs_sec"`
	Runs                []map[string]any `json:"runs"`
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func ensureFiles(paths Paths) error {
	if !fileExists(filepath.Join(paths.BinDir, "server")) {
		return fmt.Errorf("server binary not found at %s", filepath.Join(paths.BinDir, "server"))
	}
	if !fileExists(filepath.Join(paths.BinDir, "committee-sampling")) {
		return fmt.Errorf("committee-sampling binary not found at %s", filepath.Join(paths.BinDir, "committee-sampling"))
	}

	if err := os.MkdirAll(paths.ConfigsDir, 0o755); err != nil {
		return err
	}

	devServerCfg := filepath.Join(paths.ConfigsDir, "dev-server.json")
	if !fileExists(devServerCfg) {
		defaultCfg := map[string]any{
			"network": map[string]any{
				"listen_address": "0.0.0.0",
				"listen_port":    4001,
			},
			"logging": map[string]any{
				"level": "INFO",
			},
		}
		f, err := os.Create(devServerCfg)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(defaultCfg); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
		fmt.Printf("Created default bootstrap server config at: %s\n", devServerCfg)
	}

	return nil
}

func mkdirs(paths Paths) error {
	if err := os.MkdirAll(paths.LogsDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(paths.ConfigsDir, 0o755); err != nil {
		return err
	}
	// Clear previous PIDs file
	return os.WriteFile(paths.PidsFile, []byte(""), 0o644)
}

func startBootstrapServer(paths Paths) (*Proc, error) {
	fmt.Println("Starting DHT bootstrap server...")
	serverBin := filepath.Join(paths.BinDir, "server")
	serverCfg := filepath.Join(paths.ConfigsDir, "dev-server.json")

	if err := os.MkdirAll(filepath.Dir(paths.BootstrapLog), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.Create(paths.BootstrapLog)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(serverBin, "-config", serverCfg)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	// We will not wait here; ensure reaping in a goroutine
	go func() { _ = cmd.Wait(); _ = logFile.Close() }()

	if err := os.WriteFile(paths.BootstrapPidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0o644); err != nil {
		return nil, err
	}
	fmt.Printf("Bootstrap server started (PID: %d)\n", cmd.Process.Pid)
	fmt.Println("Waiting 10 seconds for bootstrap server to be ready...")
	time.Sleep(10 * time.Second)
	return &Proc{Cmd: cmd}, nil
}

func readBootstrapAddress(paths Paths) (string, error) {
	b, err := os.ReadFile(paths.BootstrapAddressFile)
	if err != nil {
		return "", fmt.Errorf("failed to read bootstrap address from file: %w", err)
	}
	addr := strings.TrimSpace(string(b))
	if addr == "" {
		return "", errors.New("bootstrap address is empty")
	}
	fmt.Printf("Bootstrap address: %s\n", addr)
	return addr, nil
}

func writeCommitteeConfig(
	paths Paths,
	sessionID string,
	maxOutbound int,
	diameter int,
	numNodes int,
	bootstrapAddr string,
	logLevel string,
	verifyTimeout string,
	configPath string,
	metricsEnabled bool,
	pushGatewayEnabled bool,
	dropOnSendEnabled bool,
	dropOnSendProbability float64,
	committeeSize int,
) (string, error) {
	cfg := map[string]any{
		"network": map[string]any{
			"listen_port":         0,
			"max_outbound_degree": maxOutbound,
			"discovery_config": map[string]any{
				"protocol_id":     "/committee-sampling/1.0.0",
				"interval":        "5s",
				"bootstrap_peers": []string{bootstrapAddr},
			},
			"drop_on_send":             dropOnSendEnabled,
			"drop_on_send_probability": dropOnSendProbability,
		},
		"graph": map[string]any{
			"diameter":       diameter,
			"grading_levels": 5,
		},
		"committee": map[string]any{
			"session_id":     sessionID,
			"lambda":         256,
			"weight":         1,
			"delta_w":        10,
			"committee_size": committeeSize,
			"delay":          20,
			"total_weight":   numNodes,
		},
		"synchronization": map[string]any{
			"type":                   0,
			"ex_ante_round_timeout":  verifyTimeout,
			"ex_post_round_timeout":  verifyTimeout,
			"mdag_round_timeout":     "10s",
			"start_time":             time.Now().Unix() + 60,
			"building_graph_timeout": "2m",
			"time_server":            "time.google.com",
		},
		"logger": map[string]any{"level": logLevel},
		"metrics": map[string]any{
			"enabled": metricsEnabled,
			"push_gateway": map[string]any{
				"enabled": pushGatewayEnabled,
				"url":     "http://localhost:9091",
			},
			"http_server": map[string]any{
				"enabled": false,
				"port":    0,
				"path":    "/metrics",
			},
			"push_interval": "30s",
			"job_name":      "committee-sampling-simulation",
			"instance_name": "",
		},
	}

	f, err := os.Create(configPath)
	if err != nil {
		return "", err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cfg); err != nil {
		_ = f.Close()
		return "", err
	}
	_ = f.Close()
	return configPath, nil
}

func startNode(paths Paths, nodeID int, configPath string) (*Proc, error) {
	logFile := filepath.Join(paths.LogsDir, fmt.Sprintf("node-%d-log.log", nodeID))
	lf, err := os.Create(logFile)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(filepath.Join(paths.BinDir, "committee-sampling"), "-config", configPath)
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		_ = lf.Close()
		return nil, err
	}
	go func() { _ = cmd.Wait(); _ = lf.Close() }()

	f, _ := os.OpenFile(paths.PidsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if f != nil {
		_, _ = fmt.Fprintf(f, "%d\n", cmd.Process.Pid)
		_ = f.Close()
	}
	fmt.Printf("Node-%d started (PID: %d, Log: %s)\n", nodeID, cmd.Process.Pid, logFile)
	return &Proc{Cmd: cmd}, nil
}

func isRunning(p *Proc) bool {
	if p == nil || p.Cmd == nil || p.Cmd.Process == nil {
		return false
	}
	// Signal 0 to check if process exists
	err := p.Cmd.Process.Signal(syscall.Signal(0))
	return err == nil
}

func killRandomLoop(procs *Processes, stop <-chan struct{}, upTo int, delaySec int, probability float64) {
	if upTo <= 0 || probability <= 0 {
		return
	}
	mrand.Seed(time.Now().UnixNano())
	killed := make(map[int]struct{})
	ticker := time.NewTicker(time.Duration(delaySec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if len(killed) >= upTo {
				return
			}
			if mrand.Float64() < probability {
				// Build list of viable victims
				var candidates []*Proc
				for _, np := range procs.Nodes {
					if np == nil || np.Cmd == nil || np.Cmd.Process == nil {
						continue
					}
					pid := np.Cmd.Process.Pid
					if _, done := killed[pid]; done {
						continue
					}
					if isRunning(np) {
						candidates = append(candidates, np)
					}
				}
				if len(candidates) == 0 {
					continue
				}
				victim := candidates[mrand.Intn(len(candidates))]
				_ = victim.Cmd.Process.Signal(syscall.SIGTERM)
				killed[victim.Cmd.Process.Pid] = struct{}{}
				fmt.Printf("Killed node PID %d\n", victim.Cmd.Process.Pid)
			}
		}
	}
}

func tarGzDir(srcDir, destTarGz string, rootName string) error {
	out, err := os.Create(destTarGz)
	if err != nil {
		return err
	}
	defer out.Close()
	gz := gzip.NewWriter(out)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(filepath.Join(rootName, rel))
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = name
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, f); err != nil {
				_ = f.Close()
				return err
			}
			_ = f.Close()
		}
		return nil
	})
}

func backupAndSummarize(paths Paths) {
	if _, err := os.Stat(paths.LogsDir); err != nil {
		return
	}
	archive := filepath.Join(paths.BaseDir, fmt.Sprintf("logs-archive-%s.tar.gz", time.Now().Format("20060102-150405")))
	fmt.Printf("Archiving logs to: %s\n", archive)
	_ = tarGzDir(paths.LogsDir, archive, "logs")

	fmt.Println()
	fmt.Println("Debugging Summary:")
	fmt.Printf("- Bootstrap server log: %s\n", filepath.Base(paths.BootstrapLog))
	fmt.Printf("- Node logs archive: %s\n", archive)
}

func cleanup(paths Paths, procs *Processes) {
	fmt.Println()
	fmt.Println("Stopping all nodes...")

	// Stop nodes
	for _, np := range procs.Nodes {
		if isRunning(np) {
			_ = np.Cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	// Stop bootstrap
	if b, err := os.ReadFile(paths.BootstrapPidFile); err == nil {
		pidStr := strings.TrimSpace(string(b))
		if pidStr != "" {
			if pid, err2 := strconvAtoi(pidStr); err2 == nil {
				_ = syscall.Kill(pid, syscall.SIGTERM)
			}
		}
		_ = os.Remove(paths.BootstrapPidFile)
	}

	// Backup logs
	if _, err := os.Stat(paths.LogsDir); err == nil {
		backupAndSummarize(paths)
	}

	fmt.Println("Cleaning up temporary logs...")
	_ = os.RemoveAll(paths.LogsDir)
	_ = os.Remove(paths.PidsFile)
	_ = os.Remove(paths.BootstrapAddressFile)

	fmt.Println("Cleaning up temporary configs...")
	_ = os.RemoveAll(paths.ConfigsDir)

	fmt.Println("All nodes and bootstrap server stopped")
	fmt.Println("Logs archived for debugging - look for logs-archive-*.tar.gz in the project root")
	fmt.Println("Configuration files removed")
}

func ceilFloatToInt(v float64) int { return int(math.Ceil(v)) }

func strconvAtoi(s string) (int, error) {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid integer: %s", s)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func runOneSimulation(
	base Paths,
	numNodes int,
	maxOutbound int,
	diameter int,
	logLevel string,
	runLabel string,
	metricsPushPercent float64,
	dropOnSendPercent float64,
	dropOnSendProbability float64,
	killRandomUpTo int,
	killRandomDelaySec int,
	killProbability float64,
	verifyTimeout string,
	committeeSize int,
) error {
	if numNodes < 2 {
		return fmt.Errorf("number_of_nodes must be >= 2")
	}
	if maxOutbound < 1 {
		return fmt.Errorf("max_outbound_degree must be >= 1")
	}
	if diameter < 2 {
		return fmt.Errorf("diameter must be >= 2")
	}
	switch logLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log_level must be one of debug, info, warn, error")
	}

	// Per-run paths
	runPaths := base
	runPaths.LogsDir = filepath.Join(base.BaseDir, "logs", runLabel)
	runPaths.PidsFile = filepath.Join(base.BaseDir, fmt.Sprintf("node_pids_%s.txt", runLabel))
	runPaths.BootstrapLog = filepath.Join(runPaths.LogsDir, "bootstrap.log")
	runPaths.BootstrapPidFile = filepath.Join(base.BaseDir, fmt.Sprintf("bootstrap_pid_%s.txt", runLabel))

	fmt.Printf("🚀 Starting %d committee-sampling simulation nodes...\n", numNodes)
	ts := time.Now().UTC().Format("20060102-150405UTC")
	sessionID := fmt.Sprintf("simulation-%s-n%d-m%d-d%d-c%d", ts, numNodes, maxOutbound, diameter, committeeSize)
	fmt.Printf("Session ID: %s\n", sessionID)
	fmt.Printf("Logs directory: %s\n", runPaths.LogsDir)
	fmt.Printf("Configs directory: %s\n\n", runPaths.ConfigsDir)

	if err := mkdirs(runPaths); err != nil {
		return err
	}
	if err := ensureFiles(runPaths); err != nil {
		return err
	}

	var procs Processes
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	cleanupAndExit := func() {
		stopOnce.Do(func() { close(stopCh) })
		cleanup(runPaths, &procs)
	}

	// Trap signals
	sigCh := make(chan os.Signal, 2)
	signalNotify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cleanupAndExit(); os.Exit(0) }()

	// Start bootstrap
	srv, err := startBootstrapServer(runPaths)
	if err != nil {
		return err
	}
	procs.Server = srv

	// Read bootstrap address
	addr, err := readBootstrapAddress(runPaths)
	if err != nil {
		return err
	}

	// Determine counts
	pushCount := 0
	if metricsPushPercent > 0 {
		pushCount = maxInt(1, ceilFloatToInt(float64(numNodes)*(metricsPushPercent/100.0)))
	}
	dropCount := 0
	if dropOnSendPercent > 0 {
		dropCount = maxInt(1, ceilFloatToInt(float64(numNodes)*(dropOnSendPercent/100.0)))
	}
	remaining := maxInt(0, numNodes-pushCount-dropCount)

	// Write up to three configs
	var monitoredCfg, dropCfg, baseCfg string
	if pushCount > 0 {
		monitoredCfg, err = writeCommitteeConfig(runPaths, sessionID, maxOutbound, diameter, numNodes, addr, logLevel, verifyTimeout, filepath.Join(runPaths.ConfigsDir, fmt.Sprintf("committee-sampling-conf-%s-monitored.json", runLabel)), true, true, false, 0.0, committeeSize)
		if err != nil {
			return err
		}
	}
	if dropCount > 0 {
		dropCfg, err = writeCommitteeConfig(runPaths, sessionID, maxOutbound, diameter, numNodes, addr, logLevel, verifyTimeout, filepath.Join(runPaths.ConfigsDir, fmt.Sprintf("committee-sampling-conf-%s-unmonitored-drop.json", runLabel)), false, false, true, dropOnSendProbability, committeeSize)
		if err != nil {
			return err
		}
	}
	if remaining > 0 {
		baseCfg, err = writeCommitteeConfig(runPaths, sessionID, maxOutbound, diameter, numNodes, addr, logLevel, verifyTimeout, filepath.Join(runPaths.ConfigsDir, fmt.Sprintf("committee-sampling-conf-%s-unmonitored.json", runLabel)), false, false, false, 0.0, committeeSize)
		if err != nil {
			return err
		}
	}

	// Start nodes
	fmt.Println("Starting nodes...")
	for i := 1; i <= numNodes; i++ {
		var cfg string
		if i <= pushCount {
			cfg = monitoredCfg
		} else if i <= pushCount+dropCount {
			cfg = dropCfg
		} else {
			cfg = baseCfg
		}
		np, err := startNode(runPaths, i, cfg)
		if err != nil {
			return err
		}
		procs.Nodes = append(procs.Nodes, np)
	}

	// Killer goroutine
	go killRandomLoop(&procs, stopCh, killRandomUpTo, killRandomDelaySec, killProbability)

	fmt.Println()
	fmt.Printf("All %d nodes started successfully!\n\n", numNodes)
	fmt.Println("Simulation Status:")
	fmt.Printf("- Nodes: %d\n", numNodes)
	fmt.Printf("- Max outbound degree: %d (provided)\n", maxOutbound)
	fmt.Printf("- Diameter: %d (provided)\n", diameter)
	fmt.Printf("- Log level: %s\n", logLevel)
	fmt.Println("- Ports: Auto-assigned by system (port 0 configured)")
	fmt.Printf("- Session ID: %s\n", sessionID)
	fmt.Println("- Discovery: DHT with bootstrap server")
	fmt.Printf("- Log files: %s\n", filepath.Join(runPaths.LogsDir, "node-*-log.log"))
	fmt.Printf("- Bootstrap log: %s\n", runPaths.BootstrapLog)
	fmt.Println("- Committee configs:")
	for _, p := range []string{monitoredCfg, dropCfg, baseCfg} {
		if p != "" {
			fmt.Printf("  - %s\n", p)
		}
	}
	fmt.Printf("- Server config: %s\n\n", filepath.Join(runPaths.ConfigsDir, "dev-server.json"))

	// Monitor
	completed := 0
	total := len(procs.Nodes)
	for {
		running := 0
		for _, np := range procs.Nodes {
			if isRunning(np) {
				running++
			}
		}
		newCompleted := total - running
		if newCompleted > completed {
			completed = newCompleted
			fmt.Printf("%s - Completed: %d/%d, Running: %d\n", time.Now().Format("15:04:05"), completed, total, running)
		}
		if running == 0 {
			fmt.Printf("%s - All nodes have stopped.\n", time.Now().Format("15:04:05"))
			break
		}
		time.Sleep(10 * time.Second)
	}

	fmt.Println("Cleaning up and creating log archive...")
	cleanupAndExit()
	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Minimal signal notify to avoid importing os/signal in older Go? We'll use it.
// We'll wrap to keep imports tidy above.
func signalNotify(c chan<- os.Signal, sig ...os.Signal) { osSignal.Notify(c, sig...) }

func main() {
	defaultLogLevel := flag.String("default-log-level", "info", "Default log level used when a run omits 'log_level' (debug|info|warn|error)")
	metricsPushPercent := flag.Float64("metrics-push-percent", 20.0, "Percentage [0-100] of nodes per run that enable PushGateway metrics")
	dropOnSendPercent := flag.Float64("drop-on-send-percent", 0.0, "Percentage [0-100] of nodes per run that enable simulated drop-on-send")
	dropOnSendProbability := flag.Float64("drop-on-send-probability", 0.1, "Probability [0-1] used when drop-on-send is enabled")
	killRandomUpTo := flag.Int("kill-random-up-to", 0, "If >0, randomly terminate up to this many node processes per run")
	killRandomDelaySec := flag.Int("kill-random-delay-sec", 0, "Seconds to wait between kill attempts")
	killProbability := flag.Float64("kill-probability", 0.1, "Probability [0-1] used when killing a node on each attempt")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: main <batch_config.json> [flags]")
		os.Exit(1)
	}
	batchPath := flag.Arg(0)

	baseDir, _ := filepath.Abs(".")
	paths := Paths{
		BaseDir:              baseDir,
		BinDir:               filepath.Join(baseDir, "bin"),
		LogsDir:              filepath.Join(baseDir, "logs"),
		ConfigsDir:           filepath.Join(baseDir, "configs"),
		PidsFile:             filepath.Join(baseDir, "node_pids.txt"),
		BootstrapLog:         filepath.Join(baseDir, "bootstrap.log"),
		BootstrapPidFile:     filepath.Join(baseDir, "bootstrap_pid.txt"),
		BootstrapAddressFile: filepath.Join(baseDir, "bootstrap_address.txt"),
	}

	data, err := os.ReadFile(batchPath)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	var batch BatchConfig
	if err := json.Unmarshal(data, &batch); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	if len(batch.Runs) == 0 {
		fmt.Println("Error: batch config must contain a non-empty 'runs' array")
		os.Exit(1)
	}

	for idx, runMap := range batch.Runs {
		getInt := func(keys ...string) (int, bool) {
			for _, k := range keys {
				if v, ok := runMap[k]; ok {
					switch t := v.(type) {
					case float64:
						return int(t), true
					case int:
						return t, true
					case json.Number:
						if i, err := t.Int64(); err == nil {
							return int(i), true
						}
					}
				}
			}
			return 0, false
		}
		getFloat := func(key string, def float64) float64 {
			if v, ok := runMap[key]; ok {
				switch t := v.(type) {
				case float64:
					return t
				case json.Number:
					if f, err := t.Float64(); err == nil {
						return f
					}
				}
			}
			return def
		}
		getString := func(key, def string) string {
			if v, ok := runMap[key]; ok {
				if s, ok2 := v.(string); ok2 {
					return s
				}
			}
			return def
		}

		n, okN := getInt("number_of_nodes", "num_nodes")
		m, okM := getInt("max_outbound_degree", "max_degree")
		d, okD := getInt("diameter")
		if !okN || !okM || !okD {
			fmt.Printf("Error: run #%d missing required fields\n", idx+1)
			os.Exit(1)
		}

		ll := getString("log_level", *defaultLogLevel)
		pushPct := getFloat("metrics_push_percent", *metricsPushPercent)
		dropPct := getFloat("drop_on_send_percent", *dropOnSendPercent)
		dropProb := getFloat("drop_on_send_probability", *dropOnSendProbability)
		killUpTo := int(getFloat("kill_random_up_to", float64(*killRandomUpTo)))
		killDelay := int(getFloat("kill_random_delay_sec", float64(*killRandomDelaySec)))
		killProb := getFloat("kill_probability", *killProbability)
		verifyTimeout := getString("verify_timeout", "30s")
		committeeSize := int(getFloat("committee_size", 30))

		runLabel := fmt.Sprintf("run-%02d-%dn-%dm-d%d", idx+1, n, m, d)
		if err := runOneSimulation(paths, n, m, d, ll, runLabel, pushPct, dropPct, dropProb, killUpTo, killDelay, killProb, verifyTimeout, committeeSize); err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}

		if idx < len(batch.Runs)-1 && batch.SleepBetweenRunsSec > 0 {
			fmt.Printf("Sleeping %ds before the next run...\n", batch.SleepBetweenRunsSec)
			time.Sleep(time.Duration(batch.SleepBetweenRunsSec) * time.Second)
		}
	}
}
