#!/bin/bash

# Script to run committee-sampling simulation with configurable parameters
# Usage: ./run_simulation.sh <number_of_nodes> <max_outbound_degree> <diameter> [log_level]

set -e

# Check if required parameters are provided
if [ $# -lt 3 ] || [ $# -gt 4 ]; then
    echo "Usage: $0 <number_of_nodes> <max_outbound_degree> <diameter> [log_level]"
    echo "Example: $0 20 4 6 debug"
    echo "Example: $0 20 4 6        (defaults to info log level)"
    echo "Log levels: debug, info, warn, error (default: info)"
    exit 1
fi

NUM_NODES=$1
MAX_OUTBOUND_DEGREE=$2
DIAMETER=$3
LOG_LEVEL=${4:-info}

# Validate number of nodes
if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [ "$NUM_NODES" -lt 2 ]; then
    echo "Error: Number of nodes must be a positive integer >= 2"
    exit 1
fi

# Validate max outbound degree
if ! [[ "$MAX_OUTBOUND_DEGREE" =~ ^[0-9]+$ ]] || [ "$MAX_OUTBOUND_DEGREE" -lt 1 ]; then
    echo "Error: Max outbound degree must be a positive integer >= 1"
    exit 1
fi

# Validate diameter
if ! [[ "$DIAMETER" =~ ^[0-9]+$ ]] || [ "$DIAMETER" -lt 2 ]; then
    echo "Error: Diameter must be a positive integer >= 2"
    exit 1
fi

# Validate log level
case "$LOG_LEVEL" in
    debug|info|warn|error)
        ;;
    *)
        echo "Error: Log level must be one of: debug, info, warn, error"
        exit 1
        ;;
esac

# Configuration
SESSION_ID="simulation-$(date +%s)"
LOGS_DIR="./logs"
CONFIGS_DIR="./configs"
PIDS_FILE="./node_pids.txt"
BIN_DIR="./bin"

echo "🚀 Starting $NUM_NODES committee-sampling simulation nodes..."
echo "Session ID: $SESSION_ID"
echo "Logs directory: $LOGS_DIR"
echo "Configs directory: $CONFIGS_DIR"
echo ""

# Check if binaries exist
if [ ! -f "$BIN_DIR/server" ]; then
    echo "Error: Server binary not found at $BIN_DIR/server"
    exit 1
fi

if [ ! -f "$BIN_DIR/committee-sampling" ]; then
    echo "Error: Committee-sampling binary not found at $BIN_DIR/committee-sampling"
    exit 1
fi

if [ ! -f "$CONFIGS_DIR/dev-server.json" ]; then
    echo "Error: Server config not found at $CONFIGS_DIR/dev-server.json"
    exit 1
fi

# Create directories
mkdir -p "$LOGS_DIR"
mkdir -p "$CONFIGS_DIR"

# Clear previous PIDs file
> "$PIDS_FILE"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping all nodes..."
    if [ -f "$PIDS_FILE" ]; then
        while IFS= read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "Stopping node with PID: $pid"
                kill "$pid"
            fi
        done < "$PIDS_FILE"
    fi
    
    # Stop bootstrap server
    if [ -f "bootstrap_pid.txt" ]; then
        BOOTSTRAP_PID=$(cat bootstrap_pid.txt)
        if kill -0 "$BOOTSTRAP_PID" 2>/dev/null; then
            echo "Stopping bootstrap server (PID: $BOOTSTRAP_PID)"
            kill "$BOOTSTRAP_PID"
        fi
        rm -f bootstrap_pid.txt
    fi
    
    # Create backup of logs for debugging
    if [ -d "$LOGS_DIR" ]; then
        BACKUP_DIR="./logs-backup-$(date +%Y%m%d-%H%M%S)"
        echo "Creating backup of logs at: $BACKUP_DIR"
        cp -r "$LOGS_DIR" "$BACKUP_DIR"
        
        # Show a summary of what happened
        echo ""
        echo "Debugging Summary:"
        echo "- Bootstrap server log: bootstrap.log"
        echo "- Node logs backup: $BACKUP_DIR/"
        echo "- Neighbor connection summary:"
        
        # Count successful neighbor connections
        if [ -f "bootstrap.log" ]; then
            echo "  - Bootstrap connections: $(grep -c "Node connected to DHT" bootstrap.log || echo "0")"
        fi
        
        # Show peer discovery attempts
        if ls "$BACKUP_DIR"/node-*-log.log 1> /dev/null 2>&1; then
            echo "  - Peer discovery attempts: $(grep -c "Attempting to connect to discovered peer" "$BACKUP_DIR"/node-*-log.log 2>/dev/null || echo "0")"
            echo "  - Successful neighbor additions: $(grep -c "Successfully added neighbor" "$BACKUP_DIR"/node-*-log.log 2>/dev/null || echo "0")"
        fi
        
        echo ""
        echo "To debug further:"
        echo "  - Check bootstrap server: cat bootstrap.log"
        echo "  - Check node logs: ls $BACKUP_DIR/"
        echo "  - Monitor peer discovery: grep 'Attempting to connect' $BACKUP_DIR/node-*-log.log"
        echo "  - Check neighbor connections: grep 'Successfully added neighbor' $BACKUP_DIR/node-*-log.log"
    fi
    
    # Clean up temporary logs (but keep configs as requested)
    echo "Cleaning up temporary logs..."
    rm -rf "$LOGS_DIR"
    rm -f "$PIDS_FILE"
    rm -f "bootstrap_address.txt"
    
    echo "All nodes and bootstrap server stopped"
    echo "Logs backed up for debugging - check the logs-backup-* directory"
    echo "Configuration files preserved in $CONFIGS_DIR/"
    exit 0
}

# Trap signals to cleanup
trap cleanup SIGINT SIGTERM

# Determine if log level was provided or defaulted
if [ $# -eq 4 ]; then
    LOG_LEVEL_STATUS="$LOG_LEVEL (provided)"
else
    LOG_LEVEL_STATUS="$LOG_LEVEL (default)"
fi

echo "Network parameters:"
echo "- Number of nodes: $NUM_NODES"
echo "- Max outbound degree: $MAX_OUTBOUND_DEGREE"
echo "- Diameter: $DIAMETER"
echo "- Log level: $LOG_LEVEL_STATUS"
echo ""

# Function to create committee-sampling config file
create_committee_config() {
    local config_file="$CONFIGS_DIR/committee-sampling-conf.json"
    
    cat > "$config_file" << EOF
{
  "network": {
    "listen_port": 0,
    "max_outbound_degree": $MAX_OUTBOUND_DEGREE,
    "discovery_config": {
      "protocol_id": "/committee-sampling/1.0.0",
      "interval": "5s",
      "bootstrap_peers": ["$BOOTSTRAP_ADDRESS"]
    }
  },
  "graph": {
    "diameter": $DIAMETER,
    "grading_levels": 5
  },
  "committee": {
    "session_id": "$SESSION_ID",
    "lambda": 256,
    "weight": 1,
    "delta_w": 10,
    "committee_size": 30,
    "delay": 20,
    "total_weight": $NUM_NODES
  },
  "synchronization": {
  "type": 0,
    "ex_ante_round_timeout": "30s",
    "ex_post_round_timeout": "30s",
    "mdag_round_timeout": "5s",
    "start_time": $(($(date +%s) + 60)),
    "building_graph_timeout": "2m",
    "time_server": "time.google.com"
  },
  "logger": {
    "level": "$LOG_LEVEL"
  },
  "metrics": {
    "enabled": false,
    "push_gateway": {
      "enabled": false,
      "url": "http://localhost:9091"
    },
    "http_server": {
      "enabled": false,
      "port": 0,
      "path": "/metrics"
    },

    "push_interval": "30s",
    "job_name": "committee-sampling-simulation",
    "instance_name": ""
  }
}
EOF
    
    echo "$config_file"
}

# Function to start a node
start_node() {
    local node_id=$1
    local config_file=$2
    local log_file="$LOGS_DIR/node-$node_id-log.log"
    
    echo "Starting node-$node_id with config: $config_file..."
    
    # Start the node in the background with -config flag
    "$BIN_DIR/committee-sampling" -config "$config_file" > "$log_file" 2>&1 &
    
    # Save PID
    local pid=$!
    echo "$pid" >> "$PIDS_FILE"
    
    echo "Node-$node_id started (PID: $pid, Port: auto-assigned, Log: $log_file)"
}

# Start bootstrap server first
echo "Starting DHT bootstrap server..."

# Start bootstrap server using the compiled binary and existing config
"$BIN_DIR/server" -config "$CONFIGS_DIR/dev-server.json" > bootstrap.log 2>&1 &
BOOTSTRAP_PID=$!
echo "$BOOTSTRAP_PID" > bootstrap_pid.txt

echo "Bootstrap server started (PID: $BOOTSTRAP_PID)"
echo "Waiting 10 seconds for bootstrap server to be ready..."
sleep 10

# Read the bootstrap address from the file
BOOTSTRAP_ADDRESS_FILE="./bootstrap_address.txt"
if [ -f "$BOOTSTRAP_ADDRESS_FILE" ]; then
    BOOTSTRAP_ADDRESS=$(cat "$BOOTSTRAP_ADDRESS_FILE")
    echo "Bootstrap address: $BOOTSTRAP_ADDRESS"
else
    echo "Failed to read bootstrap address from file"
    echo "Check bootstrap.log for the bootstrap address and update the script manually"
    exit 1
fi

# Create config file
echo ""
echo "Creating committee-sampling configuration..."
config_file=$(create_committee_config)

# Start all nodes
echo ""
echo "Starting nodes..."
for i in $(seq 1 $NUM_NODES); do
    start_node "$i" "$config_file"
done

echo ""
echo "All $NUM_NODES nodes started successfully!"
echo ""
echo "Simulation Status:"
echo "- Nodes: $NUM_NODES"
echo "- Max outbound degree: $MAX_OUTBOUND_DEGREE (provided)"
echo "- Diameter: $DIAMETER (provided)"
echo "- Log level: $LOG_LEVEL_STATUS"
echo "- Ports: Auto-assigned by system (port 0 configured)"
echo "- Session ID: $SESSION_ID"
echo "- Discovery: DHT with bootstrap server"
echo "- Log files: $LOGS_DIR/node-*-log.log"
echo "- Bootstrap log: bootstrap.log"
echo "- Committee config: $config_file"
echo "- Server config: $CONFIGS_DIR/dev-server.json"
echo ""
echo "Useful commands:"
echo "- Monitor node logs: tail -f $LOGS_DIR/node-1-log.log"
echo "- Monitor all node logs: tail -f $LOGS_DIR/*.log"
echo "- Monitor bootstrap server: tail -f bootstrap.log"
echo "- Check running processes: ps aux | grep committee-sampling"
echo "- Stop all nodes: Press Ctrl+C"
echo ""

# Monitor nodes
echo "Monitoring nodes (press Ctrl+C to stop all, or wait for automatic completion)..."
echo "Nodes will automatically stop when the committee-sampling simulation completes."
echo ""

consecutive_zero_counts=0
while true; do
    running_count=0
    if [ -f "$PIDS_FILE" ]; then
        while IFS= read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                running_count=$((running_count + 1))
            fi
        done < "$PIDS_FILE"
    fi
    
    echo "$(date '+%H:%M:%S') - Running nodes: $running_count/$NUM_NODES"
    
    # Check if all nodes have completed
    if [ "$running_count" -eq 0 ]; then
        consecutive_zero_counts=$((consecutive_zero_counts + 1))
        echo "$(date '+%H:%M:%S') - All nodes have stopped. Waiting 30 seconds to confirm completion..."
        
        # Wait for 30 seconds (3 checks) to confirm all nodes are really done
        if [ "$consecutive_zero_counts" -ge 3 ]; then
            echo ""
            echo "🎉 All committee-sampling nodes have completed successfully!"
            echo "Simulation finished at: $(date)"
            echo ""
            break
        fi
    else
        consecutive_zero_counts=0
    fi
    
    sleep 60
done

# Call cleanup to stop bootstrap server and create log backup
echo "Cleaning up and creating log backup..."
cleanup