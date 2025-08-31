#!/bin/bash

# Script to start 20 committee-sampling nodes for testing
# Each node will have its own configuration and log file

set -e

# Configuration
NUM_NODES=30
BASE_PORT=8000
SESSION_ID="test-session-$(date +%s)"
LOGS_DIR="./logs"
CONFIGS_DIR="./test-configs"
PIDS_FILE="./node_pids.txt"

echo "🚀 Starting $NUM_NODES committee-sampling nodes..."
echo "Session ID: $SESSION_ID"
echo "Base port: $BASE_PORT"
echo "Logs directory: $LOGS_DIR"
echo "Configs directory: $CONFIGS_DIR"
echo ""

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
    
    # Clean up temporary configs and logs
    echo "Cleaning up temporary configurations and logs..."
    rm -rf "$CONFIGS_DIR"
    rm -rf "$LOGS_DIR"
    rm -f "$PIDS_FILE"
    rm -f "$BOOTSTRAP_SERVER_DIR/bootstrap_address.txt"
    
    echo "All nodes and bootstrap server stopped and cleaned up"
    echo "Logs backed up for debugging - check the logs-backup-* directory"
    exit 0
}

# Trap signals to cleanup
trap cleanup SIGINT SIGTERM

# Function to create config file
create_config() {
    local config_file="$CONFIGS_DIR/test-config.json"
    
    cat > "$config_file" << EOF
{
  "network": {
    "listen_port": 0,
    "max_outbound_degree": 4,
          "discovery_config": {
        "protocol_id": "/committee-sampling/1.0.0",
        "interval": "5s",
        "bootstrap_peers": ["$BOOTSTRAP_ADDRESS"]
      }
  },
  "graph": {
    "diameter": 4,
    "grading_levels": 5
  },
  "committee": {
    "session_id": "$SESSION_ID",
    "lambda": 384,
    "weight": 10,
    "delta_w": 3.0,
    "committee_size": 4,
    "delay": 20,
    "total_weight": $NUM_NODES
  },
  "synchronization": {
    "type": 0,
    "ex_ante_round_timeout": "20s",
    "ex_post_round_timeout": "20s",
    "mdag_round_timeout": "10s",
    "start_time": $(($(date +%s) + 60)),
    "building_graph_timeout": "1m",
    "time_server": "time.google.com"
  },
  "logger": {
    "level": "info"
  },
  "metrics": {
    "enabled": true,
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
    go run ./cmd/committee-sampling -config "$config_file" > "$log_file" 2>&1 &
    
    # Save PID
    local pid=$!
    echo "$pid" >> "$PIDS_FILE"
    
    echo "Node-$node_id started (PID: $pid, Port: auto-assigned, Log: $log_file)"
}

# Start bootstrap server first
echo "Starting DHT bootstrap server..."
BOOTSTRAP_SERVER_DIR="../committee-sampling-server"
if [ ! -d "$BOOTSTRAP_SERVER_DIR" ]; then
    echo "Error: Bootstrap server not found at $BOOTSTRAP_SERVER_DIR"
    echo "Please ensure committee-sampling-server is in the parent directory"
    exit 1
fi

# Build and start bootstrap server
(cd "$BOOTSTRAP_SERVER_DIR" && make build > /dev/null 2>&1)
(cd "$BOOTSTRAP_SERVER_DIR" && ./bin/server > bootstrap.log 2>&1) &
BOOTSTRAP_PID=$!
echo "$BOOTSTRAP_PID" > bootstrap_pid.txt

echo "Bootstrap server started (PID: $BOOTSTRAP_PID)"
echo "Waiting 10 seconds for bootstrap server to be ready..."
sleep 10

# Read the bootstrap address from the file
BOOTSTRAP_ADDRESS_FILE="$BOOTSTRAP_SERVER_DIR/bootstrap_address.txt"
if [ -f "$BOOTSTRAP_ADDRESS_FILE" ]; then
    BOOTSTRAP_ADDRESS=$(cat "$BOOTSTRAP_ADDRESS_FILE")
    echo "Bootstrap address: $BOOTSTRAP_ADDRESS"
else
    echo "Failed to read bootstrap address from file"
    echo "Check bootstrap.log for the bootstrap address and update the script manually"
    exit 1
fi

# Build the application
echo "Building committee-sampling application..."
make -f MakeFile build

# Create config file
echo ""
echo "Creating configuration..."
config_file=$(create_config)

# Start all nodes
echo ""
echo "Starting nodes..."
for i in $(seq 1 $NUM_NODES); do
    start_node "$i" "$config_file"
done

echo ""
echo "All $NUM_NODES nodes started successfully!"
echo ""
echo "Node Status:"
echo "- Nodes: $NUM_NODES"
echo "- Ports: Auto-assigned by system (port 0 configured)"
echo "- Session ID: $SESSION_ID"
echo "- Discovery: DHT with bootstrap server on port 4001"
echo "- Log files: $LOGS_DIR/node-*-log.log"
echo "- Bootstrap log: bootstrap.log"
echo "- Config file: $config_file"
echo ""
echo "Useful commands:"
echo "- Monitor node logs: tail -f $LOGS_DIR/node-1-log.log"
echo "- Monitor all node logs: tail -f $LOGS_DIR/*.log"
echo "- Monitor bootstrap server: tail -f bootstrap.log"

echo "- Check running processes: ps aux | grep committee-sampling"
echo "- Stop all nodes: Press Ctrl+C"
echo ""

# Monitor nodes
echo "Monitoring nodes (press Ctrl+C to stop all)..."
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
    sleep 10
done 