#!/bin/bash
# Modular Kafka Test Script
# This script provides a unified interface to both basic validation tests and load tests

# Determine script location more reliably
SCRIPT_PATH="${BASH_SOURCE[0]}"
while [ -L "${SCRIPT_PATH}" ]; do # resolve any symlinks
  SCRIPT_DIR="$(cd -P "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"
  SCRIPT_PATH="$(readlink "${SCRIPT_PATH}")"
  [[ ${SCRIPT_PATH} != /* ]] && SCRIPT_PATH="${SCRIPT_DIR}/${SCRIPT_PATH}"
done
SCRIPT_DIR="$(cd -P "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"
MODULE_DIR="${SCRIPT_DIR}/kafka-test-modules"

# Check if modules directory exists
if [[ ! -d "${MODULE_DIR}" ]]; then
  echo "ERROR: Module directory not found at ${MODULE_DIR}"
  exit 1
fi

# Output debug information
echo "Script location: ${SCRIPT_PATH}"
echo "Script directory: ${SCRIPT_DIR}"
echo "Module directory: ${MODULE_DIR}"

# Source all required modules
source "${MODULE_DIR}/kafka-test-utils.sh"
source "${MODULE_DIR}/kafka-topic-utils.sh"
source "${MODULE_DIR}/kafka-perf-utils.sh"
source "${MODULE_DIR}/kafka-load-utils.sh"
source "${MODULE_DIR}/kafka-report-utils.sh"

# Test modes
MODE_VALIDATION="validation"
MODE_LOAD="load"

# Default parameters
MODE="${MODE_VALIDATION}"
CONFIG_FILE=""
TOPIC_NAME="kafka-test-topic"
NUM_PRODUCERS=1
NUM_STEPS=5
LATENCY_THRESHOLD=100  # ms
MAX_MESSAGES=1000000
MESSAGE_SIZE=1024  # bytes
TOPIC_PARTITIONS=3
TOPIC_REPLICATION_FACTOR=3
KEEP_TOPIC=false
VERBOSE=false
METRICS_ENDPOINT="none"
HIGH_LOAD_CONFIG=false

function show_usage {
    echo "Usage: $0 --mode <validation|load> --config <client_config_file> [options]"
    echo ""
    echo "Modes:"
    echo "  --mode validation     Run a basic validation test (producer & consumer)"
    echo "  --mode load           Run a load test with increasing message volume"
    echo ""
    echo "Required Parameters:"
    echo "  -c, --config FILE     Client config file to pass to the Kafka CLI tools"
    echo ""
    echo "Common Options:"
    echo "  -t, --topic NAME      Topic to use (default: kafka-test-topic)"
    echo "  -s, --size SIZE       Message size in bytes (default: 1024)"
    echo "  -p, --partitions NUM  Number of partitions (default: 3)"
    echo "  -r, --replication NUM Replication factor (default: 3)"
    echo "  -k, --keep-topic      Keep the topic after the test completes (default: false)"
    echo "  -v, --verbose         Enable verbose output"
    echo "  -h, --help            Display this help message"
    echo ""
    echo "Load Test Options:"
    echo "  --producers NUM       Number of producer instances to start (default: 1)"
    echo "  --steps NUM           Number of load steps to try (default: 5)"
    echo "  --latency THRESHOLD   Latency threshold in ms to identify broker overload (default: 100)"
    echo "  --max-messages NUM    Maximum number of messages per producer in the final step (default: 1000000)"
    echo "  --metrics URL         URL to fetch broker metrics (format: http://host:port/metrics)"
    echo "  --high-load-config    Use optimized client settings for high-load scenarios (default: false)"
    echo ""
    echo "Examples:"
    echo "  $0 --mode validation --config client.properties --topic test-topic"
    echo "  $0 --mode load --config client.properties --topic load-test --producers 4 --steps 8 --latency 150"
    echo "  $0 --mode load --config client.properties --topic load-test --high-load-config --metrics http://kafka:8080/metrics"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            shift 2
            ;;
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -t|--topic)
            TOPIC_NAME="$2"
            shift 2
            ;;
        -s|--size)
            MESSAGE_SIZE="$2"
            shift 2
            ;;
        -p|--partitions)
            TOPIC_PARTITIONS="$2"
            shift 2
            ;;
        -r|--replication)
            TOPIC_REPLICATION_FACTOR="$2"
            shift 2
            ;;
        --producers)
            NUM_PRODUCERS="$2"
            shift 2
            ;;
        --steps)
            NUM_STEPS="$2"
            shift 2
            ;;
        --latency)
            LATENCY_THRESHOLD="$2"
            shift 2
            ;;
        --max-messages)
            MAX_MESSAGES="$2"
            shift 2
            ;;
        --metrics)
            METRICS_ENDPOINT="$2"
            shift 2
            ;;
        --high-load-config)
            HIGH_LOAD_CONFIG=true
            shift
            ;;
        -k|--keep-topic)
            KEEP_TOPIC=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            export DEBUG_MODE=true
            shift
            ;;
        -h|--help)
            show_usage
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            ;;
    esac
done

# Check for required parameters
if [[ -z "$CONFIG_FILE" ]]; then
    echo "Error: Missing required parameter: config file."
    show_usage
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Config file '$CONFIG_FILE' not found."
    exit 1
fi

if [[ "$MODE" != "$MODE_VALIDATION" && "$MODE" != "$MODE_LOAD" ]]; then
    echo "Error: Invalid mode. Must be 'validation' or 'load'."
    show_usage
fi

# Initialize utilities
if ! type init_common_utils &>/dev/null; then
  echo "ERROR: Module functions not loaded properly. Check that all modules are available."
  exit 1
fi

init_common_utils || exit 1

# Extract bootstrap servers from config
BOOTSTRAP_SERVERS=$(parse_bootstrap_servers "$CONFIG_FILE")
if [ $? -ne 0 ]; then
    echo "Error: Could not extract bootstrap servers from config file."
    exit 1
fi

echo "Client options: $CONFIG_FILE" 
cat $CONFIG_FILE
echo ""
echo "Bootstrap server: $BOOTSTRAP_SERVERS"

# Create results directory
RESULTS_DIR=$(create_results_dir "." "kafka_test_${MODE}")
if [ $? -ne 0 ]; then
    echo "Error: Could not create results directory."
    exit 1
fi

run_validation_test() {
    local start_time=$(get_time_ms)
    log "Starting Kafka validation test"
    
    # Create topic
    create_topic "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "$TOPIC_PARTITIONS" "$TOPIC_REPLICATION_FACTOR" || return 1
    
    # Define number of messages based on message size to keep test reasonable
    local num_messages=10000
    if [ $MESSAGE_SIZE -gt 10000 ]; then
        num_messages=1000
    elif [ $MESSAGE_SIZE -gt 1000 ]; then
        num_messages=5000
    fi
    
    # Run producer test
    run_producer_test "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "$num_messages" "$MESSAGE_SIZE" "-1" "$RESULTS_DIR/producer_test.txt" || return 1
    
    # Run consumer test
    run_consumer_test "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "$num_messages" "$RESULTS_DIR/consumer_test.txt" || return 1
    
    # Delete topic if not keeping it
    if [ "$KEEP_TOPIC" != "true" ]; then
        delete_topic "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "false" || log "Warning: Topic deletion failed"
    else
        log "Keeping topic ${TOPIC_NAME} as requested"
    fi
    
    # Calculate total time
    local end_time=$(get_time_ms)
    local total_time=$((end_time - start_time))
    log "Total test time: $((total_time / 1000)) seconds"
    
    # Generate summary
    log "Validation Test Summary:"
    log "  Topic: $TOPIC_NAME"
    log "  Messages: $num_messages"
    log "  Message Size: $MESSAGE_SIZE bytes"
    log "  Producer Throughput: $PRODUCER_THROUGHPUT records/sec"
    log "  Producer Avg Latency: $PRODUCER_AVG_LATENCY ms"
    log "  Consumer Throughput: $CONSUMER_THROUGHPUT records/sec"
    
    log "Test completed successfully"
    log "Results saved to: $RESULTS_DIR"
    return 0
}

run_load_test() {
    local start_time=$(get_time_ms)
    log "Starting Kafka load testing"
    
    # Create topic
    create_topic "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "$TOPIC_PARTITIONS" "$TOPIC_REPLICATION_FACTOR" || return 1
    
    # Run multi-step load test
    run_multi_step_test "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "$RESULTS_DIR" \
        "$NUM_STEPS" "$NUM_PRODUCERS" "$LATENCY_THRESHOLD" "$MAX_MESSAGES" \
        "$METRICS_ENDPOINT" "$HIGH_LOAD_CONFIG" || return 1
    
    # Delete topic if not keeping it
    if [ "$KEEP_TOPIC" != "true" ]; then
        delete_topic "$BOOTSTRAP_SERVERS" "$CONFIG_FILE" "$TOPIC_NAME" "false" || log "Warning: Topic deletion failed"
    else
        log "Keeping topic ${TOPIC_NAME} as requested"
    fi
    
    # Generate report
    generate_report "$RESULTS_DIR" "$TOPIC_NAME" "$NUM_PRODUCERS" "$MESSAGE_SIZE" \
        "$TOPIC_PARTITIONS" "$TOPIC_REPLICATION_FACTOR" "$LATENCY_THRESHOLD"
    
    # Calculate total time
    local end_time=$(get_time_ms)
    local total_time=$((end_time - start_time))
    log "Total test time: $((total_time / 1000)) seconds"
    
    log "Load test completed successfully"
    log "Results saved to: $RESULTS_DIR"
    log "Report generated: $RESULTS_DIR/load_test_report.txt"
    return 0
}

# Main function
main() {
    if [ "$MODE" = "$MODE_VALIDATION" ]; then
        run_validation_test
    elif [ "$MODE" = "$MODE_LOAD" ]; then
        run_load_test
    fi
    
    return $?
}

# Run main function
main
exit $?