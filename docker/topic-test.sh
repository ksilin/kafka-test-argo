#!/bin/bash

function show_usage {
    echo "Usage: $0 -c | --config <client_config_file> [-t TOPIC]"
    echo ""
    echo "Parameters:"
    echo "  -c, --config  Client config file to pass to the CLI tools"
    echo "  -t, --topic             Topic to use"
    echo ""
    echo "Example: $0 -c myfile.properties -t test-topic"
    exit 1
}

CONFIG_FILE=""
TOPIC_NAME=""


while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -t|--topic)
            TOPIC_NAME="$2"
            shift 2
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
if [[ -z "$CONFIG_FILE" || -z "$TOPIC_NAME" ]]; then
    echo "Error: Missing required parameters."
    show_usage
fi

echo "Client options: $CONFIG_FILE" 
cat $CONFIG_FILE
echo ""

TOPIC_NAME=${TOPIC_NAME:-"test-topic-$(date +%s)"}
NUM_MESSAGES=${NUM_MESSAGES:-10000}
MESSAGE_SIZE=${MESSAGE_SIZE:-1024}
TOPIC_PARTITIONS=${TOPIC_PARTITIONS:-3}
TOPIC_REPLICATION_FACTOR=${TOPIC_REPLICATION_FACTOR:-3}

BOOTSTRAP_SERVERS=$(cat $CONFIG_FILE | grep 'bootstrap.servers' | cut -d'=' -f2 | tr -d '[:space:]')

echo "Bootstrap server: $BOOTSTRAP_SERVERS"

# Set up logging
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# set up timers
if date +%s%3N | grep -q N; then
  # System doesn't support %N format (likely macOS)
  get_time_ms() {
    # Alternative implementation that works on macOS
    # Get seconds and add Python-calculated milliseconds
    date_s=$(date +%s)
    ms=$(python -c 'import time; print(int(round(time.time() * 1000)) % 1000)')
    echo "$((date_s * 1000 + ms))"
  }
else
  # System supports %N format (likely Linux)
  get_time_ms() {
    date +%s%3N
  }
fi


create_topic() {
  log "Creating topic ${TOPIC_NAME}"
  topic_start=$(get_time_ms)
  
  kafka-topics --bootstrap-server ${BOOTSTRAP_SERVERS} --command-config "$CONFIG_FILE" \
    --create --topic ${TOPIC_NAME} \
    --partitions ${TOPIC_PARTITIONS} \
    --replication-factor ${TOPIC_REPLICATION_FACTOR}
  
  if [ $? -ne 0 ]; then
    log "ERROR: Topic creation failed!"
    return 1
  fi
  
  topic_end=$(get_time_ms)
  export TOPIC_CREATE_TIME=$((topic_end - topic_start))
  log "Topic creation took ${TOPIC_CREATE_TIME}ms"
}

run_producer_test() {
  log "Running producer performance test with ${NUM_MESSAGES} messages of size ${MESSAGE_SIZE} bytes"
  
  kafka-producer-perf-test \
    --topic ${TOPIC_NAME} \
    --num-records ${NUM_MESSAGES} \
    --record-size ${MESSAGE_SIZE} \
    --throughput -1 \
    --producer.config "$CONFIG_FILE" > /tmp/producer-metrics.txt
  
  if [ $? -ne 0 ]; then
    log "ERROR: Producer test failed!"
    return 1
  fi
  
  log "Producer test completed"
  # Extract key metrics from output
  export PRODUCER_THROUGHPUT=$(grep -oP "records/sec \(\d+\.\d+\)" /tmp/producer-metrics.txt | grep -oP "\d+\.\d+")
  export PRODUCER_AVG_LATENCY=$(grep -oP "ms avg latency" /tmp/producer-metrics.txt | grep -oP "\d+\.\d+")
  export PRODUCER_MAX_LATENCY=$(grep -oP "ms max latency" /tmp/producer-metrics.txt | grep -oP "\d+\.\d+")
  
  log "Producer throughput: ${PRODUCER_THROUGHPUT} records/sec"
  log "Producer avg latency: ${PRODUCER_AVG_LATENCY} ms"
  log "Producer max latency: ${PRODUCER_MAX_LATENCY} ms"
}

# Function to run consumer performance test
run_consumer_test() {
  log "Running consumer performance test for ${NUM_MESSAGES} messages"
  
  kafka-consumer-perf-test \
    --bootstrap-server ${BOOTSTRAP_SERVERS} \
    --consumer.config "$CONFIG_FILE" \
    --topic ${TOPIC_NAME} \
    --messages ${NUM_MESSAGES} > /tmp/consumer-metrics.txt
  
  if [ $? -ne 0 ]; then
    log "ERROR: Consumer test failed!"
    return 1
  fi
  
  log "Consumer test completed"
  # Extract key metrics from output
  export CONSUMER_THROUGHPUT=$(grep -oP "nMsg/sec: \d+\.\d+" /tmp/consumer-metrics.txt | grep -oP "\d+\.\d+")
  export CONSUMER_MB_SEC=$(grep -oP "MB/sec: \d+\.\d+" /tmp/consumer-metrics.txt | grep -oP "\d+\.\d+")
  
  log "Consumer throughput: ${CONSUMER_THROUGHPUT} records/sec"
  log "Consumer bandwidth: ${CONSUMER_MB_SEC} MB/sec"
}

delete_topic() {
  log "Deleting topic ${TOPIC_NAME}"
  delete_start=$(get_time_ms)
  
  kafka-topics --bootstrap-server ${BOOTSTRAP_SERVERS} --command-config "$CONFIG_FILE" \
    --delete --topic ${TOPIC_NAME}
  
  if [ $? -ne 0 ]; then
    log "ERROR: Topic deletion failed!"
    return 1
  fi
  
  delete_end=$(get_time_ms)
  export TOPIC_DELETE_TIME=$((delete_end - delete_start))
  log "Topic deletion took ${TOPIC_DELETE_TIME}ms"
}


main() {
  log "Starting Kafka validation test"
  start_time=$(get_time_ms)
  
  # Run tests
  create_topic || return 1
  run_producer_test || return 1
  run_consumer_test || return 1
  delete_topic || return 1
  
  # Calculate total time
  end_time=$(get_time_ms)
  export TOTAL_TIME=$((end_time - start_time))
  log "Total test time: ${TOTAL_TIME} seconds"
  
  # Generate final report
  # generate_report
  
  log "Test completed successfully"
  return 0
}

main
exit $?
