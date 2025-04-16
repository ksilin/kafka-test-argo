#!/bin/bash
# Kafka Performance Testing Utilities
# Functions for running performance tests with Kafka producers and consumers

# Source common utilities
SCRIPT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
source "${SCRIPT_DIR}/kafka-test-utils.sh"

# Run a producer performance test
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
#   $4: Number of messages
#   $5: Message size in bytes (default: 1024)
#   $6: Throughput limit (-1 for unlimited)
#   $7: Output file (default: /tmp/producer-metrics.txt)
#   $8: Run in background (default: false)
# Returns:
#   Output file path if run in background, or 0 on success, 1 on failure
# Exports:
#   PRODUCER_THROUGHPUT - Throughput in records/sec
#   PRODUCER_AVG_LATENCY - Average latency in ms
#   PRODUCER_MAX_LATENCY - Maximum latency in ms
run_producer_test() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  local num_messages="$4"
  local message_size="${5:-1024}"
  local throughput="${6:--1}"
  local output_file="${7:-/tmp/producer-metrics.txt}"
  local run_bg="${8:-false}"
  
  log "Running producer performance test with ${num_messages} messages of size ${message_size} bytes"
  
  # Create command
  local cmd="kafka-producer-perf-test \
    --topic ${topic_name} \
    --num-records ${num_messages} \
    --record-size ${message_size} \
    --throughput ${throughput} \
    --producer.config \"${config_file}\" \
    --print-metrics"
  
  # Run in background or foreground
  if [ "$run_bg" = "true" ]; then
    eval "${cmd} > \"${output_file}\" 2>&1 &"
    # Return the output file path for later use - ensure it's the only output
    printf "%s" "${output_file}"
  else
    eval "${cmd} > \"${output_file}\""
    
    if [ $? -ne 0 ]; then
      log "ERROR: Producer test failed!"
      return 1
    fi
    
    # Parse the metrics
    parse_producer_metrics "${output_file}"
    
    log "Producer test completed successfully"
    return 0
  fi
}

# Parse producer metrics from output file
# Arguments:
#   $1: Output file path
# Returns:
#   0 on success, 1 on failure
# Exports:
#   PRODUCER_THROUGHPUT - Throughput in records/sec
#   PRODUCER_AVG_LATENCY - Average latency in ms
#   PRODUCER_MAX_LATENCY - Maximum latency in ms
parse_producer_metrics() {
  local output_file="$1"
  
  if [ ! -f "${output_file}" ]; then
    log "ERROR: Producer metrics file ${output_file} not found!"
    return 1
  fi
  
  # Check if the file contains completion markers
  if ! grep -q "records sent" "${output_file}" && ! grep -q "records/sec" "${output_file}"; then
    log "ERROR: Producer metrics file ${output_file} doesn't contain expected output!"
    return 1
  fi
  
  # Extract metrics
  export PRODUCER_THROUGHPUT=$(grep -o '[0-9]\+\.[0-9]\+ records/sec' "${output_file}" | grep -o '[0-9]\+\.[0-9]\+')
  export PRODUCER_AVG_LATENCY=$(grep -o '[0-9]\+\.[0-9]\+ ms avg latency' "${output_file}" | grep -o '[0-9]\+\.[0-9]\+')
  export PRODUCER_MAX_LATENCY=$(grep -o '[0-9]\+\.[0-9]\+ ms max latency' "${output_file}" | grep -o '[0-9]\+\.[0-9]\+')
  
  # Validate metrics
  if [ -z "${PRODUCER_THROUGHPUT}" ] || [ -z "${PRODUCER_AVG_LATENCY}" ] || [ -z "${PRODUCER_MAX_LATENCY}" ]; then
    log "WARNING: Some producer metrics could not be parsed from ${output_file}"
    # Set defaults for missing values
    PRODUCER_THROUGHPUT="${PRODUCER_THROUGHPUT:-0}"
    PRODUCER_AVG_LATENCY="${PRODUCER_AVG_LATENCY:-0}"
    PRODUCER_MAX_LATENCY="${PRODUCER_MAX_LATENCY:-0}"
  fi
  
  log "Producer throughput: ${PRODUCER_THROUGHPUT} records/sec"
  log "Producer avg latency: ${PRODUCER_AVG_LATENCY} ms"
  log "Producer max latency: ${PRODUCER_MAX_LATENCY} ms"
  
  return 0
}

# Run a consumer performance test
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
#   $4: Number of messages
#   $5: Output file (default: /tmp/consumer-metrics.txt)
# Returns:
#   0 on success, 1 on failure
# Exports:
#   CONSUMER_THROUGHPUT - Throughput in records/sec
#   CONSUMER_MB_SEC - Throughput in MB/sec
run_consumer_test() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  local num_messages="$4"
  local output_file="${5:-/tmp/consumer-metrics.txt}"
  
  log "Running consumer performance test for ${num_messages} messages"
  
  kafka-consumer-perf-test \
    --bootstrap-server "${bootstrap_servers}" \
    --consumer.config "${config_file}" \
    --topic "${topic_name}" \
    --messages "${num_messages}" > "${output_file} \
    --print-metrics
    "
  
  if [ $? -ne 0 ]; then
    log "ERROR: Consumer test failed!"
    return 1
  fi
  
  # Parse metrics
  parse_consumer_metrics "${output_file}"
  
  log "Consumer test completed successfully"
  return 0
}

# Parse consumer metrics from output file
# Arguments:
#   $1: Output file path
# Returns:
#   0 on success, 1 on failure
# Exports:
#   CONSUMER_THROUGHPUT - Throughput in records/sec
#   CONSUMER_MB_SEC - Throughput in MB/sec
parse_consumer_metrics() {
  local output_file="$1"
  
  if [ ! -f "${output_file}" ]; then
    log "ERROR: Consumer metrics file ${output_file} not found!"
    return 1
  fi
  
  # Extract metrics - these might need adjustment based on the exact output format
  export CONSUMER_THROUGHPUT=$(tail -n 1 "${output_file}" | cut -d',' -f6 | tr -d ' ')
  export CONSUMER_MB_SEC=$(tail -n 1 "${output_file}" | cut -d',' -f4 | tr -d ' ')
  
  # Validate metrics
  if [ -z "${CONSUMER_THROUGHPUT}" ] || [ -z "${CONSUMER_MB_SEC}" ]; then
    log "WARNING: Some consumer metrics could not be parsed from ${output_file}"
    # Set defaults for missing values
    CONSUMER_THROUGHPUT="${CONSUMER_THROUGHPUT:-0}"
    CONSUMER_MB_SEC="${CONSUMER_MB_SEC:-0}"
  fi
  
  log "Consumer throughput: ${CONSUMER_THROUGHPUT} records/sec"
  log "Consumer bandwidth: ${CONSUMER_MB_SEC} MB/sec"
  
  return 0
}

# Wait for producers to complete
# Arguments:
#   $1: Array of output file paths
#   $2: Timeout in seconds (default: 180)
#   $3: Message count (for adaptive timeout)
# Returns:
#   0 if all producers completed successfully, 1 otherwise
wait_for_producers() {
  # First argument might be an array of files or the timeout value
  # We need to extract the arguments properly
  local args=("$@")
  local output_files=()
  local timeout_base=180  # Base timeout (3 minutes)
  local message_count=1000
  local timeout_factor=1
  
  # Check if we have at least one argument (which should be a file path)
  if [ ${#args[@]} -gt 0 ]; then
    # Extract file paths (all arguments except potentially the last two)
    for ((i=0; i<${#args[@]}; i++)); do
      # If this is a valid file path, add it to our output_files array
      if [[ "${args[i]}" == *"/"* && "${args[i]}" == *".txt" ]]; then
        output_files+=("${args[i]}")
      fi
    done
    
    # The last two arguments may be timeout and message count if they're numeric
    if [[ "${args[${#args[@]}-1]}" =~ ^[0-9]+$ ]]; then
      message_count="${args[${#args[@]}-1]}"
      
      if [[ "${args[${#args[@]}-2]}" =~ ^[0-9]+$ ]]; then
        timeout_base="${args[${#args[@]}-2]}"
      fi
    fi
  fi
  
  # Safety check - make sure we have at least one valid file path
  if [ ${#output_files[@]} -eq 0 ]; then
    log "ERROR: No valid output file paths provided to wait_for_producers"
    return 1
  fi
  
  # Log the files we're waiting for
  log "Waiting for ${#output_files[@]} producer output files"
  
  # Scale timeout based on message count
  if (( message_count > 100000 )); then
    timeout_factor=5  # 15 minutes for very large message counts
  elif (( message_count > 10000 )); then
    timeout_factor=2  # 6 minutes for large message counts
  fi
  
  local timeout=$((timeout_base * timeout_factor))
  local start_time=$(date +%s)
  
  log "Waiting for producers to complete (timeout: ${timeout}s for ${message_count} messages)..."
  
  # Wait first for processes to complete
  wait
  
  log "Background processes completed, waiting for output file completion..."
  
  # Now wait for files to be fully written and contain completion markers
  local complete=false
  local wait_time=5

  for ((attempt=1; attempt<=20; attempt++)); do
    # Check if any files exist and watch their size
    local any_exists=false
    local all_complete=true
    local file_sizes=""
    
    for file in "${output_files[@]}"; do
      if [ -f "$file" ]; then
        any_exists=true
        local size=$(wc -c < "$file" 2>/dev/null || echo "0")
        file_sizes="$file_sizes $size"
        
        # Check if file contains completion markers
        if ! grep -q "records sent" "$file" 2>/dev/null && ! grep -q "records/sec" "$file" 2>/dev/null; then
          all_complete=false
        fi
      else
        all_complete=false
      fi
    done
    
    # Break if all files are complete
    if $all_complete && $any_exists; then
      log "All output files complete"
      complete=true
      break
    fi
    
    # Adaptive waiting - wait longer for more messages
    if (( message_count > 100000 )); then
      # For very large tests, give them more time (8-15 sec)
      wait_time=$((8 + (attempt / 2)))
    elif (( message_count > 10000 )); then
      # For larger tests, give them more time (5-10 sec)
      wait_time=$((5 + (attempt / 3)))
    else
      # For small tests (< 10K msgs), give 3-5 sec
      wait_time=$((3 + (attempt / 10)))
    fi
    
    log "Waiting for file completion (attempt $attempt, sizes:$file_sizes)..."
    sleep $wait_time
    
    # Check if we've exceeded the timeout
    local current_time=$(date +%s)
    local elapsed=$((current_time - start_time))
    if (( elapsed > timeout )); then
      log "WARNING: Timeout after ${elapsed}s waiting for completion"
      break
    fi
  done
  
  # Final status check
  local success_count=0
  local missing_count=0
  local incomplete_count=0
  
  for file in "${output_files[@]}"; do
    if [ -f "$file" ]; then
      if grep -q "records sent" "$file" 2>/dev/null || grep -q "records/sec" "$file" 2>/dev/null; then
        success_count=$((success_count + 1))
      else
        incomplete_count=$((incomplete_count + 1))
        log "WARNING: File $file exists but doesn't contain completion markers"
      fi
    else
      missing_count=$((missing_count + 1))
      log "WARNING: Output file $file not found"
    fi
  done
  
  log "Producer test status: $success_count complete, $missing_count missing, $incomplete_count incomplete"
  
  # Return success if at least one producer completed successfully
  if [ $success_count -gt 0 ]; then
    return 0
  else
    return 1
  fi
}

# If script is executed directly, print help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Kafka Performance Testing Utilities"
  echo "This script provides functions for running performance tests with Kafka producers and consumers."
  echo "It is intended to be sourced by other scripts."
  echo ""
  echo "Example usage:"
  echo "  source $(basename ${BASH_SOURCE[0]})"
  echo "  run_producer_test \"localhost:9092\" \"client.properties\" \"test-topic\" 10000 1024"
  echo "  run_consumer_test \"localhost:9092\" \"client.properties\" \"test-topic\" 10000"
  echo ""
fi