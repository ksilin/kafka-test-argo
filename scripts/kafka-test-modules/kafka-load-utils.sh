#!/bin/bash
# Kafka Load Testing Utilities
# Functions for load testing Kafka with increasing message volume

# Source common utilities
SCRIPT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
source "${SCRIPT_DIR}/kafka-test-utils.sh"
source "${SCRIPT_DIR}/kafka-perf-utils.sh"
source "${SCRIPT_DIR}/kafka-broker-monitor.sh"

# Calculate number of messages for a step in load testing
# Arguments:
#   $1: Step number (1-based)
#   $2: Maximum messages per producer (default: 1000000)
# Returns:
#   Number of messages for this step
calculate_messages_for_step() {
  local step="$1"
  local max_messages="${2:-1000000}"
  
  # Ensure inputs are numeric integers
  if ! [[ "$step" =~ ^[0-9]+$ ]]; then
    log "ERROR: Invalid step number: '$step'. Using default step 1."
    step=1
  fi
  
  if ! [[ "$max_messages" =~ ^[0-9]+$ ]]; then
    log "ERROR: Invalid max messages: '$max_messages'. Using default 1000000."
    max_messages=1000000
  fi
  
  # Calculate the message count using powers of 10
  # For steps 1-6, this will give: 1K -> 10K -> 100K -> 1M -> 10M -> 100M
  local messages=$((1000 * (10 ** (step - 1))))
  
  # Cap at MAX_MESSAGES if needed
  if (( messages > max_messages )); then
    messages=$max_messages
  fi
  
  echo $messages
}

# Check if broker is overloaded based on latency threshold
# Arguments:
#   $1: Average latency
#   $2: Latency threshold (default: 100)
# Returns:
#   0 (true) if broker is overloaded, 1 (false) otherwise
is_broker_overloaded() {
  local avg_latency="$1"
  local threshold="${2:-100}"
  
  # Validate threshold
  if ! [[ "$threshold" =~ ^[0-9]+$ ]]; then
    log "ERROR: Invalid threshold: '$threshold'. Using default 100."
    threshold=100
  fi
  
  # Convert to integers for comparison
  local avg_int
  
  # First handle completely non-numeric values
  if [ -z "$avg_latency" ]; then
    avg_int=0
    log "WARNING: Empty average latency provided. Treating as 0."
  elif [[ "$avg_latency" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    # Valid number format, strip decimal part
    avg_int=$(echo "$avg_latency" | sed 's/\..*//')
  else
    # Try to extract any numeric part
    avg_int=$(echo "$avg_latency" | grep -o '[0-9]\+' | head -1)
    if [ -z "$avg_int" ]; then
      avg_int=0
      log "WARNING: Non-numeric average latency: '$avg_latency'. Treating as 0."
    else
      log "WARNING: Partially numeric average latency: '$avg_latency'. Using extracted value: $avg_int"
    fi
  fi
  
  # Now do the comparison with safe values
  if [ $avg_int -gt $threshold ]; then
    log "Broker considered overloaded: latency $avg_int ms > threshold $threshold ms"
    return 0  # true in bash
  fi
  
  log "Broker not overloaded: latency $avg_int ms <= threshold $threshold ms"
  return 1  # false in bash
}

# Collect metrics from multiple producer test outputs
# Arguments:
#   $1: Step number
#   $2: Results directory
#   $3: Number of producers
# Exports:
#   PRODUCER_AVG_LATENCY - Average latency across all producers
#   PRODUCER_MAX_LATENCY - Maximum latency across all producers
#   PRODUCER_THROUGHPUT - Total throughput across all producers
collect_producer_metrics() {
  local step="$1"
  local results_dir="$2"
  local num_producers="$3"
  local avg_latency_sum=0
  local max_latency=0
  local throughput_sum=0
  local count=0
  
  for ((i=1; i<=$num_producers; i++)); do
    local output_file="${results_dir}/producer_${i}_step_${step}.txt"
    
    # Check if file exists
    if [ ! -f "$output_file" ]; then
      log "WARNING: Output file $output_file not found"
      continue
    fi
    
    # Check file content and size
    if [ ! -s "$output_file" ]; then
      log "WARNING: Output file $output_file is empty"
      continue
    fi
    
    # Process the producer metrics file
    parse_producer_metrics "$output_file"
    
    # Add to sums (with safety checks)
    if [ ! -z "$PRODUCER_AVG_LATENCY" ] && [ "$PRODUCER_AVG_LATENCY" != "0" ]; then
      # Use awk for floating point addition if available, otherwise use integer math
      if command -v awk >/dev/null 2>&1; then
        avg_latency_sum=$(awk "BEGIN {print $avg_latency_sum + $PRODUCER_AVG_LATENCY}")
      else
        # Convert to integer by removing decimal part
        local avg_int=$(echo "$PRODUCER_AVG_LATENCY" | sed 's/\..*//')
        local sum_int=$(echo "$avg_latency_sum" | sed 's/\..*//')
        avg_latency_sum=$((sum_int + avg_int))
      fi
    fi
    
    if [ ! -z "$PRODUCER_THROUGHPUT" ] && [ "$PRODUCER_THROUGHPUT" != "0" ]; then
      if command -v awk >/dev/null 2>&1; then
        throughput_sum=$(awk "BEGIN {print $throughput_sum + $PRODUCER_THROUGHPUT}")
      else
        # Convert to integer by removing decimal part
        local throughput_int=$(echo "$PRODUCER_THROUGHPUT" | sed 's/\..*//')
        local sum_int=$(echo "$throughput_sum" | sed 's/\..*//')
        throughput_sum=$((sum_int + throughput_int))
      fi
    fi
    
    # Update max latency if this one is higher
    if [ ! -z "$PRODUCER_MAX_LATENCY" ] && [ "$PRODUCER_MAX_LATENCY" != "0" ]; then
      # Compare integers - simple but effective
      local max_int=$(echo "$max_latency" | sed 's/\..*//')
      local prod_max_int=$(echo "$PRODUCER_MAX_LATENCY" | sed 's/\..*//')
      if [ $prod_max_int -gt $max_int ]; then
        max_latency=$PRODUCER_MAX_LATENCY
      fi
    fi
    
    count=$((count + 1))
  done
  
  # Calculate global averages
  if [ $count -gt 0 ]; then
    if command -v awk >/dev/null 2>&1; then
      PRODUCER_AVG_LATENCY=$(awk "BEGIN {printf \"%.2f\", $avg_latency_sum / $count}")
      PRODUCER_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $throughput_sum}")
    else
      # Integer division only
      PRODUCER_AVG_LATENCY=$((avg_latency_sum / count))
      PRODUCER_THROUGHPUT=$throughput_sum
    fi
    PRODUCER_MAX_LATENCY=$max_latency
  else
    # Default values if we couldn't get metrics
    log "WARNING: No valid metrics found for step $step. Using default values."
    PRODUCER_AVG_LATENCY=0
    PRODUCER_THROUGHPUT=0
    PRODUCER_MAX_LATENCY=0
  fi
  
  # Ensure we have valid numbers (not empty)
  PRODUCER_AVG_LATENCY=${PRODUCER_AVG_LATENCY:-0}
  PRODUCER_THROUGHPUT=${PRODUCER_THROUGHPUT:-0}
  PRODUCER_MAX_LATENCY=${PRODUCER_MAX_LATENCY:-0}
  
  log "Step $step - Producer metrics:"
  log "  Average latency: ${PRODUCER_AVG_LATENCY} ms"
  log "  Max latency: ${PRODUCER_MAX_LATENCY} ms"
  log "  Total throughput: ${PRODUCER_THROUGHPUT} records/sec"
}

# Run a multi-step load test with increasing message volumes
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
#   $4: Results directory
#   $5: Number of steps (default: 5)
#   $6: Number of producers (default: 1)
#   $7: Latency threshold (default: 100)
#   $8: Maximum messages per producer (default: 1000000)
#   $9: Metrics endpoint (optional, default: none)
#   $10: High-load config (true/false, default: false)
# Returns:
#   0 on success, 1 on failure
run_multi_step_test() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  local results_dir="$4"
  local num_steps="${5:-5}"
  local num_producers="${6:-1}"
  local latency_threshold="${7:-100}"
  local max_messages="${8:-1000000}"
  local metrics_endpoint="${9:-none}"
  local high_load_config="${10:-false}"
  
  log "Starting multi-step load test with $num_steps steps and $num_producers producer(s)"
  log "Results will be saved to $results_dir"
  
  # Export bootstrap servers for broker monitoring
  export BOOTSTRAP_SERVERS="$bootstrap_servers"
  
  # Initialize broker monitoring if metrics endpoint is provided
  if [ "$metrics_endpoint" != "none" ]; then
    init_broker_monitoring "$metrics_endpoint" 10
    start_broker_monitoring
  fi
  
  # Use high-load client config if requested
  local test_config_file="$config_file"
  if [ "$high_load_config" = "true" ]; then
    local high_load_config_file="${results_dir}/high_load_client.properties"
    generate_high_load_client_config "$config_file" "$high_load_config_file"
    test_config_file="$high_load_config_file"
    log "Using high-load client configuration: $test_config_file"
  fi
  
  # Create results CSV file
  local results_file="${results_dir}/results.csv"
  echo "step,messages_per_producer,total_messages,num_producers,avg_latency,max_latency,throughput,is_broker_overloaded" > "$results_file"
  
  # Initialize variables
  local broker_overloaded=false
  export current_step=0
  
  # Run load test with increasing load until broker is overloaded or we reach max steps
  while [[ $current_step -lt $num_steps && $broker_overloaded == false ]]; do
    current_step=$((current_step + 1))
    
    # Calculate message count for this step
    local messages_per_producer=$(calculate_messages_for_step $current_step $max_messages)
    local total_messages=$((messages_per_producer * num_producers))
    
    log "===== Step $current_step/$num_steps: $messages_per_producer messages per producer, $total_messages total ====="
    
    # Run producer test with specified number of producers
    local output_files=()
    for ((i=1; i<=$num_producers; i++)); do
      # Prepare the output file path
      local output_path="$results_dir/producer_${i}_step_${current_step}.txt"
      
      # Run the producer test in background
      local output_file=$(run_producer_test "$bootstrap_servers" "$test_config_file" "$topic_name" \
        "$messages_per_producer" "1024" "-1" "$output_path" "true")
      
      # Add to the output files array
      output_files+=("$output_path")
    done
    
    # Wait for all producers to complete
    wait_for_producers "${output_files[@]}" "300" "$messages_per_producer"
    
    # Collect and analyze metrics
    collect_producer_metrics "$current_step" "$results_dir" "$num_producers"
    
    # Check if broker is overloaded
    if is_broker_overloaded "$PRODUCER_AVG_LATENCY" "$latency_threshold"; then
      broker_overloaded=true
      log "BROKER OVERLOADED at step $current_step with $messages_per_producer messages per producer"
    fi
    
    # Record results
    echo "$current_step,$messages_per_producer,$total_messages,$num_producers,$PRODUCER_AVG_LATENCY,$PRODUCER_MAX_LATENCY,$PRODUCER_THROUGHPUT,$broker_overloaded" >> "$results_file"
    
    # Sleep between steps to let the system recover
    if [[ $broker_overloaded == false && $current_step -lt $num_steps ]]; then
      log "Waiting 10 seconds between steps..."
      sleep 10
    fi
  done
  
  if [[ $broker_overloaded == true ]]; then
    log "Test completed - broker overload detected at step $current_step"
  else
    log "Test completed - no broker overload detected after all steps"
  fi
  
  # Stop broker monitoring if it was started
  if [ "$metrics_endpoint" != "none" ]; then
    stop_broker_monitoring
    
    # Copy metrics summary to results directory if it exists
    if [ -f "${BROKER_METRICS_DIR}/metrics_summary.txt" ]; then
      cp "${BROKER_METRICS_DIR}/metrics_summary.txt" "${results_dir}/broker_metrics_summary.txt"
      log "Broker metrics summary copied to: ${results_dir}/broker_metrics_summary.txt"
    fi
  fi
  
  # Generate a test summary including client config if high-load config was used
  local summary_file="${results_dir}/test_summary.txt"
  {
    echo "# Load Test Summary"
    echo "# Generated: $(date)"
    echo ""
    echo "## Test Configuration"
    echo "- Bootstrap Servers: $bootstrap_servers"
    echo "- Topic: $topic_name"
    echo "- Steps: $num_steps"
    echo "- Producers: $num_producers"
    echo "- Latency Threshold: $latency_threshold ms"
    echo "- Maximum Messages: $max_messages per producer"
    echo "- High-Load Config: $high_load_config"
    echo ""
    
    if [ "$high_load_config" = "true" ]; then
      echo "## Client Configuration"
      echo "\`\`\`properties"
      cat "$test_config_file"
      echo "\`\`\`"
      echo ""
    fi
    
    echo "## Results Summary"
    echo "- Steps Completed: $current_step of $num_steps"
    echo "- Broker Overloaded: $broker_overloaded"
    if [ "$broker_overloaded" = "true" ]; then
      echo "- Overload Detected At: Step $current_step"
      echo "- Messages per Producer at Overload: $(calculate_messages_for_step $current_step $max_messages)"
      echo "- Average Latency at Overload: $PRODUCER_AVG_LATENCY ms"
      echo "- Max Latency at Overload: $PRODUCER_MAX_LATENCY ms"
      echo "- Throughput at Overload: $PRODUCER_THROUGHPUT records/sec"
    fi
    
    echo ""
    echo "## Results by Step"
    cat "$results_file" | sed 's/,/|/g' | sed 's/^/|/' | sed 's/$/|/'
  } > "$summary_file"
  
  log "Test summary generated: $summary_file"
  return 0
}

# If script is executed directly, print help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Kafka Load Testing Utilities"
  echo "This script provides functions for load testing Kafka with increasing message volume."
  echo "It is intended to be sourced by other scripts."
  echo ""
  echo "Example usage:"
  echo "  source $(basename ${BASH_SOURCE[0]})"
  echo "  run_multi_step_test \"localhost:9092\" \"client.properties\" \"test-topic\" \"./results\" 5 1 100 1000000"
  echo ""
fi