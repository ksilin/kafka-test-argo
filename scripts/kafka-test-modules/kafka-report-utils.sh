#!/bin/bash
# Kafka Testing Report Utilities
# Functions for generating reports from Kafka performance/load tests

# Source common utilities
SCRIPT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
source "${SCRIPT_DIR}/kafka-test-utils.sh"

# Generate a basic test report
# Arguments:
#   $1: Results directory
#   $2: Topic name
#   $3: Number of producers
#   $4: Message size
#   $5: Partitions
#   $6: Replication factor
#   $7: Latency threshold
#   $8: Report file name (default: "load_test_report.txt")
# Returns:
#   0 on success, 1 on failure
generate_report() {
  local results_dir="$1"
  local topic_name="$2"
  local num_producers="$3"
  local message_size="$4"
  local partitions="$5"
  local replication="$6"
  local latency_threshold="$7"
  local report_file="${8:-${results_dir}/load_test_report.txt}"
  
  log "Generating report in $report_file..."
  
  # Check if results CSV exists
  local results_file="${results_dir}/results.csv"
  if [ ! -f "$results_file" ]; then
    log "ERROR: Results file $results_file not found!"
    return 1
  fi
  
  # Create the report
  echo "Kafka Load Test Report" > "$report_file"
  echo "======================" >> "$report_file"
  echo "" >> "$report_file"
  echo "Test Parameters:" >> "$report_file"
  echo "  Topic: $topic_name" >> "$report_file"
  echo "  Producers: $num_producers" >> "$report_file"
  echo "  Message Size: $message_size bytes" >> "$report_file"
  echo "  Partitions: $partitions" >> "$report_file"
  echo "  Replication Factor: $replication" >> "$report_file"
  echo "  Latency Threshold: $latency_threshold ms" >> "$report_file"
  echo "" >> "$report_file"
  echo "Results Summary:" >> "$report_file"
  
  # Add a summary of each step
  while IFS=, read -r step messages_per_producer total_messages num_producers avg_latency max_latency throughput is_overloaded; do
    # Skip header
    if [[ "$step" == "step" ]]; then continue; fi
    
    echo "  Step $step:" >> "$report_file"
    echo "    Messages per producer: $messages_per_producer" >> "$report_file"
    echo "    Total messages: $total_messages" >> "$report_file"
    echo "    Avg latency: $avg_latency ms" >> "$report_file"
    echo "    Max latency: $max_latency ms" >> "$report_file"
    echo "    Throughput: $throughput records/sec" >> "$report_file"
    echo "    Broker overloaded: $is_overloaded" >> "$report_file"
    echo "" >> "$report_file"
  done < "$results_file"
  
  if grep -q "true" "$results_file"; then
    # Find at which step the broker got overloaded
    local overload_step=$(grep "true" "$results_file" | head -1 | cut -d',' -f1)
    local overload_msgs=$(grep "true" "$results_file" | head -1 | cut -d',' -f2)
    local overload_throughput=$(grep "true" "$results_file" | head -1 | cut -d',' -f7)
    
    echo "Broker Overload Detection:" >> "$report_file"
    echo "  Detected at step: $overload_step" >> "$report_file"
    echo "  Messages per producer: $overload_msgs" >> "$report_file"
    echo "  Total throughput at overload: $overload_throughput records/sec" >> "$report_file"
    
    # If we have previous step data, calculate the estimated max capacity
    if [[ $overload_step -gt 1 ]]; then
      local prev_step=$((overload_step - 1))
      local prev_throughput=$(grep "^$prev_step," "$results_file" | cut -d',' -f7)
      
      echo "  Estimated safe throughput: $prev_throughput records/sec" >> "$report_file"
    fi
  else
    echo "No broker overload detected during this test run." >> "$report_file"
    echo "Consider increasing the number of steps, messages, or producers to find the overload point." >> "$report_file"
  fi
  
  log "Report generated: $report_file"
  return 0
}

# Generate a master report for multi-instance testing
# Arguments:
#   $1: Root results directory
#   $2: Report file name (default: "master_report.txt")
# Returns:
#   0 on success, 1 on failure
generate_master_report() {
  local results_dir="$1"
  local report_file="${2:-${results_dir}/master_report.txt}"
  
  log "Generating master report in $report_file..."
  
  echo "Kafka Multi-Instance Load Test Master Report" > "$report_file"
  echo "==========================================" >> "$report_file"
  echo "" >> "$report_file"
  
  # Find all test directories
  for dir in "$results_dir"/producers_*; do
    if [ -d "$dir" ]; then
      local num_producers=$(basename "$dir" | cut -d'_' -f2)
      echo "Test with $num_producers producers:" >> "$report_file"
      
      local results_file="$dir/results.csv"
      if [ -f "$results_file" ]; then
        # Find the highest throughput achieved
        local max_throughput=$(tail -n +2 "$results_file" | cut -d',' -f7 | sort -n | tail -1)
        echo "  Max throughput: $max_throughput records/sec" >> "$report_file"
        
        # Check if broker was overloaded
        if grep -q "true" "$results_file"; then
          local overload_step=$(grep "true" "$results_file" | head -1 | cut -d',' -f1)
          local overload_msgs=$(grep "true" "$results_file" | head -1 | cut -d',' -f2)
          echo "  Broker overloaded at step $overload_step with $overload_msgs messages per producer" >> "$report_file"
          
          # Calculate the estimated max capacity from previous step
          if [[ $overload_step -gt 1 ]]; then
            local prev_step=$((overload_step - 1))
            local prev_throughput=$(grep "^$prev_step," "$results_file" | cut -d',' -f7)
            echo "  Estimated safe throughput: $prev_throughput records/sec" >> "$report_file"
          fi
        else
          echo "  No broker overload detected" >> "$report_file"
        fi
      fi
      echo "" >> "$report_file"
    fi
  done
  
  # Determine if we found the producer-limited or broker-limited case
  echo "Test Summary:" >> "$report_file"
  if grep -q "No broker overload detected" "$report_file"; then
    if grep -q "Broker overloaded" "$report_file"; then
      echo "  Found both producer-limited and broker-limited scenarios." >> "$report_file"
      echo "  The broker overload threshold was reached when using multiple producer instances." >> "$report_file"
    else
      echo "  All tests were producer-limited. The broker was not overloaded." >> "$report_file"
      echo "  Consider using more producer instances or larger message counts to find the broker limit." >> "$report_file"
    fi
  else
    echo "  All tests reached broker overload. The system is broker-limited." >> "$report_file"
  fi
  
  log "Master report generated: $report_file"
  return 0
}

# Export test results to CSV format
# Arguments:
#   $1: Results directory 
#   $2: Output CSV file name
#   $3: Include raw data (default: false)
# Returns:
#   0 on success, 1 on failure
export_csv() {
  local results_dir="$1"
  local output_file="$2"
  local include_raw="${3:-false}"
  
  log "Exporting results to CSV: $output_file"
  
  # Check if results exists
  local results_file="${results_dir}/results.csv"
  if [ ! -f "$results_file" ]; then
    log "ERROR: Results file $results_file not found!"
    return 1
  fi
  
  # Copy the results to the output file
  cp "$results_file" "$output_file"
  
  # Add additional data if requested
  if [ "$include_raw" = "true" ]; then
    # Add headers for raw data
    echo "" >> "$output_file"
    echo "Raw Data:" >> "$output_file"
    
    # Find all producer output files and add their contents
    for file in "$results_dir"/producer_*_step_*.txt; do
      if [ -f "$file" ]; then
        local basename=$(basename "$file")
        echo "" >> "$output_file"
        echo "File: $basename" >> "$output_file"
        echo "----------------------------" >> "$output_file"
        cat "$file" >> "$output_file"
      fi
    done
  fi
  
  log "CSV export completed: $output_file"
  return 0
}

# If script is executed directly, print help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Kafka Testing Report Utilities"
  echo "This script provides functions for generating reports from Kafka performance/load tests."
  echo "It is intended to be sourced by other scripts."
  echo ""
  echo "Example usage:"
  echo "  source $(basename ${BASH_SOURCE[0]})"
  echo "  generate_report \"./results\" \"test-topic\" 1 1024 3 3 100"
  echo "  export_csv \"./results\" \"./results/export.csv\""
  echo ""
fi