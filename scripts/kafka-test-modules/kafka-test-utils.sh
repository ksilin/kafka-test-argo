#!/bin/bash
# Kafka Test Utilities
# Common utilities for Kafka testing scripts

# Set this to true for debug output
DEBUG_MODE=${DEBUG_MODE:-false}

# Standardized logging function
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Debug logging (only prints if DEBUG_MODE is true)
debug_log() {
  if [ "$DEBUG_MODE" = "true" ]; then
    log "DEBUG: $1"
  fi
}

# Cross-platform millisecond timer
# Returns current time in milliseconds
init_timer() {
  # Determine the best method to get milliseconds based on the platform
  if date +%s%3N | grep -q N; then
    # System doesn't support %N format (likely macOS)
    get_time_ms() {
      # Alternative implementation for macOS
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
  debug_log "Timer initialized"
}

# Extract bootstrap servers from Kafka client config
# Arguments:
#   $1: Path to config file
# Returns:
#   Bootstrap servers string
parse_bootstrap_servers() {
  local config_file="$1"
  local bootstrap_servers=""
  
  if [ -f "$config_file" ]; then
    bootstrap_servers=$(grep 'bootstrap.servers' "$config_file" | cut -d'=' -f2 | tr -d '[:space:]')
    if [ -z "$bootstrap_servers" ]; then
      log "ERROR: No bootstrap.servers found in config file $config_file"
      return 1
    fi
  else
    log "ERROR: Config file $config_file not found"
    return 1
  fi
  
  echo "$bootstrap_servers"
}

# Simple arithmetic function to replace bc
# Arguments:
#   $1: Expression to evaluate (e.g. "5+3" or "10>5")
# Returns:
#   Result of the expression
calc() {
  local expression="$1"
  
  # Extract operation type
  if [[ "$expression" == *"+"* ]]; then
    local left="${expression%+*}"
    local right="${expression#*+}"
    echo "$(($left + $right))"
  elif [[ "$expression" == *"-"* ]]; then
    local left="${expression%-*}"
    local right="${expression#*-}"
    echo "$(($left - $right))"
  elif [[ "$expression" == *"/"* ]]; then
    local left="${expression%/*}"
    local right="${expression#*/}"
    # Integer division only
    echo "$(($left / $right))"
  elif [[ "$expression" == *">"* ]]; then
    local left="${expression%>*}"
    local right="${expression#*>}"
    if (( $(echo "$left" | sed 's/\..*//') > $(echo "$right" | sed 's/\..*//') )); then
      echo "1"
    else
      echo "0"
    fi
  elif [[ "$expression" == *"<"* ]]; then
    local left="${expression%<*}"
    local right="${expression#*<}"
    if (( $(echo "$left" | sed 's/\..*//') < $(echo "$right" | sed 's/\..*//') )); then
      echo "1"
    else
      echo "0"
    fi
  else
    # Default to just returning the value
    echo "$expression"
  fi
}

# Check if Kafka CLI tools are available
# Returns 0 if all tools are found, 1 otherwise
check_kafka_cli_tools() {
  local required_tools=("kafka-topics" "kafka-producer-perf-test" "kafka-consumer-perf-test")
  local missing_tools=()
  
  for tool in "${required_tools[@]}"; do
    if ! command -v "$tool" &> /dev/null; then
      missing_tools+=("$tool")
    fi
  done
  
  if [ ${#missing_tools[@]} -gt 0 ]; then
    log "ERROR: The following Kafka CLI tools are required but not found:"
    for tool in "${missing_tools[@]}"; do
      log "  - $tool"
    done
    log "Make sure they are in your PATH."
    log "You may need to run: export PATH=\$PATH:/path/to/confluent/bin"
    return 1
  fi
  
  return 0
}

# Create a results directory for test output
# Returns the path to the created directory
create_results_dir() {
  local base_dir="${1:-"."}"
  local dir_name="${2:-"results"}"
  local results_dir="${base_dir}/${dir_name}_$(date +%Y%m%d_%H%M%S)"
  
  mkdir -p "$results_dir"
  if [ $? -ne 0 ]; then
    log "ERROR: Failed to create results directory $results_dir"
    return 1
  fi
  
  echo "$results_dir"
}

# Initialize common utilities
init_common_utils() {
  # Initialize timer
  init_timer
  
  # Check for required tools
  check_kafka_cli_tools
  return $?
}

# If script is executed directly, print help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Kafka Test Utilities"
  echo "This script provides common utilities for Kafka testing and is intended to be sourced by other scripts."
  echo ""
  echo "Example usage:"
  echo "  source $(basename ${BASH_SOURCE[0]})"
  echo "  init_common_utils"
  echo "  bootstrap_servers=\$(parse_bootstrap_servers \"client.properties\")"
  echo "  start_time=\$(get_time_ms)"
  echo "  log \"Test started\""
  echo ""
fi