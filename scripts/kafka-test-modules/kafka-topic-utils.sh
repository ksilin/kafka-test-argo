#!/bin/bash
# Kafka Topic Management Utilities
# Functions for creating, deleting, and managing Kafka topics

# Source common utilities
SCRIPT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
source "${SCRIPT_DIR}/kafka-test-utils.sh"

# Check if a topic exists
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
# Returns:
#   0 if topic exists, 1 otherwise
check_topic_exists() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  
  debug_log "Checking if topic ${topic_name} exists"
  
  kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
    --describe --topic "${topic_name}" &> /dev/null
  
  return $?
}

# Create a Kafka topic with error handling
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
#   $4: Number of partitions (default: 3)
#   $5: Replication factor (default: 3)
# Returns:
#   0 on success, 1 on failure
# Exports:
#   TOPIC_CREATE_TIME - Time taken to create the topic in ms
create_topic() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  local partitions="${4:-3}"
  local replication="${5:-3}"
  
  log "Creating topic ${topic_name} (partitions: ${partitions}, replication: ${replication})"
  
  # Start timer
  local topic_start=$(get_time_ms)
  
  # First check if topic already exists
  if check_topic_exists "${bootstrap_servers}" "${config_file}" "${topic_name}"; then
    log "Topic ${topic_name} already exists, using existing topic"
    local topic_end=$(get_time_ms)
    export TOPIC_CREATE_TIME=$((topic_end - topic_start))
    return 0
  fi
  
  # Topic doesn't exist, create it
  kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
    --create --topic "${topic_name}" \
    --partitions "${partitions}" \
    --replication-factor "${replication}"
  
  local create_status=$?
  if [ $create_status -ne 0 ]; then
    log "ERROR: Topic creation failed! Will attempt to continue with existing topic if available."
    # We'll try to continue even if topic creation reports failure
    # This handles race conditions where the topic was created between our check and create
    if check_topic_exists "${bootstrap_servers}" "${config_file}" "${topic_name}"; then
      log "Topic ${topic_name} exists despite creation error, continuing with test."
      local topic_end=$(get_time_ms)
      export TOPIC_CREATE_TIME=$((topic_end - topic_start))
      return 0
    else
      return 1
    fi
  fi
  
  local topic_end=$(get_time_ms)
  export TOPIC_CREATE_TIME=$((topic_end - topic_start))
  log "Topic creation took ${TOPIC_CREATE_TIME}ms"
  return 0
}

# Delete a Kafka topic
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
#   $4: Fail on error (default: true)
# Returns:
#   0 on success, 1 on failure (if fail_on_error is true)
# Exports:
#   TOPIC_DELETE_TIME - Time taken to delete the topic in ms
delete_topic() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  local fail_on_error="${4:-true}"
  
  log "Attempting to delete topic ${topic_name}"
  local delete_start=$(get_time_ms)
  
  # Check if topic exists before attempting deletion
  if ! check_topic_exists "${bootstrap_servers}" "${config_file}" "${topic_name}"; then
    log "Topic ${topic_name} does not exist, nothing to delete"
    return 0
  fi
  
  # Topic exists, delete it
  kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
    --delete --topic "${topic_name}"
  
  local delete_status=$?
  local delete_end=$(get_time_ms)
  export TOPIC_DELETE_TIME=$((delete_end - delete_start))
  
  if [ $delete_status -ne 0 ]; then
    log "WARNING: Topic deletion failed or topic is pending deletion"
    # This is a warning; kafka will delete the topic asynchronously
    if [ "$fail_on_error" = "true" ]; then
      return 1
    fi
  else 
    log "Topic deletion initiated"
  fi
  
  log "Topic deletion process took ${TOPIC_DELETE_TIME}ms"
  return 0
}

# List available topics
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Output file (optional)
# Returns:
#   List of topics, one per line
list_topics() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local output_file="${3:-""}"
  
  log "Listing available topics"
  
  if [ -n "$output_file" ]; then
    # Output to file
    kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
      --list > "${output_file}"
    return $?
  else
    # Output to stdout
    kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
      --list
    return $?
  fi
}

# Get topic details
# Arguments:
#   $1: Bootstrap servers
#   $2: Config file path
#   $3: Topic name
#   $4: Output file (optional)
# Returns:
#   Topic details
get_topic_details() {
  local bootstrap_servers="$1"
  local config_file="$2"
  local topic_name="$3"
  local output_file="${4:-""}"
  
  log "Getting details for topic ${topic_name}"
  
  if [ -n "$output_file" ]; then
    # Output to file
    kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
      --describe --topic "${topic_name}" > "${output_file}"
    return $?
  else
    # Output to stdout
    kafka-topics --bootstrap-server "${bootstrap_servers}" --command-config "${config_file}" \
      --describe --topic "${topic_name}"
    return $?
  fi
}

# If script is executed directly, print help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Kafka Topic Management Utilities"
  echo "This script provides functions for creating, deleting, and managing Kafka topics."
  echo "It is intended to be sourced by other scripts."
  echo ""
  echo "Example usage:"
  echo "  source $(basename ${BASH_SOURCE[0]})"
  echo "  create_topic \"localhost:9092\" \"client.properties\" \"test-topic\" 3 3"
  echo "  list_topics \"localhost:9092\" \"client.properties\""
  echo "  delete_topic \"localhost:9092\" \"client.properties\" \"test-topic\""
  echo ""
fi