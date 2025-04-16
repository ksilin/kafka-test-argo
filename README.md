# Kafka Test Argo

This project provides modular scripts for automated validation, performance, and load testing of Kafka deployments. It supports both local testing and Kubernetes deployment via Argo CD.

## Project Structure

- `scripts/` - Contains all test scripts
  - `kafka-test-modular.sh` - Main unified test script supporting validation and load modes
  - `topic-test-modular.sh` - Simple topic validation script
  - `kafka-test-modules/` - Modular utility functions
    - `kafka-test-utils.sh` - Common utilities
    - `kafka-topic-utils.sh` - Topic management
    - `kafka-perf-utils.sh` - Performance test functions
    - `kafka-load-utils.sh` - Load testing functions
    - `kafka-report-utils.sh` - Report generation

- `kafka-test-manifests/` - Kubernetes manifests for deploying tests
  - `Dockerfile` - Container image with modular scripts
  - Various job definitions for different test types
  - ConfigMaps and volume configurations

## Quick Start

### Running Tests Locally

#### Basic Validation Test

```sh
./scripts/kafka-test-modular.sh --mode validation --config client.properties --topic validation-test-topic
```

#### Load Test with Multiple Producers

```sh
./scripts/kafka-test-modular.sh --mode load --config client.properties --topic load-test-topic --producers 4 --steps 5 --latency 100
```

### Running Tests in Kubernetes

```sh
# Deploy the validation test job
kubectl apply -f kafka-test-manifests/kafka-test-job-modular.yaml

# Deploy the load test job
kubectl apply -f kafka-test-manifests/kafka-load-test-job.yaml

# Deploy the job that keeps results accessible
kubectl apply -f kafka-test-manifests/kafka-test-job-results-fixed.yaml
```

## Testing Locally

For local testing, you may need to set up port forwarding to access your Kafka cluster:

```sh
# Port forward to Kafka service
kubectl port-forward service/kafka -n confluent 9071:9071

# Ensure Kafka CLI tools are in your path
export PATH=$PATH:/path/to/confluent/bin

# Test connection
kafka-topics --bootstrap-server localhost:9071 --list --command-config ./gke-client.config.local.props
```

You might also need to modify your `/etc/hosts` file for domain name resolution.