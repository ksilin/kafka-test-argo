# Kafka Test Python

A Python-based tool for running Kafka performance tests with scenario-based configurations.

## Features

- Run performance tests with various scenarios (message sizes, throughput limits, duration)
- Two implementation options:
  - Kafka CLI tools (using kafka-producer-perf-test)
  - Native Python client (using confluent-kafka)
- Support for concurrent producers
- Detailed metric collection and reporting
- HTML report generation
- Configurable topic creation and cleanup

## Installation

### Local Development

```bash
cd scripts/kafka-test-py
pip install -e .
```

### Docker

```bash
cd /Users/ksilin/Code/workspaces/confluent/customer.code/Nestle/kafka-test-argo
docker build -t kafka-scenario-test-python:latest -f kafka-test-manifests/Dockerfile-python .
```

## Usage

### Run Scenario Tests from File

```bash
# Using Kafka CLI tools implementation (default)
python -m cli run --config client.properties --scenarios scenarios.txt

# Using Kafka CLI tools (default)
python -m cli run-single --config client.properties --scenario "test,1024,5000,60,2"

# Using Python client implementation - untested
python -m cli run-python --config client.properties --scenarios scenarios.txt
```


### Command Line Options

The tool offers several commands:

```sh
  run             Run Kafka tests using Kafka CLI tools (default)
  run-cli         Explicitly run tests using Kafka CLI tools
  run-python      Run tests using Python client
  run-single      Run a single scenario using Kafka CLI tools (default)
  run-single-cli  Explicitly run a single scenario using Kafka CLI tools
  run-single-python  Run a single scenario using Python client
```

Common options for all commands:

```sh
  --config, -c TEXT         Client config file to use  [required]
  --topic-base, -t TEXT     Base name for topics  [default: kafka-perf-test]
  --results-dir, -r TEXT    Directory to store results  [default: ./results]
  --keep-topics, -k         Keep topics after tests complete [default is false]
  --verbose, -v             Enable verbose output
  --help                    Show this message and exit.
```

For running multiple scenarios:

```sh
  --scenarios, -s TEXT      Scenario file with test parameters  [required]
```

For running a single scenario:

```sh
  --scenario, -s TEXT       Scenario string (name,msg_size,throughput,duration,producers)  [required]
```

## Scenario File Format

The scenario file consists of lines in the format:

```sh
name,msg_size,throughput,duration,producers
```

Example:

```sh
# Small messages (100 bytes)
small-unlimited,100,-1,60,1         # Unlimited throughput, 1 producer
small-throttled,100,10000,60,1      # 10K records/sec, 1 producer
small-parallel,100,5000,60,4        # 5K records/sec per producer, 4 producers
```

- `name`: Unique identifier for the scenario
- `msg_size`: Message size in bytes
- `throughput`: Max throughput per producer (-1 for unlimited)
- `duration`: Test duration in seconds
- `producers`: Number of concurrent producers

## Running in Kubernetes

1. Build and push the Docker image:
   ```bash
   docker build -t kafka-scenario-test-python:latest -f kafka-test-manifests/Dockerfile-python .
   ```

```sh
docker buildx build \
-f kafka-test-manifests/Dockerfile-python \
--platform linux/amd64 \
-t ghcr.io/ksilin/kafka-pytest:0.0.1 \
. --push
```

2. Deploy the test job:
  
```bash
kubectl apply -f kafka-test-manifests/kafka-scenario-test-job-python.yaml
```


## Development

### Testing

Run the tests:
```bash
cd scripts/kafka-test-py
pytest
```

Run with coverage:
```bash
cd scripts/kafka-test-py
pytest --cov=kafka_test
```