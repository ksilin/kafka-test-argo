import concurrent.futures
import csv
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaException, Producer

from core.models import KafkaScenario, ProducerMetrics, ScenarioResult
from utils.topic import create_topic, delete_topic


class ScenarioRunner:
    """
    Runs Kafka performance test scenarios
    """

    def __init__(
        self,
        config_file: str,
        results_dir: str = "./results",
        topic_base: str = "kafka-perf-test",
        keep_topics: bool = False,
    ):
        """
        Initialize the scenario runner

        Args:
            config_file: Path to the Kafka client config file
            results_dir: Directory to store results
            topic_base: Base name for topics
            keep_topics: Whether to keep topics after test
        """
        self.config_file = config_file
        self.results_dir = results_dir
        self.topic_base = topic_base
        self.keep_topics = keep_topics

        # Parse config file
        from utils.config import get_bootstrap_servers, parse_client_config

        self.client_config = parse_client_config(config_file)
        self.bootstrap_servers = get_bootstrap_servers(self.client_config)

        # Create results directory
        os.makedirs(results_dir, exist_ok=True)

    def load_scenarios(self, scenario_file: str) -> List[KafkaScenario]:
        """
        Load scenarios from a file

        Args:
            scenario_file: Path to the scenario file

        Returns:
            List of KafkaScenario objects
        """
        scenarios = []

        with open(scenario_file, "r") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue

                try:
                    scenario = KafkaScenario.from_string(line)
                    scenarios.append(scenario)
                except Exception as e:
                    print(f"Error parsing scenario: {line}, {e}")

        return scenarios

    def run_scenario(self, scenario: KafkaScenario) -> ScenarioResult:
        """
        Run a single scenario

        Args:
            scenario: Scenario to run

        Returns:
            ScenarioResult object with metrics
        """
        print(f"Running scenario: {scenario.name}")
        print(f"  Message Size: {scenario.msg_size} bytes")
        print(f"  Throughput: {scenario.throughput} records/sec per producer")
        print(f"  Duration: {scenario.duration} seconds")
        print(f"  Producers: {scenario.producers}")

        # Create topic
        topic_name = f"{self.topic_base}-{scenario.name}"
        partitions = max(
            3, scenario.producers * 2
        )  # At least 2 partitions per producer

        create_topic(
            self.client_config,
            topic_name,
            partitions=partitions,
            replication_factor=3,  # Assuming 3 brokers
        )

        # Setup result directory
        scenario_dir = os.path.join(self.results_dir, scenario.name)
        os.makedirs(scenario_dir, exist_ok=True)

        try:
            # Run the producers
            producer_metrics = self._run_timed_producers(
                scenario, topic_name, scenario_dir
            )

            # Calculate aggregated metrics
            total_throughput = sum(m.throughput for m in producer_metrics)
            avg_latency = (
                sum(m.avg_latency for m in producer_metrics) / len(producer_metrics)
                if producer_metrics
                else 0
            )
            max_latency = (
                max(m.max_latency for m in producer_metrics) if producer_metrics else 0
            )

            # Create the result
            result = ScenarioResult(
                scenario=scenario,
                producer_metrics=producer_metrics,
                total_throughput=total_throughput,
                avg_latency=avg_latency,
                max_latency=max_latency,
            )

            # Save results
            self._save_scenario_results(result, scenario_dir)

            return result

        finally:
            # Clean up topics unless keep_topics is set
            if not self.keep_topics:
                delete_topic(self.client_config, topic_name)

    def run_scenarios(self, scenarios: List[KafkaScenario]) -> List[ScenarioResult]:
        """
        Run multiple scenarios

        Args:
            scenarios: List of scenarios to run

        Returns:
            List of ScenarioResult objects
        """
        results = []

        # Create summary file
        summary_path = os.path.join(self.results_dir, "summary.csv")
        with open(summary_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "scenario",
                    "message_size",
                    "throughput_limit",
                    "duration",
                    "producers",
                    "total_throughput",
                    "avg_latency",
                    "max_latency",
                ]
            )

        for scenario in scenarios:
            # Run the scenario
            result = self.run_scenario(scenario)
            results.append(result)

            # Add to summary
            with open(summary_path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        scenario.name,
                        scenario.msg_size,
                        scenario.throughput,
                        scenario.duration,
                        scenario.producers,
                        f"{result.total_throughput:.2f}",
                        f"{result.avg_latency:.2f}",
                        f"{result.max_latency:.2f}",
                    ]
                )

            # Sleep between scenarios to let the system recover
            print("Waiting 15 seconds between scenarios...")
            time.sleep(15)

        # Generate the HTML report
        self._generate_html_report()

        return results

    def _run_timed_producers(
        self,
        scenario: KafkaScenario,
        topic_name: str,
        output_dir: str,
    ) -> List[ProducerMetrics]:
        """
        Run timed producer performance tests

        Args:
            scenario: Scenario configuration
            topic_name: Topic to produce to
            output_dir: Directory to save output files

        Returns:
            List of ProducerMetrics
        """
        # Launch producers in separate threads
        producer_futures = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=scenario.producers
        ) as executor:
            for i in range(scenario.producers):
                output_file = os.path.join(output_dir, f"producer_{i+1}.txt")
                future = executor.submit(
                    self._run_timed_producer,
                    producer_id=i + 1,
                    topic_name=topic_name,
                    message_size=scenario.msg_size,
                    throughput=scenario.throughput,
                    duration=scenario.duration,
                    output_file=output_file,
                )
                producer_futures.append((future, output_file))

            # Wait for all producers and collect metrics
            metrics = []
            for future, output_file in producer_futures:
                try:
                    # Get metrics from the producer
                    producer_metrics = future.result()
                    metrics.append(producer_metrics)
                except Exception as e:
                    print(f"Producer failed: {e}")

        return metrics

    def _run_timed_producer(
        self,
        producer_id: int,
        topic_name: str,
        message_size: int,
        throughput: int,
        duration: int,
        output_file: str,
    ) -> ProducerMetrics:
        """
        Run a single timed producer test

        Args:
            producer_id: ID of the producer (for logging)
            topic_name: Topic to produce to
            message_size: Size of each message in bytes
            throughput: Target throughput in messages per second (-1 for unlimited)
            duration: Test duration in seconds
            output_file: File to save output to

        Returns:
            ProducerMetrics with the results
        """
        # Configure producer
        config = {**self.client_config}

        # Add client.id
        config["client.id"] = f"producer-{producer_id}"

        # Create producer
        producer = Producer(config)

        # Prepare metrics
        start_time = time.time()
        end_time = start_time + duration
        message_count = 0
        latency_sum = 0
        max_latency = 0

        # Prepare message payload
        message_payload = b"X" * message_size

        # Prepare output file
        with open(output_file, "w") as f:
            f.write(f"Producer {producer_id} test to {topic_name}\n")
            f.write(f"Message size: {message_size} bytes\n")
            f.write(f"Throughput limit: {throughput} msg/sec\n")
            f.write(f"Duration: {duration} seconds\n\n")

        # Calculate throttling parameters if needed
        throttle = throughput > 0
        throttle_period = 1.0 / throughput if throttle else 0
        batch_size = 100  # Process in batches for throttling

        # Callback for message delivery
        def delivery_callback(err, msg):
            nonlocal latency_sum, max_latency

            if err:
                print(f"Message delivery failed: {err}")
            else:
                # Calculate latency
                send_time = msg.timestamp()[1] / 1000  # Convert to seconds
                current_time = time.time()
                latency_ms = (current_time - send_time) * 1000

                # Update metrics
                latency_sum += latency_ms
                max_latency = max(max_latency, latency_ms)

        # Set initial batch start time
        batch_start = time.time()

        # Produce messages until duration is reached
        try:
            while time.time() < end_time:
                # Send a batch of messages
                batch_count = 0
                for _ in range(batch_size):
                    if time.time() >= end_time:
                        break

                    # Produce message with timestamp
                    producer.produce(
                        topic_name,
                        value=message_payload,
                        timestamp=int(time.time() * 1000),  # Current time in ms
                        callback=delivery_callback,
                    )

                    message_count += 1
                    batch_count += 1

                # Ensure all messages are sent
                producer.poll(0)

                # Throttle if needed
                if throttle and batch_count > 0:
                    batch_elapsed = time.time() - batch_start
                    expected_time = batch_count * throttle_period

                    if batch_elapsed < expected_time:
                        time.sleep(expected_time - batch_elapsed)

                    # Reset batch start time
                    batch_start = time.time()

        except Exception as e:
            print(f"Producer {producer_id} error: {e}")

        finally:
            # Ensure all messages are delivered
            producer.flush()

        # Calculate final metrics
        actual_duration = time.time() - start_time
        throughput_achieved = (
            message_count / actual_duration if actual_duration > 0 else 0
        )
        avg_latency = latency_sum / message_count if message_count > 0 else 0

        # Save the final metrics to the output file
        with open(output_file, "a") as f:
            f.write(f"\nTest completed\n")
            f.write(f"Records sent: {message_count}\n")
            f.write(f"Throughput: {throughput_achieved:.2f} records/sec\n")
            f.write(f"Average latency: {avg_latency:.2f} ms\n")
            f.write(f"Maximum latency: {max_latency:.2f} ms\n")
            f.write(f"Test duration: {actual_duration:.2f} seconds\n")

        # Create metrics object
        return ProducerMetrics(
            throughput=throughput_achieved,
            avg_latency=avg_latency,
            max_latency=max_latency,
            duration=actual_duration,
            record_count=message_count,
            record_size=message_size,
        )

    def _save_scenario_results(self, result: ScenarioResult, scenario_dir: str) -> None:
        """
        Save scenario results to files

        Args:
            result: Scenario result to save
            scenario_dir: Directory to save results to
        """
        # Save results as CSV
        results_file = os.path.join(scenario_dir, "results.csv")

        with open(results_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "producer",
                    "throughput",
                    "avg_latency",
                    "max_latency",
                    "duration",
                    "message_size",
                    "throughput_limit",
                ]
            )

            # Add each producer's results
            for i, metrics in enumerate(result.producer_metrics):
                writer.writerow(
                    [
                        i + 1,
                        f"{metrics.throughput:.2f}",
                        f"{metrics.avg_latency:.2f}",
                        f"{metrics.max_latency:.2f}",
                        f"{metrics.duration:.2f}",
                        metrics.record_size,
                        result.scenario.throughput,
                    ]
                )

            # Add total row
            writer.writerow(
                [
                    "TOTAL",
                    f"{result.total_throughput:.2f}",
                    f"{result.avg_latency:.2f}",
                    f"{result.max_latency:.2f}",
                    f"{result.scenario.duration}",
                    result.scenario.msg_size,
                    result.scenario.throughput,
                ]
            )

    def _generate_html_report(self) -> None:
        """
        Generate an HTML report from the test results
        """
        summary_path = os.path.join(self.results_dir, "summary.csv")
        report_path = os.path.join(self.results_dir, "report.html")

        if not os.path.exists(summary_path):
            print("No summary file found, skipping HTML report generation")
            return

        # Read summary data
        summary_data = []
        with open(summary_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                summary_data.append(row)

        # Generate HTML
        with open(report_path, "w") as f:
            f.write(
                f"""<!DOCTYPE html>
<html>
<head>
  <title>Kafka Performance Test Results</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; }}
    h1 {{ color: #333; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ background-color: #f2f2f2; }}
    tr:nth-child(even) {{ background-color: #f9f9f9; }}
    .scenario-header {{ font-weight: bold; background-color: #e6f2ff; }}
  </style>
</head>
<body>
  <h1>Kafka Performance Test Results</h1>
  <p>Test Date: {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
  <p>Bootstrap Server: {self.bootstrap_servers}</p>
  
  <h2>Summary Results</h2>
  <table>
    <tr>
      <th>Scenario</th>
      <th>Message Size (bytes)</th>
      <th>Throughput Limit</th>
      <th>Duration (s)</th>
      <th>Producers</th>
      <th>Total Throughput (rec/s)</th>
      <th>Avg Latency (ms)</th>
      <th>Max Latency (ms)</th>
    </tr>
"""
            )

            # Add rows for each scenario
            for row in summary_data:
                f.write(
                    f"""    <tr>
      <td>{row['scenario']}</td>
      <td>{row['message_size']}</td>
      <td>{row['throughput_limit']}</td>
      <td>{row['duration']}</td>
      <td>{row['producers']}</td>
      <td>{row['total_throughput']}</td>
      <td>{row['avg_latency']}</td>
      <td>{row['max_latency']}</td>
    </tr>
"""
                )

            f.write(
                """  </table>
</body>
</html>
"""
            )

        print(f"HTML report generated: {report_path}")
