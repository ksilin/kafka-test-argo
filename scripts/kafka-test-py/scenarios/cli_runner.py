import concurrent.futures
import csv
import os
import subprocess
import time
from typing import Dict, List, Optional, Tuple

from core.models import KafkaScenario, ProducerMetrics, ScenarioResult
from utils.config import get_bootstrap_servers, parse_client_config


class KafkaCliRunner:
    """
    Runs Kafka performance test scenarios using Kafka CLI tools
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
        self.client_config = parse_client_config(config_file)
        self.bootstrap_servers = get_bootstrap_servers(self.client_config)

        # Create results directory
        os.makedirs(results_dir, exist_ok=True)

        # Detect number of brokers
        self.broker_count = self._detect_broker_count()
        print(f"Detected {self.broker_count} broker(s)")

    def _detect_broker_count(self) -> int:
        """
        Detect the number of brokers in the cluster

        Returns:
            Number of brokers, or 1 if detection fails
        """
        try:
            # Try to get metadata to count brokers
            cmd = [
                "kafka-broker-api-versions",
                "--bootstrap-server",
                self.bootstrap_servers,
                "--command-config",
                self.config_file,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split("\n")

            # Count unique broker IDs
            broker_count = 0
            for line in lines:
                if "id:" in line:
                    broker_count += 1

            return max(1, broker_count)
        except Exception as e:
            print(f"Error detecting broker count: {e}")
            return 1  # Default to 1 if detection fails

    def load_scenarios(self, scenario_file: str) -> List[KafkaScenario]:
        """
        Load scenarios from a file

        Args:
            scenario_file: Path to the scenario file

        Returns:
            List of KafkaScenario objects
        """
        scenarios = []
        line_number = 0

        with open(scenario_file, "r") as f:
            for line in f:
                line_number += 1
                line = line.strip()

                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue

                try:
                    # Parse the scenario line
                    scenario = KafkaScenario.from_string(line)
                    scenarios.append(scenario)
                    print(
                        f"Loaded scenario: {scenario.name} - {scenario.msg_size} bytes, "
                        + f"{scenario.throughput} msg/sec, {scenario.duration}s, {scenario.producers} producers"
                    )
                except Exception as e:
                    print(f"Error parsing scenario on line {line_number}: {line}")
                    print(f"  Error details: {e}")

        if not scenarios:
            print("WARNING: No valid scenarios found in file")

        return scenarios

    def run_scenario(self, scenario: KafkaScenario) -> ScenarioResult:
        """
        Run a single scenario using Kafka CLI tools

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

        # Create topic using kafka-topics CLI tool instead of AdminClient
        topic_name = f"{self.topic_base}-{scenario.name}"
        partitions = max(
            3, scenario.producers * 2
        )  # At least 2 partitions per producer

        # Use Kafka CLI tool to create topic, omitting replication factor
        # This will raise an exception if topic creation fails
        self._create_topic_cli(
            topic_name=topic_name, partitions=partitions
        )

        # Setup result directory
        scenario_dir = os.path.join(self.results_dir, scenario.name)
        os.makedirs(scenario_dir, exist_ok=True)

        try:
            # Run the producers in parallel
            # This will raise an exception if producer tests fail
            producer_metrics = self._run_timed_producers(
                scenario=scenario, topic_name=topic_name, scenario_dir=scenario_dir
            )

            # Calculate aggregated metrics
            total_throughput = sum(m.throughput for m in producer_metrics)
            avg_latency = sum(m.avg_latency for m in producer_metrics) / len(
                producer_metrics
            )
            max_latency = max(m.max_latency for m in producer_metrics)

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

            # Clean up topics unless keep_topics is set
            if not self.keep_topics:
                self._delete_topic_cli(topic_name)

            return result

        except Exception as e:
            print(f"ERROR: Scenario execution failed: {e}")
            # Clean up topics unless keep_topics is set
            if not self.keep_topics:
                self._delete_topic_cli(topic_name)
            # Re-raise the exception to propagate the failure
            raise

    def run_scenarios(self, scenarios: List[KafkaScenario]) -> List[ScenarioResult]:
        """
        Run multiple scenarios

        Args:
            scenarios: List of scenarios to run

        Returns:
            List of ScenarioResult objects
            
        Raises:
            Exception: If any scenario fails
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
            try:
                # Run the scenario
                result = self.run_scenario(scenario)
                results.append(result)

                # Check if scenario was successful
                if not result.success:
                    error_msg = f"Scenario {scenario.name} failed to produce messages"
                    print(f"ERROR: {error_msg}")
                    # Generate report with what we have so far
                    self._generate_html_report()
                    # Throw an exception to immediately fail the job
                    raise RuntimeError(error_msg)

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

            except Exception as e:
                print(f"ERROR: Execution stopped due to failure in scenario {scenario.name}: {e}")
                import traceback
                traceback.print_exc()
                
                # Generate report with results collected so far
                self._generate_html_report()
                
                # Re-raise to stop execution
                raise

        # Generate the HTML report for successful completion
        self._generate_html_report()

        return results

    def _run_timed_producers(
        self,
        scenario: KafkaScenario,
        topic_name: str,
        scenario_dir: str,
    ) -> List[ProducerMetrics]:
        """
        Run timed producer performance tests in parallel

        Args:
            scenario: Scenario configuration
            topic_name: Topic to produce to
            scenario_dir: Directory to save output files

        Returns:
            List of ProducerMetrics
        """
        # Calculate number of messages
        messages_per_sec = 1000  # default to 1000 msg/sec for unlimited
        if scenario.throughput > 0:
            messages_per_sec = scenario.throughput

        # Add safety factor of 2x to ensure we don't run out of messages
        num_messages = messages_per_sec * scenario.duration * 2

        # Set limits for sanity
        if num_messages < 1000:
            num_messages = 1000
        elif num_messages > 100000000:
            num_messages = 100000000

        # Launch producers in parallel
        producer_futures = []

        print(f"Starting {scenario.producers} producers in parallel")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=scenario.producers
        ) as executor:
            # Submit all producer tasks first
            for i in range(scenario.producers):
                output_file = os.path.join(scenario_dir, f"producer_{i+1}.txt")

                # Small delay between starting producers to avoid overlapping log entries
                if i > 0:
                    time.sleep(0.5)

                future = executor.submit(
                    self._run_producer_perf_test,
                    producer_id=i + 1,
                    topic_name=topic_name,
                    num_messages=num_messages,
                    message_size=scenario.msg_size,
                    throughput=scenario.throughput,
                    duration=scenario.duration,
                    output_file=output_file,
                )
                producer_futures.append((future, output_file))
                print(f"Submitted producer {i+1} task")

            # Wait for all producers and collect metrics
            metrics = []
            producer_errors = []
            for future, output_file in producer_futures:
                try:
                    producer_metrics = future.result()
                    # Check if producer test was successful
                    if producer_metrics.record_count == 0:
                        error_msg = f"Producer test produced no records: {output_file}"
                        print(f"ERROR: {error_msg}")
                        producer_errors.append(error_msg)
                    metrics.append(producer_metrics)
                except Exception as e:
                    error_msg = f"Producer failed: {e}"
                    print(f"ERROR: {error_msg}")
                    import traceback
                    traceback.print_exc()
                    producer_errors.append(error_msg)
            
            # If any producers failed, raise an exception
            if producer_errors:
                if not metrics:
                    raise RuntimeError(f"All producers failed: {'; '.join(producer_errors)}")
                elif len(metrics) < scenario.producers:
                    raise RuntimeError(f"Some producers failed: {'; '.join(producer_errors)}")

        return metrics

    def _run_producer_perf_test(
        self,
        producer_id: int,
        topic_name: str,
        num_messages: int,
        message_size: int,
        throughput: int,
        duration: int,
        output_file: str,
    ) -> ProducerMetrics:
        """
        Run Kafka producer performance test using kafka-producer-perf-test tool

        Args:
            producer_id: Producer ID (for logging)
            topic_name: Topic name
            num_messages: Number of messages to send
            message_size: Message size in bytes
            throughput: Throughput limit (-1 for unlimited)
            duration: Test duration in seconds
            output_file: File to save output

        Returns:
            ProducerMetrics object
        """
        # Prepare command
        cmd = [
            "kafka-producer-perf-test",
            "--topic",
            topic_name,
            "--num-records",
            str(num_messages),
            "--record-size",
            str(message_size),
            "--throughput",
            str(throughput if throughput > 0 else 999999999),
            "--producer.config",
            self.config_file,
            "--print-metrics",
        ]

        print(f"Running producer {producer_id}: {' '.join(cmd)}")

        # Start time
        start_time = time.time()

        # Run with timeout
        try:
            # Create process
            with open(output_file, "w") as f:
                process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

                # Wait for duration or process to finish
                time_elapsed = 0
                while time_elapsed < duration and process.poll() is None:
                    time.sleep(1)
                    time_elapsed = int(time.time() - start_time)

                # Kill if still running
                if process.poll() is None:
                    print(
                        f"Terminating producer {producer_id} after {duration} seconds"
                    )
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        except Exception as e:
            print(f"Error running producer {producer_id}: {e}")

        # Calculate actual duration
        end_time = time.time()
        actual_duration = end_time - start_time

        # Parse the output file to extract metrics
        return self._parse_producer_perf_output(
            output_file, actual_duration, message_size
        )

    def _parse_producer_perf_output(
        self, output_file: str, duration: float, message_size: int
    ) -> ProducerMetrics:
        """
        Parse the output of kafka-producer-perf-test to extract metrics

        Args:
            output_file: Path to the output file
            duration: Actual test duration in seconds
            message_size: Message size in bytes

        Returns:
            ProducerMetrics object
        """
        throughput = 0.0
        avg_latency = 0.0
        max_latency = 0.0
        record_count = 0

        try:
            with open(output_file, "r") as f:
                content = f.read()

                # We'll analyze all lines that match the pattern and use the last one
                # for final metrics, since it's the most representative
                for line in content.split("\n"):
                    if "records sent" in line and "records/sec" in line:
                        try:
                            # Example line: "25009 records sent, 5001,8 records/sec (4,88 MB/sec), 1,2 ms avg latency, 11,0 ms max latency."
                            parts = line.split(",")

                            # Parse records sent
                            records_part = parts[0].strip()
                            record_count = int(records_part.split()[0])

                            # Parse throughput
                            throughput_part = parts[1].strip()
                            throughput_str = throughput_part.split()[0].replace(
                                ",", "."
                            )
                            throughput = float(throughput_str)

                            # Parse avg latency
                            latency_part = parts[2].strip()
                            avg_latency_str = latency_part.split()[0].replace(",", ".")
                            avg_latency = float(avg_latency_str)

                            # Parse max latency
                            max_latency_part = parts[3].strip()
                            max_latency_str = max_latency_part.split()[0].replace(
                                ",", "."
                            )
                            max_latency = float(max_latency_str)

                            print(
                                f"Parsed metrics - records: {record_count}, throughput: {throughput}, "
                                + f"avg_latency: {avg_latency}, max_latency: {max_latency}"
                            )
                        except Exception as e:
                            print(f"WARNING: Error parsing line: {line}: {e}")
                            # Continue to next line in case this one failed

                # If we couldn't find metrics the standard way, try looking for alternative formats
                if throughput == 0 and record_count > 0 and duration > 0:
                    throughput = record_count / duration

        except Exception as e:
            print(f"Error parsing perf test output: {e}")

        # Create metrics object
        return ProducerMetrics(
            throughput=throughput,
            avg_latency=avg_latency,
            max_latency=max_latency,
            duration=duration,
            record_count=record_count,
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

    def _create_topic_cli(
        self, topic_name: str, partitions: int = 3, replication_factor: int = None
    ) -> bool:
        """
        Create a Kafka topic using kafka-topics CLI tool

        Args:
            topic_name: Topic name
            partitions: Number of partitions
            replication_factor: Replication factor (optional)

        Returns:
            True if successful

        Raises:
            RuntimeError: If topic creation fails
        """
        if replication_factor:
            print(
                f"Creating topic {topic_name} with {partitions} partitions, replication factor {replication_factor}"
            )
        else:
            print(
                f"Creating topic {topic_name} with {partitions} partitions (default replication factor)"
            )

        # Check if topic already exists
        cmd = [
            "kafka-topics",
            "--bootstrap-server",
            self.bootstrap_servers,
            "--command-config",
            self.config_file,
            "--list",
        ]

        try:
            # Run the command to list topics
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            topics = result.stdout.strip().split("\n")

            if topic_name in topics:
                print(f"Topic {topic_name} already exists")
                return True

            # Create the topic
            cmd = [
                "kafka-topics",
                "--bootstrap-server",
                self.bootstrap_servers,
                "--command-config",
                self.config_file,
                "--create",
                "--topic",
                topic_name,
                "--partitions",
                str(partitions),
            ]

            # Only add replication factor if explicitly specified
            if replication_factor is not None:
                cmd.extend(["--replication-factor", str(replication_factor)])

            # Run the command
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(f"Topic {topic_name} created successfully")
            return True

        except subprocess.CalledProcessError as e:
            error_msg = f"Error creating topic {topic_name}: {e}"
            print(f"ERROR: {error_msg}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            # Raise an exception instead of returning False
            raise RuntimeError(error_msg)

    def _delete_topic_cli(self, topic_name: str) -> bool:
        """
        Delete a Kafka topic using kafka-topics CLI tool

        Args:
            topic_name: Topic name

        Returns:
            True if successful, False otherwise
        """
        print(f"Deleting topic {topic_name}")

        cmd = [
            "kafka-topics",
            "--bootstrap-server",
            self.bootstrap_servers,
            "--command-config",
            self.config_file,
            "--delete",
            "--topic",
            topic_name,
        ]

        try:
            # Run the command
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(f"Topic {topic_name} deleted successfully")
            return True

        except subprocess.CalledProcessError as e:
            print(f"Error deleting topic {topic_name}: {e}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            return False

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
  <title>Kafka Performance Test Results (CLI)</title>
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
  <h1>Kafka Performance Test Results (CLI)</h1>
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
