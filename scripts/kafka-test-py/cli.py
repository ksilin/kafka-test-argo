#!/usr/bin/env python3
import logging
import os
import sys
from typing import Any, Callable, TypeVar, cast

import click

from scenarios.cli_runner import KafkaCliRunner

# Import both runner implementations
from scenarios.python_runner import ScenarioRunner as PythonRunner

# Type variable for generic return type
T = TypeVar('T')

def run_critical(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a function and exit immediately if it fails.
    
    Args:
        func: Function to run
        *args: Positional arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
        
    Raises:
        SystemExit: If the function raises an exception
    """
    try:
        result = func(*args, **kwargs)
        return result
    except Exception as e:
        logging.error(f"Critical operation failed: {e}")
        sys.exit(1)


def setup_logging(verbose: bool) -> None:
    """Set up logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def cli(verbose: bool) -> None:
    """Kafka scenario-based testing tool"""
    setup_logging(verbose)


@cli.command(name="run-python")
@click.option("--config", "-c", required=True, help="Client config file to use")
@click.option(
    "--scenarios", "-s", required=True, help="Scenario file with test parameters"
)
@click.option(
    "--topic-base", "-t", default="kafka-perf-test", help="Base name for topics"
)
@click.option(
    "--results-dir", "-r", default="./results", help="Directory to store results"
)
@click.option(
    "--keep-topics", "-k", is_flag=True, help="Keep topics after tests complete"
)
def run_python(
    config: str, scenarios: str, topic_base: str, results_dir: str, keep_topics: bool
) -> None:
    """Run Kafka scenario-based performance tests using Python client"""
    # Check if file paths exist
    if not os.path.exists(config):
        click.echo(f"Error: Config file '{config}' not found.")
        sys.exit(1)

    if not os.path.exists(scenarios):
        click.echo(f"Error: Scenario file '{scenarios}' not found.")
        sys.exit(1)

    # Create results directory
    os.makedirs(results_dir, exist_ok=True)

    # Initialize runner
    runner = PythonRunner(
        config_file=config,
        results_dir=results_dir,
        topic_base=topic_base,
        keep_topics=keep_topics,
    )

    # Load scenarios
    click.echo(f"Loading scenarios from {scenarios}")
    scenario_list = runner.load_scenarios(scenarios)
    click.echo(f"Found {len(scenario_list)} scenarios")

    # Run scenarios with fail-fast behavior
    click.echo("Starting scenario tests using Python client...")
    results = run_critical(runner.run_scenarios, scenario_list)

    # Print summary
    click.echo("\nTest Summary:")
    failed_scenarios = 0
    for result in results:
        status = "PASS" if result.success else "FAIL"
        if not result.success:
            failed_scenarios += 1
        click.echo(
            f"{result.scenario.name}: {status} - "
            f"Throughput: {result.total_throughput:.2f} records/sec "
            f"({result.throughput_mb_per_sec:.2f} MB/sec), "
            f"Avg Latency: {result.avg_latency:.2f} ms"
        )

    click.echo(f"\nResults saved to {results_dir}")
    click.echo(f"HTML report: {os.path.join(results_dir, 'report.html')}")
    
    # Exit with error if any scenario failed (should never happen with fail-fast enabled)
    if failed_scenarios > 0:
        sys.exit(1)


@cli.command(name="run-cli")
@click.option("--config", "-c", required=True, help="Client config file to use")
@click.option(
    "--scenarios", "-s", required=True, help="Scenario file with test parameters"
)
@click.option(
    "--topic-base", "-t", default="kafka-perf-test", help="Base name for topics"
)
@click.option(
    "--results-dir", "-r", default="./results", help="Directory to store results"
)
@click.option(
    "--keep-topics", "-k", is_flag=True, help="Keep topics after tests complete"
)
def run_cli(
    config: str, scenarios: str, topic_base: str, results_dir: str, keep_topics: bool
) -> None:
    """Run Kafka scenario-based performance tests using Kafka CLI tools"""
    # Check if file paths exist
    if not os.path.exists(config):
        click.echo(f"Error: Config file '{config}' not found.")
        sys.exit(1)

    if not os.path.exists(scenarios):
        click.echo(f"Error: Scenario file '{scenarios}' not found.")
        sys.exit(1)

    # Create results directory
    os.makedirs(results_dir, exist_ok=True)

    # Initialize runner
    runner = KafkaCliRunner(
        config_file=config,
        results_dir=results_dir,
        topic_base=topic_base,
        keep_topics=keep_topics,
    )

    # Load scenarios
    click.echo(f"Loading scenarios from {scenarios}")
    scenario_list = runner.load_scenarios(scenarios)
    click.echo(f"Found {len(scenario_list)} scenarios")

    # Run scenarios with fail-fast behavior
    click.echo("Starting scenario tests using Kafka CLI tools...")
    results = run_critical(runner.run_scenarios, scenario_list)

    # Print summary
    click.echo("\nTest Summary:")
    failed_scenarios = 0
    for result in results:
        status = "PASS" if result.success else "FAIL"
        if not result.success:
            failed_scenarios += 1
        click.echo(
            f"{result.scenario.name}: {status} - "
            f"Throughput: {result.total_throughput:.2f} records/sec "
            f"({result.throughput_mb_per_sec:.2f} MB/sec), "
            f"Avg Latency: {result.avg_latency:.2f} ms"
        )

    click.echo(f"\nResults saved to {results_dir}")
    click.echo(f"HTML report: {os.path.join(results_dir, 'report.html')}")
    
    # Exit with error if any scenario failed (should never happen with fail-fast enabled)
    if failed_scenarios > 0:
        sys.exit(1)


# Default command (for backward compatibility)
@cli.command(name="run")
@click.option("--config", "-c", required=True, help="Client config file to use")
@click.option(
    "--scenarios", "-s", required=True, help="Scenario file with test parameters"
)
@click.option(
    "--topic-base", "-t", default="kafka-perf-test", help="Base name for topics"
)
@click.option(
    "--results-dir", "-r", default="./results", help="Directory to store results"
)
@click.option(
    "--keep-topics", "-k", is_flag=True, help="Keep topics after tests complete"
)
def run(
    config: str, scenarios: str, topic_base: str, results_dir: str, keep_topics: bool
) -> None:
    """Run Kafka scenario-based performance tests (default: CLI implementation)"""
    # Debug: Print arguments received
    print("DEBUG: run command received:")
    print(f"  config: {config}")
    print(f"  scenarios: {scenarios}")
    print(f"  topic_base: {topic_base}")
    print(f"  results_dir: {results_dir}")
    print(f"  keep_topics: {keep_topics}")

    # Let's manually implement this function for now to avoid the issue
    print("DEBUG: Initializing KafkaCliRunner...")

    # Check if files exist
    if not os.path.exists(config):
        click.echo(f"Error: Config file '{config}' not found.")
        return

    if not os.path.exists(scenarios):
        click.echo(f"Error: Scenario file '{scenarios}' not found.")
        return

    print("DEBUG: Files exist, creating runner...")

    try:
        # Initialize runner
        runner = KafkaCliRunner(
            config_file=config,
            results_dir=results_dir,
            topic_base=topic_base,
            keep_topics=keep_topics,
        )

        print("DEBUG: Loading scenarios...")
        # Load scenarios
        scenario_list = runner.load_scenarios(scenarios)
        print(f"DEBUG: Found {len(scenario_list)} scenarios")

        # Run scenarios
        print("DEBUG: Starting scenario tests...")
        try:
            results = runner.run_scenarios(scenario_list)

            # Print summary
            print("\nTest Summary:")
            failed_scenarios = 0
            for result in results:
                status = "PASS" if result.success else "FAIL"
                if not result.success:
                    failed_scenarios += 1
                print(
                    f"{result.scenario.name}: {status} - "
                    f"Throughput: {result.total_throughput:.2f} records/sec "
                    f"({result.throughput_mb_per_sec:.2f} MB/sec), "
                    f"Avg Latency: {result.avg_latency:.2f} ms"
                )

            print(f"\nResults saved to {results_dir}")
            print(f"HTML report: {os.path.join(results_dir, 'report.html')}")

            # Exit with error if any scenario failed
            if failed_scenarios > 0:
                print(f"ERROR: {failed_scenarios} scenario(s) failed")
                sys.exit(1)

        except Exception as e:
            print(f"ERROR: Test execution failed: {e}")
            sys.exit(1)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()


@cli.command(name="run-single-cli")
@click.option("--config", "-c", required=True, help="Client config file to use")
@click.option(
    "--scenario",
    "-s",
    required=True,
    help="Scenario string (name,msg_size,throughput,duration,producers)",
)
@click.option(
    "--topic-base", "-t", default="kafka-perf-test", help="Base name for topics"
)
@click.option(
    "--results-dir", "-r", default="./results", help="Directory to store results"
)
@click.option(
    "--keep-topics", "-k", is_flag=True, help="Keep topics after tests complete"
)
def run_single_cli(
    config: str, scenario: str, topic_base: str, results_dir: str, keep_topics: bool
) -> None:
    """Run a single Kafka performance test scenario using CLI tools"""
    from core.models import KafkaScenario

    # Check if config file exists
    if not os.path.exists(config):
        click.echo(f"Error: Config file '{config}' not found.")
        sys.exit(1)

    # Create results directory
    os.makedirs(results_dir, exist_ok=True)

    try:
        # Parse scenario
        scenario_obj = KafkaScenario.from_string(scenario)

        # Initialize runner
        runner = KafkaCliRunner(
            config_file=config,
            results_dir=results_dir,
            topic_base=topic_base,
            keep_topics=keep_topics,
        )

        # Run scenario with fail-fast behavior
        click.echo(f"Running scenario using CLI tools: {scenario_obj.name}")
        result = run_critical(runner.run_scenario, scenario_obj)

        # Print summary
        status = "PASS" if result.success else "FAIL"
        click.echo("\nTest Summary:")
        click.echo(
            f"{result.scenario.name}: {status} - "
            f"Throughput: {result.total_throughput:.2f} records/sec "
            f"({result.throughput_mb_per_sec:.2f} MB/sec), "
            f"Avg Latency: {result.avg_latency:.2f} ms"
        )

        scenario_dir = os.path.join(results_dir, scenario_obj.name)
        click.echo(f"\nResults saved to {scenario_dir}")

    except ValueError as e:
        click.echo(f"Error parsing scenario: {e}")
        sys.exit(1)


@cli.command(name="run-single-python")
@click.option("--config", "-c", required=True, help="Client config file to use")
@click.option(
    "--scenario",
    "-s",
    required=True,
    help="Scenario string (name,msg_size,throughput,duration,producers)",
)
@click.option(
    "--topic-base", "-t", default="kafka-perf-test", help="Base name for topics"
)
@click.option(
    "--results-dir", "-r", default="./results", help="Directory to store results"
)
@click.option(
    "--keep-topics", "-k", is_flag=True, help="Keep topics after tests complete"
)
def run_single_python(
    config: str, scenario: str, topic_base: str, results_dir: str, keep_topics: bool
) -> None:
    """Run a single Kafka performance test scenario using Python client"""
    from core.models import KafkaScenario

    # Check if config file exists
    if not os.path.exists(config):
        click.echo(f"Error: Config file '{config}' not found.")
        sys.exit(1)

    # Create results directory
    os.makedirs(results_dir, exist_ok=True)

    try:
        # Parse scenario
        scenario_obj = KafkaScenario.from_string(scenario)

        # Initialize runner
        runner = PythonRunner(
            config_file=config,
            results_dir=results_dir,
            topic_base=topic_base,
            keep_topics=keep_topics,
        )

        # Run scenario with fail-fast behavior
        click.echo(f"Running scenario using Python client: {scenario_obj.name}")
        result = run_critical(runner.run_scenario, scenario_obj)

        # Print summary
        status = "PASS" if result.success else "FAIL"
        click.echo("\nTest Summary:")
        click.echo(
            f"{result.scenario.name}: {status} - "
            f"Throughput: {result.total_throughput:.2f} records/sec "
            f"({result.throughput_mb_per_sec:.2f} MB/sec), "
            f"Avg Latency: {result.avg_latency:.2f} ms"
        )

        scenario_dir = os.path.join(results_dir, scenario_obj.name)
        click.echo(f"\nResults saved to {scenario_dir}")

    except ValueError as e:
        click.echo(f"Error parsing scenario: {e}")
        sys.exit(1)


# Default run-single command (for backward compatibility)
@cli.command(name="run-single")
@click.option("--config", "-c", required=True, help="Client config file to use")
@click.option(
    "--scenario",
    "-s",
    required=True,
    help="Scenario string (name,msg_size,throughput,duration,producers)",
)
@click.option(
    "--topic-base", "-t", default="kafka-perf-test", help="Base name for topics"
)
@click.option(
    "--results-dir", "-r", default="./results", help="Directory to store results"
)
@click.option(
    "--keep-topics", "-k", is_flag=True, help="Keep topics after tests complete"
)
def run_single(
    config: str, scenario: str, topic_base: str, results_dir: str, keep_topics: bool
) -> None:
    """Run a single Kafka performance test scenario (default: CLI implementation)"""
    # Debug: Print arguments received
    print("DEBUG: run-single command received:")
    print(f"  config: {config}")
    print(f"  scenario: {scenario}")
    print(f"  topic_base: {topic_base}")
    print(f"  results_dir: {results_dir}")
    print(f"  keep_topics: {keep_topics}")

    # Let's manually implement this function for now to avoid the issue
    print("DEBUG: Initializing KafkaCliRunner...")

    # Check if files exist
    if not os.path.exists(config):
        click.echo(f"Error: Config file '{config}' not found.")
        return

    # Create results directory
    os.makedirs(results_dir, exist_ok=True)

    try:
        # Parse scenario
        from core.models import KafkaScenario

        scenario_obj = KafkaScenario.from_string(scenario)

        # Initialize runner
        from scenarios.cli_runner import KafkaCliRunner

        runner = KafkaCliRunner(
            config_file=config,
            results_dir=results_dir,
            topic_base=topic_base,
            keep_topics=keep_topics,
        )

        # Run scenario with fail-fast behavior
        print(f"Running scenario: {scenario_obj.name}")
        result = run_critical(runner.run_scenario, scenario_obj)

        # Print summary
        status = "PASS" if result.success else "FAIL"
        print("\nTest Summary:")
        print(
            f"{result.scenario.name}: {status} - "
            f"Throughput: {result.total_throughput:.2f} records/sec "
            f"({result.throughput_mb_per_sec:.2f} MB/sec), "
            f"Avg Latency: {result.avg_latency:.2f} ms"
        )

        scenario_dir = os.path.join(results_dir, scenario_obj.name)
        print(f"\nResults saved to {scenario_dir}")

        # Exit with error if scenario failed
        if not result.success:
            sys.exit(1)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def main() -> None:
    """Main entry point"""
    # Debug: Print out command line arguments
    import sys

    print("DEBUG: Command line arguments:", sys.argv)
    cli(auto_envvar_prefix="KAFKA_TEST")


if __name__ == "__main__":
    main()
