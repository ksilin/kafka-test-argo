import pytest

from core.models import KafkaScenario, ProducerMetrics, ScenarioResult


class TestKafkaScenario:
    def test_from_string_full(self):
        """Test creating a scenario from a full string"""
        scenario_str = "test,1024,5000,60,2"
        scenario = KafkaScenario.from_string(scenario_str)

        assert scenario.name == "test"
        assert scenario.msg_size == 1024
        assert scenario.throughput == 5000
        assert scenario.duration == 60
        assert scenario.producers == 2

    def test_from_string_minimal(self):
        """Test creating a scenario with minimal parameters"""
        scenario_str = "test,1024,5000"
        scenario = KafkaScenario.from_string(scenario_str)

        assert scenario.name == "test"
        assert scenario.msg_size == 1024
        assert scenario.throughput == 5000
        assert scenario.duration == 60  # default
        assert scenario.producers == 1  # default

    def test_from_string_invalid(self):
        """Test exception when string is invalid"""
        scenario_str = "test"

        with pytest.raises(ValueError):
            KafkaScenario.from_string(scenario_str)


class TestScenarioResult:
    def test_success_property(self):
        """Test the success property logic"""
        # Create a successful scenario result
        scenario = KafkaScenario("test", 1024, 5000, 60, 2)
        metrics = [
            ProducerMetrics(100, 10, 50, 60, 6000, 1024),
            ProducerMetrics(120, 12, 60, 60, 7200, 1024),
        ]

        result = ScenarioResult(
            scenario=scenario,
            producer_metrics=metrics,
            total_throughput=220,
            avg_latency=11,
            max_latency=60,
        )

        assert result.success is True

        # Create a failed scenario result
        failed_metrics = [
            ProducerMetrics(0, 0, 0, 60, 0, 1024),
            ProducerMetrics(0, 0, 0, 60, 0, 1024),
        ]

        failed_result = ScenarioResult(
            scenario=scenario,
            producer_metrics=failed_metrics,
            total_throughput=0,
            avg_latency=0,
            max_latency=0,
        )

        assert failed_result.success is False
