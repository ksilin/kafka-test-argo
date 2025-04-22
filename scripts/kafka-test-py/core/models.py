from dataclasses import dataclass
from typing import List


@dataclass
class KafkaScenario:
    name: str
    msg_size: int
    throughput: int  # -1 means unlimited
    duration: int
    producers: int = 1

    @classmethod
    def from_string(cls, scenario_str: str) -> "KafkaScenario":
        """Create a scenario from a string in the format 'name,msg_size,throughput,duration,producers'"""
        # Remove comments (anything after #)
        if "#" in scenario_str:
            scenario_str = scenario_str.split("#")[0].strip()

        # Split by comma and strip each part
        parts = [part.strip() for part in scenario_str.strip().split(",")]

        # Basic validation
        if len(parts) < 3:
            raise ValueError(
                f"Invalid scenario format: {scenario_str}. Expected at least name,msg_size,duration"
            )

        # Parse with defaults
        name = parts[0]
        msg_size = int(parts[1])
        throughput = int(parts[2]) if len(parts) > 2 and parts[2] else -1
        duration = int(parts[3]) if len(parts) > 3 and parts[3] else 60
        producers = int(parts[4]) if len(parts) > 4 and parts[4] else 1

        return cls(
            name=name,
            msg_size=msg_size,
            throughput=throughput,
            duration=duration,
            producers=producers,
        )


@dataclass
class ProducerMetrics:
    throughput: float  # records/second
    avg_latency: float  # milliseconds
    max_latency: float  # milliseconds
    duration: float  # seconds
    record_count: int
    record_size: int


@dataclass
class ScenarioResult:
    scenario: KafkaScenario
    producer_metrics: List[ProducerMetrics]
    total_throughput: float
    avg_latency: float
    max_latency: float

    @property
    def success(self) -> bool:
        """A scenario is successful if it produced at least some records"""
        return any(m.record_count > 0 for m in self.producer_metrics)

    @property
    def throughput_mb_per_sec(self) -> float:
        """Calculate throughput in MB/s based on record size and records/sec"""
        bytes_per_sec = self.total_throughput * self.scenario.msg_size
        return bytes_per_sec / (1024 * 1024)  # Convert to MB/s
