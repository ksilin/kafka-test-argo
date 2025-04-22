import os
from typing import Any, Dict, Optional


def parse_client_config(config_file: str) -> Dict[str, Any]:
    """
    Parse Kafka client configuration file.
    Supports both Java properties format and YAML.

    Args:
        config_file: Path to the config file

    Returns:
        Dictionary with Kafka client configuration
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found: {config_file}")

    # Determine file format based on extension
    if config_file.endswith(".properties"):
        return _parse_properties(config_file)
    elif config_file.endswith((".yaml", ".yml")):
        return _parse_yaml(config_file)
    else:
        # Default to properties format
        return _parse_properties(config_file)


def _parse_properties(file_path: str) -> Dict[str, Any]:
    """Parse Java properties file format"""
    config = {}

    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()

    return config


def _parse_yaml(file_path: str) -> Dict[str, Any]:
    """Parse YAML file format"""
    import yaml

    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def get_bootstrap_servers(config: Dict[str, Any]) -> str:
    """
    Extract bootstrap servers from a config dictionary

    Args:
        config: Kafka configuration dictionary

    Returns:
        Bootstrap servers string
    """
    # Check common property names
    for key in ["bootstrap.servers", "bootstrap_servers", "bootstrapServers"]:
        if key in config:
            return config[key]

    # Check for broker list
    for key in ["broker.list", "metadata.broker.list"]:
        if key in config:
            return config[key]

    raise ValueError("Could not find bootstrap servers in config")
