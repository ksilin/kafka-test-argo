import os
import tempfile

import pytest

from utils.config import get_bootstrap_servers, parse_client_config


class TestConfigUtils:
    def test_parse_properties_file(self):
        """Test parsing a properties file"""
        with tempfile.NamedTemporaryFile(
            suffix=".properties", mode="w", delete=False
        ) as f:
            f.write(
                """
            # Kafka configuration
            bootstrap.servers=localhost:9092
            security.protocol=PLAINTEXT
            client.id=test-client
            """
            )
            config_file = f.name

        try:
            config = parse_client_config(config_file)

            assert config is not None
            assert isinstance(config, dict)
            assert config.get("bootstrap.servers") == "localhost:9092"
            assert config.get("security.protocol") == "PLAINTEXT"
            assert config.get("client.id") == "test-client"
        finally:
            # Clean up
            os.unlink(config_file)

    def test_parse_yaml_file(self):
        """Test parsing a YAML file"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            f.write(
                """
            # Kafka configuration
            bootstrap.servers: localhost:9092
            security.protocol: PLAINTEXT
            client.id: test-client
            """
            )
            config_file = f.name

        try:
            config = parse_client_config(config_file)

            assert config is not None
            assert isinstance(config, dict)
            assert config.get("bootstrap.servers") == "localhost:9092"
            assert config.get("security.protocol") == "PLAINTEXT"
            assert config.get("client.id") == "test-client"
        finally:
            # Clean up
            os.unlink(config_file)

    def test_file_not_found(self):
        """Test exception when file is not found"""
        with pytest.raises(FileNotFoundError):
            parse_client_config("/path/to/nonexistent/file.properties")

    def test_get_bootstrap_servers(self):
        """Test extracting bootstrap servers from config"""
        # Test with bootstrap.servers
        config = {"bootstrap.servers": "localhost:9092"}
        assert get_bootstrap_servers(config) == "localhost:9092"

        # Test with bootstrap_servers
        config = {"bootstrap_servers": "broker1:9092,broker2:9092"}
        assert get_bootstrap_servers(config) == "broker1:9092,broker2:9092"

        # Test with metadata.broker.list
        config = {"metadata.broker.list": "kafka:9092"}
        assert get_bootstrap_servers(config) == "kafka:9092"

        # Test with no bootstrap servers
        config = {"client.id": "test-client"}
        with pytest.raises(ValueError):
            get_bootstrap_servers(config)
