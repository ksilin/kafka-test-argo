from typing import Any, Dict, List, Optional

from confluent_kafka.admin import AdminClient, NewTopic


def create_topic(
    admin_config: Dict[str, Any],
    topic_name: str,
    partitions: int = 3,
    replication_factor: int = 3,
    config: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> bool:
    """
    Create a Kafka topic

    Args:
        admin_config: Admin client configuration
        topic_name: Name of the topic to create
        partitions: Number of partitions
        replication_factor: Replication factor
        config: Topic configuration
        timeout: Operation timeout in seconds

    Returns:
        True if topic was created successfully

    Raises:
        RuntimeError: If topic creation fails
    """
    admin = AdminClient(admin_config)

    # Check if topic already exists
    existing_topics = admin.list_topics(timeout=10).topics
    if topic_name in existing_topics:
        print(f"Topic {topic_name} already exists")
        return True

    # Create the topic
    new_topics = [
        NewTopic(
            topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=config or {},
        )
    ]

    # Create the topic and wait for it to be created
    fs = admin.create_topics(new_topics, operation_timeout=timeout)

    # Wait for operation to finish
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
            return True
        except Exception as e:
            error_msg = f"Failed to create topic {topic}: {e}"
            print(f"ERROR: {error_msg}")
            raise RuntimeError(error_msg)


def delete_topic(
    admin_config: Dict[str, Any],
    topic_name: str,
    timeout: int = 30,
) -> bool:
    """
    Delete a Kafka topic

    Args:
        admin_config: Admin client configuration
        topic_name: Name of the topic to delete
        timeout: Operation timeout in seconds

    Returns:
        True if topic was deleted successfully
    """
    admin = AdminClient(admin_config)

    # Check if topic exists
    existing_topics = admin.list_topics(timeout=10).topics
    if topic_name not in existing_topics:
        print(f"Topic {topic_name} does not exist")
        return True

    # Delete the topic
    fs = admin.delete_topics([topic_name], operation_timeout=timeout)

    # Wait for operation to finish
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} deleted")
            return True
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")
            return False


def list_topics(
    admin_config: Dict[str, Any],
    timeout: int = 10,
) -> List[str]:
    """
    List all Kafka topics

    Args:
        admin_config: Admin client configuration
        timeout: Operation timeout in seconds

    Returns:
        List of topic names
    """
    admin = AdminClient(admin_config)
    topic_metadata = admin.list_topics(timeout=timeout)
    return list(topic_metadata.topics.keys())
