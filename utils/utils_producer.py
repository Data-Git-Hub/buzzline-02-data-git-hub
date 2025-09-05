"""
utils_producer.py - common functions used by producers.

Producers send messages to a Kafka topic.
"""

# ============================
# Import Modules
# ============================

import os
import sys
import time
from typing import Tuple

from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import (
    KafkaAdminClient,
    ConfigResource,
    ConfigResourceType,
    NewTopic,
)

from utils.utils_logger import logger

# ============================
# Defaults
# ============================

DEFAULT_KAFKA_BROKER_ADDRESS = "localhost:9092"
DEFAULT_API_VERSION_STR = "3.5.0"  # safe fallback that kafka-python-ng understands

# ============================
# Helpers
# ============================

def get_kafka_broker_address() -> str:
    """Fetch Kafka broker address from environment or use default."""
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", DEFAULT_KAFKA_BROKER_ADDRESS)
    logger.info(f"Kafka broker address: {broker_address}")
    return broker_address


def get_kafka_api_version() -> Tuple[int, int, int]:
    """
    Read KAFKA_API_VERSION like '3.5.0' and return (3,5,0).
    Use a conservative default so the client doesn't choke on new brokers.
    """
    s = os.getenv("KAFKA_API_VERSION", DEFAULT_API_VERSION_STR)
    try:
        parts = tuple(int(p) for p in s.split(".")[:3])
        if len(parts) == 2:
            parts = (parts[0], parts[1], 0)
    except Exception:
        parts = tuple(int(p) for p in DEFAULT_API_VERSION_STR.split("."))
    logger.info(f"Kafka API version override: {parts}")
    return parts

# ============================
# Kafka Readiness Check
# ============================

def check_kafka_service_is_ready() -> bool:
    """Try an AdminClient metadata call to verify broker is reachable."""
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, api_version=api_version)
        brokers = admin_client.describe_cluster()
        logger.info(f"Kafka is ready. Brokers: {brokers}")
        admin_client.close()
        return True
    except errors.KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


# ============================
# Producer & Topic Management
# ============================

def verify_services():
    if not check_kafka_service_is_ready():
        logger.error("Kafka broker is not ready. Please check your Kafka setup. Exiting...")
        sys.exit(2)


def create_kafka_producer(value_serializer=None):
    """
    Create and return a Kafka producer instance.
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()

    if value_serializer is None:
        def value_serializer(x):
            return x.encode("utf-8")

    try:
        logger.info(f"Connecting to Kafka broker at {kafka_broker}...")
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=value_serializer,
            api_version=api_version,
        )
        logger.info("Kafka producer successfully created.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def create_kafka_topic(topic_name: str, group_id: str | None = None) -> None:
    """
    Create a fresh Kafka topic with the given name (or clear if exists).
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, api_version=api_version)

        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' already exists. Clearing it out...")
            clear_kafka_topic(topic_name, group_id)
        else:
            logger.info(f"Creating '{topic_name}'.")
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            logger.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error managing topic '{topic_name}': {e}")
        sys.exit(1)
    finally:
        try:
            admin_client.close()
        except Exception:
            pass


def clear_kafka_topic(topic_name: str, group_id: str | None):
    """
    Consume & discard all messages (and briefly set retention to 1ms) to clear a topic.
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, api_version=api_version)

    try:
        # Read current retention
        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        configs = admin_client.describe_configs([config_resource])
        original_retention = configs[config_resource].get("retention.ms", "604800000")
        logger.info(f"Original retention.ms for '{topic_name}': {original_retention}")

        # Temp retention
        admin_client.alter_configs({config_resource: {"retention.ms": "1"}})
        logger.info(f"Temporarily set retention.ms=1 for '{topic_name}'")
        time.sleep(2)

        # Drain messages
        logger.info(f"Clearing topic '{topic_name}' by consuming all messages...")
        consumer = KafkaConsumer(
            topic_name,
            group_id=group_id,
            bootstrap_servers=kafka_broker,
            api_version=api_version,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        for _ in consumer:
            pass
        consumer.close()
        logger.info(f"All messages cleared from '{topic_name}'.")

        # Restore retention
        admin_client.alter_configs({config_resource: {"retention.ms": original_retention}})
        logger.info(f"Restored retention.ms={original_retention} for '{topic_name}'.")

    except Exception as e:
        logger.error(f"Error managing retention for '{topic_name}': {e}")
    finally:
        try:
            admin_client.close()
        except Exception:
            pass


# ============================
# Main for quick test
# ============================

def main():
    logger.info("Starting utils_producer.py script...")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Check .env file and ensure Kafka is running.")
        sys.exit(2)

    logger.info("All services are ready. Proceed with producer setup.")
    create_kafka_topic("test_topic", "default_group")


if __name__ == "__main__":
    main()
