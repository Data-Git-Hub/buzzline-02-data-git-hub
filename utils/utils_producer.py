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
    Read KAFKA_API_VERSION like '3.5.0' and return (3, 5, 0).
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
# Kafka Readiness Check  (no AdminClient here)
# ============================

def check_kafka_service_is_ready(retries: int = 5, backoff_sec: float = 1.0) -> bool:
    """
    Verify broker reachability by creating a Producer and checking bootstrap connectivity.
    Avoids Admin API so we don't hit UnrecognizedBrokerVersion.
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()

    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_broker,
                api_version=api_version,
                value_serializer=lambda x: x.encode("utf-8"),
            )
            ok = producer.bootstrap_connected()
            producer.close()
            if ok:
                logger.info("Kafka is ready (bootstrap_connected = True).")
                return True
            else:
                logger.warning(f"Kafka not ready (attempt {attempt}/{retries}); retrying...")
        except Exception as e:
            logger.warning(f"Kafka readiness attempt {attempt}/{retries} failed: {e}")
        time.sleep(backoff_sec)

    logger.error("Kafka not reachable after retries.")
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
    Ensure a topic exists. Try Admin first; if Admin fails (e.g., UnrecognizedBrokerVersion),
    fall back to auto-create by sending a dummy message.
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()

    # Try Admin path
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, api_version=api_version)
        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' already exists. Clearing it out...")
            clear_kafka_topic(topic_name, group_id)
        else:
            logger.info(f"Creating topic '{topic_name}' via AdminClient.")
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            logger.info(f"Topic '{topic_name}' created successfully.")
        try:
            admin_client.close()
        except Exception:
            pass
        return
    except Exception as e:
        logger.warning(f"Admin topic management failed for '{topic_name}': {e}. Falling back to auto-create...")

    # Fallback: rely on auto.create.topics.enable by sending a "warm-up" record
    try:
        producer = create_kafka_producer()
        if not producer:
            raise RuntimeError("Producer not available for auto-create fallback.")
        logger.info(f"Auto-creating '{topic_name}' by sending a warm-up record...")
        producer.send(topic_name, value="__topic_warmup__")
        producer.flush(10)
        producer.close()
        logger.info(f"Topic '{topic_name}' should now exist (auto-create).")
    except Exception as e:
        logger.error(f"Auto-create fallback failed for '{topic_name}': {e}")
        sys.exit(1)


def clear_kafka_topic(topic_name: str, group_id: str | None):
    """
    Consume & discard all messages (and briefly set retention to 1ms) to clear a topic.
    If Admin fails, log a warning and just drain via consumer.
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()

    # Try Admin retention trick
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, api_version=api_version)
        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        configs = admin_client.describe_configs([config_resource])
        original_retention = configs[config_resource].get("retention.ms", "604800000")
        logger.info(f"Original retention.ms for '{topic_name}': {original_retention}")
        admin_client.alter_configs({config_resource: {"retention.ms": "1"}})
        logger.info(f"Temporarily set retention.ms=1 for '{topic_name}'")
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Admin retention change failed for '{topic_name}': {e}. Proceeding to drain without retention change.")
    finally:
        if admin_client:
            try:
                admin_client.close()
            except Exception:
                pass

    # Drain messages
    try:
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
    except Exception as e:
        logger.error(f"Error draining '{topic_name}': {e}")

    # Try to restore retention if we changed it
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker, api_version=api_version)
        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        # If we failed to read original earlier, this will no-op (safe).
        admin_client.alter_configs({config_resource: {"retention.ms": "604800000"}})
        logger.info(f"Retention.ms restored for '{topic_name}'.")
    except Exception:
        # It's OK if we can't restore; topic will continue with broker/topic defaults.
        pass
    finally:
        if admin_client:
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
