"""
utils_producer.py - common functions used by producers.

Producers send messages to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import (
    KafkaAdminClient,
    ConfigResource,
    ConfigResourceType,
    NewTopic,
)

# Import functions from local modules
from utils.utils_logger import logger


#####################################
# Default Configurations
#####################################

# Use IPv4 loopback by default to avoid ::1/IPv6 binding issues on Windows/WSL
DEFAULT_KAFKA_BROKER_ADDRESS = "127.0.0.1:9092"


#####################################
# Helper Functions
#####################################

def get_kafka_broker_address() -> str:
    """Fetch Kafka broker address from environment or use IPv4-safe default."""
    broker = os.getenv("KAFKA_BROKER_ADDRESS", DEFAULT_KAFKA_BROKER_ADDRESS)
    logger.info(f"Kafka broker address: {broker}")
    return broker


#####################################
# Kafka Readiness Check
#####################################

def check_kafka_service_is_ready() -> bool:
    """
    Check if Kafka is ready by connecting to the broker and fetching metadata.

    Returns:
        bool: True if Kafka is ready, False otherwise.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        brokers = admin_client.describe_cluster()
        logger.info(f"Kafka is ready. Brokers: {brokers}")
        admin_client.close()
        return True
    except errors.KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking Kafka: {e}")
        return False


#####################################
# Kafka Producer and Topic Management
#####################################

def verify_services() -> None:
    """Exit the process if Kafka is not reachable."""
    if not check_kafka_service_is_ready():
        logger.error("Kafka broker is not ready. Please check your Kafka setup. Exiting...")
        sys.exit(2)


def create_kafka_producer(value_serializer=None) -> KafkaProducer | None:
    """
    Create and return a Kafka producer instance.

    Args:
        value_serializer (callable): A custom serializer for message values.
                                     Defaults to UTF-8 string encoding.

    Returns:
        KafkaProducer | None: Configured Kafka producer instance or None on failure.
    """
    kafka_broker = get_kafka_broker_address()

    if value_serializer is None:
        def value_serializer(x):
            return x.encode("utf-8")  # Default to string serialization

    try:
        logger.info(f"Connecting to Kafka broker at {kafka_broker}...")
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=value_serializer,
        )
        logger.info("Kafka producer successfully created.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def create_kafka_topic(topic_name: str, group_id: str | None = None) -> None:
    """
    Ensure a Kafka topic exists; if it exists, clear it.

    Args:
        topic_name (str): Name of the Kafka topic.
        group_id (str | None): Consumer group used when clearing (optional).
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = None

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

        # Check if the topic exists
        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' already exists. Clearing it out...")
            clear_kafka_topic(topic_name, group_id)
        else:
            logger.info(f"Creating topic '{topic_name}'.")
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )
            admin_client.create_topics([new_topic])
            logger.info(f"Topic '{topic_name}' created successfully.")

    except Exception as e:
        logger.error(f"Error managing topic '{topic_name}': {e}")
        sys.exit(1)
    finally:
        if admin_client:
            admin_client.close()


def clear_kafka_topic(topic_name: str, group_id: str | None) -> None:
    """
    Consume and discard all messages in the Kafka topic to clear it.

    Args:
        topic_name (str): Name of the Kafka topic.
        group_id (str | None): Consumer group ID used for clearing.
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    try:
        # Fetch the current retention period
        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        configs = admin_client.describe_configs([config_resource])
        original_retention = configs[config_resource].get("retention.ms", "604800000")  # 7 days default
        logger.info(f"Original retention.ms for topic '{topic_name}': {original_retention}")

        # Temporarily set retention to 1ms
        admin_client.alter_configs({config_resource: {"retention.ms": "1"}})
        logger.info(f"Retention.ms temporarily set to 1ms for topic '{topic_name}'.")

        # Wait a moment for Kafka to apply retention and delete old data
        time.sleep(2)

        # Clear remaining messages by consuming and discarding them (non-blocking poll)
        logger.info(f"Clearing topic '{topic_name}' by consuming all messages...")
        consumer = KafkaConsumer(
            topic_name,
            group_id=group_id or "clear_topic_group",
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000,  # stop iteration when no new messages for 1s
        )

        # Iterate until consumer_timeout_ms triggers StopIteration
        for message in consumer:
            logger.debug(f"Clearing message at offset {message.offset}: {message.value}")

        consumer.close()
        logger.info(f"All messages cleared from topic '{topic_name}'.")

        # Restore the original retention period
        admin_client.alter_configs({config_resource: {"retention.ms": original_retention}})
        logger.info(f"Retention.ms restored to {original_retention} for topic '{topic_name}'.")

    except Exception as e:
        logger.error(f"Error managing retention for topic '{topic_name}': {e}")
    finally:
        admin_client.close()


#####################################
# Main Function for Testing
#####################################

def main() -> None:
    """
    Main entry point.
    """
    logger.info("Starting utils_producer.py script...")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Check .env file and ensure Kafka is running.")
        sys.exit(2)

    logger.info("All services are ready. Proceed with producer setup.")
    create_kafka_topic("test_topic", "default_group")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
