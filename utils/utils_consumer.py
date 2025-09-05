"""
utils_consumer.py - common functions used by consumers.

Consumers subscribe to a topic and read messages from the Kafka topic.
"""

from kafka import KafkaConsumer
from utils.utils_logger import logger
from .utils_producer import get_kafka_broker_address, get_kafka_api_version

def create_kafka_consumer(
    topic_provided: str | None = None,
    group_id_provided: str | None = None,
    value_deserializer_provided=None,
):
    """
    Create and return a Kafka consumer instance.
    """
    kafka_broker = get_kafka_broker_address()
    api_version = get_kafka_api_version()
    topic = topic_provided
    consumer_group_id = group_id_provided or "test_group"

    logger.info(f"Creating Kafka consumer. Topic='{topic}' and group ID='{consumer_group_id}'.")
    logger.debug(f"Kafka broker: {kafka_broker} | API version: {api_version}")

    try:
        consumer = KafkaConsumer(
            topic,
            group_id=consumer_group_id,
            value_deserializer=value_deserializer_provided or (lambda x: x.decode("utf-8")),
            bootstrap_servers=kafka_broker,
            api_version=api_version,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        logger.info("Kafka consumer created successfully.")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise
