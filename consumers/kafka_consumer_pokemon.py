"""
kafka_consumer_pokemon.py

Consumes Pokémon battle events and logs the parsed JSON.
"""

import json
import os

from dotenv import load_dotenv

from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer


def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "pokemon_battles")
    logger.info(f"[CONSUMER] Kafka topic: {topic}")
    return topic


def get_group_id() -> str:
    group_id = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "pokemon_verifier_group")
    logger.info(f"[CONSUMER] Group ID: {group_id}")
    return group_id


def main() -> None:
    logger.info("START kafka_consumer_pokemon")
    load_dotenv()

    topic = get_kafka_topic()
    group = get_group_id()

    consumer = create_kafka_consumer(
        topic_provided=topic,
        group_id_provided=group,
        value_deserializer_provided=lambda b: json.loads(b.decode("utf-8")),
    )

    try:
        logger.info(f"[CONSUMER] Polling topic '{topic}'...")
        for msg in consumer:
            event = msg.value  # already a dict
            logger.info(
                f"[EVENT] battle={event.get('battle_id','')[:8]} "
                f"turn={event.get('turn')} "
                f"{event.get('trainer')} -> {event.get('opponent')} "
                f"move={event.get('move')} dmg={event.get('damage')} "
                f"ts={event.get('ts')}"
            )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"[CONSUMER] Error: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("END kafka_consumer_pokemon")


if __name__ == "__main__":
    main()
