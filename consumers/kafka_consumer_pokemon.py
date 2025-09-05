"""
kafka_consumer_pokemon.py

Step 3: Log natural-language lines using event payload.
- For "attack" events: "<Trainer> uses <Move> that does <D> damage to <Opponent>. <Opponent> has <HP_AFTER> HP left!"
- For "win" events:    "<Trainer> knocks out <Opponent> and wins the battle (<battle_id short>)!"
"""

from __future__ import annotations
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
            event = msg.value  # dict
            et = event.get("event_type")

            if et == "attack":
                trainer = event.get("trainer")
                opponent = event.get("opponent")
                move = event.get("move")
                dmg = int(event.get("damage", 0))
                hp_after = int(event.get("hp_after", 0))
                # Natural-language log
                logger.info(f"{trainer} uses {move} that does {dmg} damage to {opponent}. {opponent} has {hp_after} HP left!")

            elif et == "win":
                trainer = event.get("trainer")
                opponent = event.get("opponent")
                bid_short = (event.get("battle_id", "") or "")[:8]
                logger.info(f"{trainer} knocks out {opponent} and wins the battle ({bid_short})!")

            else:
                # Back-compat: if older Step-2 events arrive (no event_type), print a compact line
                trainer = event.get("trainer")
                opponent = event.get("opponent")
                move = event.get("move")
                dmg = event.get("damage")
                logger.info(f"{trainer} uses {move} that does {dmg} damage to {opponent}.")
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
