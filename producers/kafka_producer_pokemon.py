"""
kafka_producer_pokemon.py

Emits structured Pokémon battle events to Kafka.

Event:
{
  "battle_id": "<uuid4>",
  "turn": <int>,
  "trainer": "<PokemonName>",
  "opponent": "<PokemonName>",
  "move": "<MoveName>",
  "damage": <int>,
  "ts": "<ISO-8601 UTC timestamp>"
}
"""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from utils.utils_logger import logger
from utils.utils_producer import create_kafka_producer
from utils.pokemon_data import load_all  # uses your CSV loader (no HP logic used here)


# ----------------------------
# Small helpers
# ----------------------------

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "pokemon_battles")
    logger.info(f"[PRODUCER] Kafka topic: {topic}")
    return topic


def get_message_interval() -> float:
    raw = os.getenv("MESSAGE_INTERVAL_SECONDS", "1")
    try:
        return float(raw)
    except ValueError:
        return 1.0


def get_data_dir() -> Path:
    # project_root/producers/this_file => go up to project root, then /data
    return Path(__file__).resolve().parents[1] / "data"


# ----------------------------
# Core battle logic (no HP)
# ----------------------------

def pick_distinct_pair(items: list[str]) -> tuple[str, str]:
    trainer = random.choice(items)
    if len(items) == 1:
        return trainer, trainer
    opponent = random.choice([x for x in items if x != trainer])
    return trainer, opponent


def pick_legal_move(trainer: str, allowed_moves: dict[str, set[str]]) -> str:
    legal = list(allowed_moves.get(trainer, set()))
    if not legal:
        logger.warning(f"[DATA] No legal moves for {trainer}. Falling back to any available move.")
        # Flatten all moves and pick one
        all_moves = sorted({m for moves in allowed_moves.values() for m in moves})
        return random.choice(all_moves) if all_moves else "Mimic"
    return random.choice(legal)


def lookup_damage(move: str, opponent: str, damage_map: dict[str, dict[str, int]]) -> int:
    try:
        return int(damage_map.get(move, {}).get(opponent, 0))
    except Exception:
        return 0


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    logger.info("START kafka_producer_pokemon")
    load_dotenv()

    topic = get_kafka_topic()
    interval = get_message_interval()

    # Load CSVs (no HP used for Step 2)
    data_dir = get_data_dir()
    try:
        pdata = load_all(data_dir)
    except Exception as e:
        logger.error(f"[DATA] Failed to load Pokémon CSVs: {e}")
        sys.exit(3)

    # Create producer (JSON serializer)
    producer = create_kafka_producer(
        value_serializer=lambda obj: json.dumps(obj, separators=(',', ':')).encode("utf-8")
    )
    if not producer:
        logger.error("[KAFKA] Failed to create producer.")
        sys.exit(2)

    # Emit events forever: short battles (3–6 turns) to exercise schema
    try:
        while True:
            battle_id = str(uuid.uuid4())
            trainer, opponent = pick_distinct_pair(pdata.pokemon)
            max_turns = random.randint(3, 6)

            logger.info(f"[BATTLE] {battle_id[:8]}: {trainer} vs {opponent} (up to {max_turns} turns)")

            for turn in range(1, max_turns + 1):
                move = pick_legal_move(trainer, pdata.allowed_moves)
                damage = lookup_damage(move, opponent, pdata.damage)

                event = {
                    "battle_id": battle_id,
                    "turn": turn,
                    "trainer": trainer,
                    "opponent": opponent,
                    "move": move,
                    "damage": damage,
                    "ts": iso_utc_now(),
                }

                logger.info(f"[SEND] {event}")
                producer.send(topic, value=event)
                # Flush lightly so errors surface but we keep good throughput
                producer.flush(timeout=5)

                time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"[PRODUCER] Error during send loop: {e}")
    finally:
        try:
            producer.close()
        except Exception:
            pass
        logger.info("END kafka_producer_pokemon")


if __name__ == "__main__":
    main()
