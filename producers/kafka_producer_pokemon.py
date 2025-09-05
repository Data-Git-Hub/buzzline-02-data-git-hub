"""
kafka_producer_pokemon.py

Produce structured Pokémon battle events to a Kafka topic.

Event schema:
{
  "battle_id": "<uuid4>",
  "turn": <int>,
  "trainer": "<PokemonName>",
  "opponent": "<PokemonName>",
  "move": "<MoveName>",
  "damage": <int>,
  "ts": "<ISO-8601 UTC timestamp>"
}

Notes:
- Uses attack matrix to ensure the trainer's move is legal for that Pokémon.
- Uses damage matrix to look up damage of (move -> opponent).
- No HP logic yet; we rotate battles after a few turns to exercise the schema.
"""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from utils.utils_logger import logger
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.pokemon_data import load_all as load_pokemon_data

def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "pokemon_battles")
    logger.info(f"[PRODUCER] Kafka topic: {topic}")
    return topic

def get_message_interval() -> float:
    raw = os.getenv("MESSAGE_INTERVAL_SECONDS", "1")
    try:
        interval = float(raw)
    except ValueError:
        interval = 1.0
    logger.info(f"[PRODUCER] Interval between events: {interval} sec")
    return interval

def get_data_dir() -> Path:
    # project_root/producers/this_file => go up to project root, then /data
    return Path(__file__).resolve().parents[1] / "data"

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def choose_battle_participants(pokemon: List[str]) -> Tuple[str, str]:
    # pick two distinct Pokémon
    trainer = random.choice(pokemon)
    opponent = random.choice([p for p in pokemon if p != trainer]) if len(pokemon) > 1 else trainer
    return trainer, opponent

def choose_legal_move(trainer: str, allowed_moves: Dict[str, List[str]]) -> str:
    legal = list(allowed_moves.get(trainer, []))
    if not legal:
        logger.warning(f"[DATA] No legal moves listed for {trainer}; falling back to any move.")
        # fallback to any known move in the dataset
        all_moves: List[str] = sorted({m for moves in allowed_moves.values() for m in moves})
        return random.choice(all_moves) if all_moves else "Mimic"
    return random.choice(legal)

def lookup_damage(move: str, opponent: str, damage_map: Dict[str, Dict[str, int]]) -> int:
    try:
        return int(damage_map.get(move, {}).get(opponent, 0))
    except Exception:
        return 0

def generate_battle_events(pokemon: List[str],
                           allowed_moves: Dict[str, List[str]],
                           damage_map: Dict[str, Dict[str, int]]):
    """
    Infinite generator that yields dict events. We keep a battle running for a few turns,
    then rotate to a new random trainer/opponent pair (no HP logic yet).
    """
    while True:
        battle_id = str(uuid.uuid4())
        turn = 1
        trainer, opponent = choose_battle_participants(pokemon)
        # 3–6 turns per battle to exercise schema
        max_turns = random.randint(3, 6)

        logger.info(f"[BATTLE] New battle {battle_id[:8]}... trainer={trainer} vs opponent={opponent} "
                    f"(up to {max_turns} turns)")

        for _ in range(max_turns):
            move = choose_legal_move(trainer, allowed_moves)
            damage = lookup_damage(move, opponent, damage_map)

            event = {
                "battle_id": battle_id,
                "turn": turn,
                "trainer": trainer,
                "opponent": opponent,
                "move": move,
                "damage": damage,
                "ts": iso_utc_now(),
            }

            yield event
            turn += 1

def main() -> None:
    logger.info("START kafka_producer_pokemon")
    load_dotenv()

    # Soft readiness check (logs a warning if not ready, but continues)
    verify_services(strict=False)

    topic = get_kafka_topic()
    interval = get_message_interval()

    # Prepare data
    data_dir = get_data_dir()
    try:
        pdata = load_pokemon_data(data_dir)
    except Exception as e:
        logger.error(f"[DATA] Failed to load Pokémon CSVs: {e}")
        sys.exit(3)

    # Ensure topic exists (uses your util)
    try:
        create_kafka_topic(topic)
        logger.info(f"[KAFKA] Topic '{topic}' ready.")
    except Exception as e:
        logger.error(f"[KAFKA] Topic error: {e}")
        sys.exit(2)

    # Producer with JSON serializer
    producer = create_kafka_producer(
        value_serializer=lambda obj: json.dumps(obj, separators=(",", ":")).encode("utf-8")
    )
    if not producer:
        logger.error("[KAFKA] Failed to create producer.")
        sys.exit(2)

    try:
        # Convert allowed_moves sets to lists for random.choice
        allowed_as_lists: Dict[str, List[str]] = {k: sorted(list(v)) for k, v in pdata.allowed_moves.items()}
        for event in generate_battle_events(pdata.pokemon, allowed_as_lists, pdata.damage):
            logger.info(f"[SEND] {event}")
            producer.send(topic, value=event)
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
