"""
kafka_producer_pokemon.py

Step 3: Battle state & KO logic
- Maintain opponent HP using pokemon_hp.csv
- Keep the same trainer until opponent HP <= 0, emit a "win" event, then start a new battle
- Enforce legal moves only
- Emit JSON for "attack" and "win" events

Attack event:
{
  "event_type": "attack",
  "battle_id": "<uuid4>",
  "turn": <int>,
  "trainer": "<PokemonName>",
  "opponent": "<PokemonName>",
  "move": "<MoveName>",
  "damage": <int>,
  "hp_before": <int>,
  "hp_after": <int>,
  "ts": "<ISO-8601 UTC>"
}

Win event:
{
  "event_type": "win",
  "battle_id": "<uuid4>",
  "turn": <int>,  # last turn that caused the KO
  "trainer": "<PokemonName>",
  "opponent": "<PokemonName>",
  "ts": "<ISO-8601 UTC>"
}
"""

from __future__ import annotations
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
from utils.utils_producer import create_kafka_producer, get_kafka_broker_address, get_kafka_api_version
from utils.pokemon_data import load_all  # returns dataclass with allowed_moves, damage, hp, pokemon, moves


# ----------------------------
# Helpers
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
    # project_root/producers/this_file => up to project root, then /data
    return Path(__file__).resolve().parents[1] / "data"


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

    # Log what we’re connecting to, handy when debugging
    _ = get_kafka_broker_address()
    _ = get_kafka_api_version()

    topic = get_kafka_topic()
    interval = get_message_interval()

    # Load CSVs (HP, legal moves, damage)
    data_dir = get_data_dir()
    try:
        pdata = load_all(data_dir)
    except Exception as e:
        logger.error(f"[DATA] Failed to load Pokémon CSVs: {e}")
        sys.exit(3)

    # Producer with JSON serializer
    producer = create_kafka_producer(
        value_serializer=lambda obj: json.dumps(obj, separators=(",", ":")).encode("utf-8")
    )
    if not producer:
        logger.error("[KAFKA] Failed to create producer.")
        sys.exit(2)

    try:
        while True:
            # New battle
            battle_id = str(uuid.uuid4())
            trainer, opponent = pick_distinct_pair(pdata.pokemon)
            # Start opponent at full HP from CSV
            try:
                opponent_hp = int(pdata.hp[opponent])
            except Exception:
                opponent_hp = 0

            turn = 1
            logger.info(f"[BATTLE] {battle_id[:8]}: {trainer} vs {opponent} (opponent HP = {opponent_hp})")

            while True:
                move = pick_legal_move(trainer, pdata.allowed_moves)
                damage = lookup_damage(move, opponent, pdata.damage)

                hp_before = opponent_hp
                hp_after = max(0, hp_before - damage)

                attack_event = {
                    "event_type": "attack",
                    "battle_id": battle_id,
                    "turn": turn,
                    "trainer": trainer,
                    "opponent": opponent,
                    "move": move,
                    "damage": damage,
                    "hp_before": hp_before,
                    "hp_after": hp_after,
                    "ts": iso_utc_now(),
                }

                logger.info(f"[SEND] {attack_event}")
                producer.send(topic, value=attack_event)
                producer.flush(timeout=5)

                time.sleep(interval)

                # KO check
                if hp_after <= 0:
                    win_event = {
                        "event_type": "win",
                        "battle_id": battle_id,
                        "turn": turn,
                        "trainer": trainer,
                        "opponent": opponent,
                        "ts": iso_utc_now(),
                    }
                    logger.info(f"[WIN] {win_event}")
                    producer.send(topic, value=win_event)
                    producer.flush(timeout=5)
                    # End this battle loop; start a brand-new one (trainer may be different next time)
                    break

                # Continue same battle with same trainer/opponent
                opponent_hp = hp_after
                turn += 1

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
