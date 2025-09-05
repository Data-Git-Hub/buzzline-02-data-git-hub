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

# Standard library
import csv
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

# External
from dotenv import load_dotenv

# Local
from utils.utils_logger import logger
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)


# ----------------------------
# Config / Environment helpers
# ----------------------------

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


# ----------------------------
# Data Loading
# ----------------------------

class PokemonData:
    """
    Loads:
    - pokemon_attack.csv (legal moves matrix; 1 = legal)
    - pokemon_damage.csv (damage matrix; integer)
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.pokemon: List[str] = []
        self.moves: List[str] = []
        self.allowed_moves: Dict[str, List[str]] = {}       # pokemon -> [moves]
        self.damage: Dict[str, Dict[str, int]] = {}         # move -> opponent -> damage

    def _read_matrix(self, filename: str) -> Tuple[List[str], List[str], List[List[str]]]:
        """
        Returns (col_headers, row_headers, rows) where:
          - col_headers: list of Pokémon (from CSV header, skipping the first blank cell)
          - row_headers: list of move names (first column per row)
          - rows: list of full rows (including the first cell which is the row header)
        """
        path = self.data_dir / filename
        if not path.exists():
            logger.error(f"[DATA] Missing file: {path}")
            raise FileNotFoundError(path)

        with path.open(newline="", encoding="utf-8") as f:
            reader = list(csv.reader(f))
        if not reader:
            raise ValueError(f"[DATA] Empty CSV: {path}")

        header = reader[0]
        # Example header: ["", "Pikachu", "Charmander", ...]
        col_headers = header[1:]  # skip first blank
        row_headers = []
        rows = []

        for r in reader[1:]:
            if not r:
                continue
            row_headers.append(r[0])
            rows.append(r)

        return col_headers, row_headers, rows

    def load(self) -> None:
        # Load attack matrix
        cols, move_names, rows = self._read_matrix("pokemon_attack.csv")
        self.pokemon = cols
        self.moves = move_names

        # Initialize allowed moves mapping
        self.allowed_moves = {pkmn: [] for pkmn in self.pokemon}

        for r in rows:
            move = r[0]
            cells = r[1:]  # align with self.pokemon
            for idx, flag in enumerate(cells):
                pkmn = self.pokemon[idx]
                try:
                    if int(flag) == 1:
                        self.allowed_moves[pkmn].append(move)
                except ValueError:
                    # Non-integer cell; treat as not allowed
                    continue

        # Load damage matrix
        cols_dmg, move_names_dmg, rows_dmg = self._read_matrix("pokemon_damage.csv")

        # sanity: Pokémon columns should match between files
        if cols_dmg != self.pokemon or move_names_dmg != self.moves:
            logger.warning("[DATA] Attack and Damage matrices have different ordering or names. "
                           "Proceeding by names but ensure CSVs are aligned.")

        # Build nested dict: damage[move][opponent] = int
        self.damage = {move: {} for move in self.moves}
        for r in rows_dmg:
            move = r[0]
            cells = r[1:]
            for idx, val in enumerate(cells):
                opponent = self.pokemon[idx]
                try:
                    self.damage[move][opponent] = int(val)
                except ValueError:
                    self.damage[move][opponent] = 0  # treat non-number as 0

        logger.info(f"[DATA] Loaded Pokémon: {len(self.pokemon)}; moves: {len(self.moves)}")
        # Small preview
        sample_pk = random.choice(self.pokemon)
        logger.info(f"[DATA] Example allowed moves for {sample_pk}: {self.allowed_moves.get(sample_pk, [])}")


# ----------------------------
# Event generation
# ----------------------------

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def choose_battle_participants(pokemon: List[str]) -> Tuple[str, str]:
    # pick two distinct Pokémon
    trainer = random.choice(pokemon)
    opponent = random.choice([p for p in pokemon if p != trainer]) if len(pokemon) > 1 else trainer
    return trainer, opponent


def choose_legal_move(trainer: str, allowed_moves: Dict[str, List[str]]) -> str:
    legal = allowed_moves.get(trainer, [])
    if not legal:
        # Fallback if CSV row was empty: allow any move (should be rare)
        logger.warning(f"[DATA] No legal moves listed for {trainer}; falling back to any move.")
        # This fallback list is intentionally conservative: pick a neutral move if present.
        for fallback in ("Body Slam", "Shadow Ball", "Quick Attack", "Mimic"):
            if fallback in legal:
                return fallback
        return random.choice(sum(allowed_moves.values(), [])) if allowed_moves else "Mimic"
    return random.choice(legal)


def lookup_damage(move: str, opponent: str, damage_map: Dict[str, Dict[str, int]]) -> int:
    try:
        return int(damage_map.get(move, {}).get(opponent, 0))
    except Exception:
        return 0


def generate_battle_events(data: PokemonData):
    """
    Infinite generator that yields dict events. We keep a battle running for a few turns,
    then rotate to a new random trainer/opponent pair (no HP logic yet).
    """
    while True:
        battle_id = str(uuid.uuid4())
        turn = 1
        trainer, opponent = choose_battle_participants(data.pokemon)
        # 3–6 turns per battle to exercise schema
        max_turns = random.randint(3, 6)

        logger.info(f"[BATTLE] New battle {battle_id[:8]}... trainer={trainer} vs opponent={opponent} "
                    f"(up to {max_turns} turns)")

        for _ in range(max_turns):
            move = choose_legal_move(trainer, data.allowed_moves)
            damage = lookup_damage(move, opponent, data.damage)

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


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    logger.info("START kafka_producer_pokemon")
    load_dotenv()
    verify_services()

    topic = get_kafka_topic()
    interval = get_message_interval()

    # Prepare data
    data_dir = get_data_dir()
    pdata = PokemonData(data_dir)
    try:
        pdata.load()
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
        for event in generate_battle_events(pdata):
            logger.info(f"[SEND] {event}")
            producer.send(topic, value=event)
            # Flush lightly to surface errors without blocking too much
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
