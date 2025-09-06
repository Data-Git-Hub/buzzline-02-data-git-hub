"""
kafka_consumer_pokemon.py

Step 3 + Metrics:
- Log natural-language lines for events.
- Validate legality of moves (should be 100% legal).
- ALERT immediately on illegal moves.
- Keep a rolling 60s window of events.
- STAGGER reports every 15s (each report covers the last 60s):
  0s:   Total KOs
  15s:  Most damage given (trainer)
  30s:  Most damage received (opponent)
  45s:  Rolling avg damage + legality rate

Env:
  KAFKA_TOPIC (default: pokemon_battles)
  KAFKA_CONSUMER_GROUP_ID_JSON (default: pokemon_verifier_group)
"""

from __future__ import annotations
import json
import os
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, Literal, Optional

from dotenv import load_dotenv

from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer
from utils.pokemon_data import load_all  # loads allowed_moves, damage, hp, pokemon, moves


AttackOrWin = Literal["attack", "win"]


@dataclass
class Event:
    ts: datetime
    type: AttackOrWin
    trainer: Optional[str] = None
    opponent: Optional[str] = None
    move: Optional[str] = None
    damage: int = 0


# ----------------------------
# Env helpers
# ----------------------------

def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "pokemon_battles")
    logger.info(f"[CONSUMER] Kafka topic: {topic}")
    return topic


def get_group_id() -> str:
    group_id = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "pokemon_verifier_group")
    logger.info(f"[CONSUMER] Group ID: {group_id}")
    return group_id


# ----------------------------
# Time helpers
# ----------------------------

def parse_ts(s: Optional[str]) -> datetime:
    """
    Parse ISO-8601; tolerate 'Z' suffix.
    If missing/invalid, use 'now' (UTC).
    """
    if not s:
        return datetime.now(timezone.utc)
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


# ----------------------------
# Rolling window metrics
# ----------------------------

class RollingWindow:
    def __init__(self, span_seconds: int = 60) -> None:
        self.span = timedelta(seconds=span_seconds)
        self.events: Deque[Event] = deque()

    def add(self, ev: Event) -> None:
        self.events.append(ev)
        self._prune(datetime.now(timezone.utc))

    def _prune(self, now: datetime) -> None:
        cutoff = now - self.span
        while self.events and self.events[0].ts < cutoff:
            self.events.popleft()

    def snapshot(self) -> list[Event]:
        self._prune(datetime.now(timezone.utc))
        return list(self.events)

    # ---- Aggregations over the last 60s ----

    def total_kos(self) -> int:
        return sum(1 for e in self.snapshot() if e.type == "win")

    def most_damage_given(self) -> tuple[Optional[str], int]:
        dmg: Dict[str, int] = defaultdict(int)
        for e in self.snapshot():
            if e.type == "attack" and e.trainer:
                dmg[e.trainer] += int(e.damage)
        if not dmg:
            return None, 0
        best = max(dmg.items(), key=lambda kv: (kv[1], kv[0]))
        return best

    def most_damage_received(self) -> tuple[Optional[str], int]:
        dmg: Dict[str, int] = defaultdict(int)
        for e in self.snapshot():
            if e.type == "attack" and e.opponent:
                dmg[e.opponent] += int(e.damage)
        if not dmg:
            return None, 0
        best = max(dmg.items(), key=lambda kv: (kv[1], kv[0]))
        return best

    def rolling_avg_damage(self) -> float:
        vals = [int(e.damage) for e in self.snapshot() if e.type == "attack"]
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def legality_rate(self, allowed_moves: Dict[str, set[str]]) -> float:
        """
        Percent of attacks that were legal in the last 60s.
        """
        attacks = [e for e in self.snapshot() if e.type == "attack"]
        if not attacks:
            return 100.0
        legal = 0
        for e in attacks:
            if e.trainer and e.move:
                if e.move in allowed_moves.get(e.trainer, set()):
                    legal += 1
        return 100.0 * legal / len(attacks)


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    logger.info("START kafka_consumer_pokemon")
    load_dotenv()

    # Load Pokémon data for legality checks
    try:
        pdata = load_all(os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
    except Exception as e:
        logger.error(f"[DATA] Failed to load Pokémon CSVs: {e}")
        return

    # Normalize allowed_moves to sets for O(1) legality checks
    allowed_moves: Dict[str, set[str]] = {
        p: set(moves) for p, moves in pdata.allowed_moves.items()
    }

    topic = get_kafka_topic()
    group = get_group_id()

    consumer = create_kafka_consumer(
        topic_provided=topic,
        group_id_provided=group,
        value_deserializer_provided=lambda b: json.loads(b.decode("utf-8")),
    )

    # Rolling 60s window + staggered reporting
    window = RollingWindow(span_seconds=60)
    report_cycle = 0  # 0: KOs, 1: dmg given, 2: dmg received, 3: avg+legality
    next_report = time.monotonic() + 15.0

    try:
        logger.info(f"[CONSUMER] Polling topic '{topic}'...")
        while True:
            # poll returns {TopicPartition: [Message, ...]}
            records = consumer.poll(timeout_ms=1000)

            # If no records, still handle the stagger timer
            for tp, msgs in (records or {}).items():
                for msg in msgs:
                    event = msg.value  # dict
                    et = event.get("event_type")

                    # Natural-language lines (per message)
                    if et == "attack":
                        trainer = event.get("trainer")
                        opponent = event.get("opponent")
                        move = event.get("move")
                        dmg = int(event.get("damage", 0))
                        hp_after = int(event.get("hp_after", 0))
                        logger.info(f"{trainer} uses {move} that does {dmg} damage to {opponent}. {opponent} has {hp_after} HP left!")

                        # Legality check (ALERT on illegal)
                        if trainer and move:
                            if move not in allowed_moves.get(trainer, set()):
                                logger.error(f"[ALERT][ILLEGAL MOVE] Trainer={trainer} used move='{move}' (not legal). Event={event}")

                        # Add to rolling window
                        ev = Event(
                            ts=parse_ts(event.get("ts")),
                            type="attack",
                            trainer=trainer,
                            opponent=opponent,
                            move=move,
                            damage=dmg,
                        )
                        window.add(ev)

                    elif et == "win":
                        trainer = event.get("trainer")
                        opponent = event.get("opponent")
                        bid_short = (event.get("battle_id", "") or "")[:8]
                        logger.info(f"{trainer} knocks out {opponent} and wins the battle ({bid_short})!")
                        ev = Event(
                            ts=parse_ts(event.get("ts")),
                            type="win",
                            trainer=trainer,
                            opponent=opponent,
                            damage=0,
                        )
                        window.add(ev)

                    else:
                        # Back-compat with Step-2 (if any)
                        trainer = event.get("trainer")
                        opponent = event.get("opponent")
                        move = event.get("move")
                        dmg = int(event.get("damage", 0))
                        logger.info(f"{trainer} uses {move} that does {dmg} damage to {opponent}.")
                        ev = Event(
                            ts=parse_ts(event.get("ts")),
                            type="attack",
                            trainer=trainer,
                            opponent=opponent,
                            move=move,
                            damage=dmg,
                        )
                        window.add(ev)

            # ---- Staggered reporting every 15s over the last 60s ----
            now_mono = time.monotonic()
            if now_mono >= next_report:
                # Ensure window pruned
                _ = window.snapshot()

                if report_cycle == 0:
                    kos = window.total_kos()
                    logger.info(f"[METRICS last 60s][KOs] total={kos}")
                elif report_cycle == 1:
                    who, dmg = window.most_damage_given()
                    if who:
                        logger.info(f"[METRICS last 60s][Most Damage Given] {who} with {dmg}")
                    else:
                        logger.info(f"[METRICS last 60s][Most Damage Given] (no data)")
                elif report_cycle == 2:
                    who, dmg = window.most_damage_received()
                    if who:
                        logger.info(f"[METRICS last 60s][Most Damage Received] {who} with {dmg}")
                    else:
                        logger.info(f"[METRICS last 60s][Most Damage Received] (no data)")
                else:
                    avg = window.rolling_avg_damage()
                    legal_rate = window.legality_rate(allowed_moves)
                    logger.info(f"[METRICS last 60s][Avg Damage] {avg:.2f} | [Legality] {legal_rate:.1f}% legal")

                report_cycle = (report_cycle + 1) % 4
                next_report += 15.0

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
