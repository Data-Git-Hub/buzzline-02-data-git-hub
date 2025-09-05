"""
utils/pokemon_data.py

Load Pokémon battle data from CSV files:
- pokemon_attack.csv : which moves each Pokémon is allowed to use
- pokemon_damage.csv : damage per move against each defender Pokémon
- pokemon_hp.csv     : base HP per Pokémon defender

Provides:
- load_all(data_dir) -> PokemonData
- data.allowed_moves[pokemon] : set[str] of legal moves
- data.damage[move][defender] : int damage
- data.hp[pokemon]            : int base HP
- data.pokemon                : sorted list of canonical Pokémon names
- data.moves                  : sorted list of move names

Notes:
- Normalizes known typos (e.g., 'Jigglyfuff' -> 'Jigglypuff').
- Validates consistency across files and raises ValueError on mismatch.
"""

from __future__ import annotations
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set
from loguru import logger

# Known header normalization (handles typos in supplied CSVs)
_SPECIES_NORMALIZE = {
    "Jigglyfuff": "Jigglypuff",
    "Jigglypuff": "Jigglypuff",
}

def _canon_species(name: str) -> str:
    name = name.strip()
    return _SPECIES_NORMALIZE.get(name, name)

@dataclass
class PokemonData:
    allowed_moves: Dict[str, Set[str]]
    damage: Dict[str, Dict[str, int]]
    hp: Dict[str, int]
    pokemon: List[str]
    moves: List[str]


def _load_attack_matrix(path: Path) -> tuple[Dict[str, Set[str]], List[str], List[str]]:
    """
    Returns (allowed_moves, moves, species)
    allowed_moves[pokemon] -> set of legal move names
    """
    allowed_moves: Dict[str, Set[str]] = {}
    moves: List[str] = []
    species: List[str] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            raise ValueError(f"{path} is empty")

        # Row 0: ["", Pikachu, Charmander, ...]
        header = rows[0]
        if not header or header[0] != "":
            # some CSV editors may save header[0] empty or 'Move'
            # tolerate 'Move'
            if not header or header[0].lower() != "move":
                logger.warning(f"First cell of header in {path} is '{header[0] if header else None}', expected '' or 'Move'")
        species = [_canon_species(h) for h in header[1:]]

        for r in rows[1:]:
            if not r:
                continue
            move = r[0].strip()
            if not move:
                continue
            moves.append(move)
            # For each species column, '1' means allowed
            for i, s in enumerate(species, start=1):
                val = r[i].strip() if i < len(r) else "0"
                if val == "1":
                    allowed_moves.setdefault(s, set()).add(move)

    # Ensure every species key exists (even if empty)
    for s in species:
        allowed_moves.setdefault(s, set())

    return allowed_moves, moves, species


def _load_damage_table(path: Path) -> tuple[Dict[str, Dict[str, int]], List[str], List[str]]:
    """
    Returns (damage, moves, species)
    damage[move][defender] -> int
    """
    damage: Dict[str, Dict[str, int]] = {}
    moves: List[str] = []
    species: List[str] = []

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            raise ValueError(f"{path} is empty")

        header = rows[0]
        species = [_canon_species(h) for h in header[1:]]

        for r in rows[1:]:
            if not r:
                continue
            move = r[0].strip()
            if not move:
                continue
            moves.append(move)
            damage[move] = {}
            for i, s in enumerate(species, start=1):
                val = r[i].strip() if i < len(r) else "0"
                try:
                    damage[move][s] = int(val)
                except ValueError:
                    raise ValueError(f"Non-integer damage in {path} at move '{move}', defender '{s}': '{val}'")

    return damage, moves, species


def _load_hp_table(path: Path) -> Dict[str, int]:
    """
    Returns hp[pokemon] -> int
    Format:
      header row: species names
      second row: integers
    """
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if len(rows) < 2:
            raise ValueError(f"{path} must have at least two rows (header + values)")
        header = [_canon_species(h) for h in rows[0]]
        values = rows[1]
        if len(values) != len(header):
            raise ValueError(f"{path} value row length {len(values)} != header length {len(header)}")
        hp: Dict[str, int] = {}
        for s, v in zip(header, values):
            try:
                hp[s] = int(v.strip())
            except ValueError:
                raise ValueError(f"Non-integer HP for '{s}' in {path}: '{v}'")
        return hp


def load_all(data_dir: str | Path) -> PokemonData:
    data_dir = Path(data_dir)
    attack_csv = data_dir / "pokemon_attack.csv"
    damage_csv = data_dir / "pokemon_damage.csv"
    hp_csv = data_dir / "pokemon_hp.csv"

    allowed_moves, moves_a, species_a = _load_attack_matrix(attack_csv)
    damage, moves_d, species_d = _load_damage_table(damage_csv)
    hp = _load_hp_table(hp_csv)

    species_hp = list(hp.keys())

    # Validate species alignment
    set_a = set(species_a)
    set_d = set(species_d)
    set_h = set(species_hp)
    if set_a != set_d or set_a != set_h:
        raise ValueError(
            "Species mismatch across CSVs:\n"
            f"  attack.csv: {sorted(set_a)}\n"
            f"  damage.csv: {sorted(set_d)}\n"
            f"  hp.csv    : {sorted(set_h)}"
        )

    # Validate moves alignment between attack and damage tables
    set_moves_a = set(moves_a)
    set_moves_d = set(moves_d)
    if set_moves_a != set_moves_d:
        # Not fatal, but highly recommended to keep identical
        logger.warning(
            "Move lists differ between attack and damage tables.\n"
            f"  attack-only moves: {sorted(set_moves_a - set_moves_d)}\n"
            f"  damage-only moves: {sorted(set_moves_d - set_moves_a)}"
        )

    # Sort canonical lists
    species_sorted = sorted(set_a)
    moves_sorted = sorted(set_moves_a | set_moves_d)

    # Ensure every listed move has a damage row for every defender
    for m in moves_sorted:
        if m not in damage:
            damage[m] = {}
        for s in species_sorted:
            damage[m].setdefault(s, 0)

    logger.info("Loaded Pokémon data successfully.")
    logger.info(f"Species: {species_sorted}")
    logger.info(f"Moves: {moves_sorted}")
    # Summaries per Pokémon
    for s in species_sorted:
        logger.info(f"{s}: HP={hp[s]}, legal_moves={sorted(allowed_moves.get(s, set()))}")

    return PokemonData(
        allowed_moves=allowed_moves,
        damage=damage,
        hp=hp,
        pokemon=species_sorted,
        moves=moves_sorted,
    )


if __name__ == "__main__":
    # Smoke test: run from project root with:
    #   py -m utils.pokemon_data
    data = load_all("data")
    print("\nSummary")
    print("-------")
    print(f"Pokémon: {', '.join(data.pokemon)}")
    print(f"Moves  : {', '.join(data.moves)}")
    for s in data.pokemon:
        print(f"- {s}: HP={data.hp[s]}, moves={', '.join(sorted(data.allowed_moves[s]))}")
