# utils/utils_logger.py
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Iterable

from loguru import logger

# ----------------------------
# Paths / folders
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
print(f"Log folder created at: {LOG_DIR}")  # early print in case logging not ready

# ----------------------------
# Role-based file naming
# ----------------------------
def _detect_role() -> str:
    role = os.getenv("LOG_ROLE")
    if role:
        return re.sub(r"[^A-Za-z0-9_.-]+", "-", role)[:32]

    # fallback heuristic from argv
    argv = " ".join(sys.argv).lower()
    if "consumer" in argv:
        return "consumer"
    if "producer" in argv:
        return "producer"
    return "app"

ROLE = _detect_role()
LOG_BASENAME = os.getenv("LOG_BASENAME", "project_log")
LOG_FILE = LOG_DIR / f"{LOG_BASENAME}-{ROLE}.log"

# ----------------------------
# Rotation / retention
# ----------------------------
ROTATION = os.getenv("LOG_ROTATION", "10 MB")     # size or time (e.g. "1 week")
# Keep last N rotated files per role; ignore permission errors (Windows locks)
def _make_safe_retention(keep: int):
    def _retention(files: Iterable[str]) -> None:
        files = sorted(files)
        to_delete = files[:-keep] if keep > 0 else []
        for f in to_delete:
            try:
                os.remove(f)
            except PermissionError:
                # another process or AV may briefly lock; skip silently
                pass
            except Exception:
                pass
    return _retention

KEEP = int(os.getenv("LOG_KEEP", "10"))
RETENTION = _make_safe_retention(KEEP)

# ----------------------------
# Configure Loguru
# ----------------------------
logger.remove()

# Console
logger.add(
    sys.stdout,
    level=os.getenv("LOG_LEVEL", "INFO"),
    backtrace=False,
    diagnose=False,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <7}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

# File (per-role)
logger.add(
    LOG_FILE,
    level=os.getenv("LOG_LEVEL", "INFO"),
    rotation=ROTATION,
    retention=RETENTION,
    enqueue=True,           # multiprocess-friendly
    backtrace=False,
    diagnose=False,
    encoding="utf-8",
)

logger.info(f"Logging to file: {LOG_FILE}")
logger.info("Log sanitization enabled, personal info will be removed")

__all__ = ["logger"]
