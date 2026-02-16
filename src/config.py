"""Central configuration for logvine."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime settings."""

    memtable_max_size: int = 32_000_000 # 32 MB default max size for MemTable before flush


def load_settings() -> Settings:
    """Load settings from environment with safe defaults."""
    raw_max_size = os.getenv("LOGVINE_MEMTABLE_MAX_SIZE")
    if raw_max_size is None:
        return Settings()

    try:
        parsed = int(raw_max_size)
    except ValueError:
        parsed = 1_000_000

    if parsed <= 0:
        parsed = 1_000_000

    return Settings(memtable_max_size=parsed)


SETTINGS = load_settings()
