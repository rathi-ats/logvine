"""Central configuration for logvine."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime settings."""

    memtable_max_size: int = 32_000_000
    controller_max_workers: int = 16


def load_settings() -> Settings:
    """Load settings from environment with safe defaults."""
    raw_max_size = os.getenv("LOGVINE_MEMTABLE_MAX_SIZE")
    raw_max_workers = os.getenv("LOGVINE_MAX_WORKERS")

    memtable_max_size = Settings.memtable_max_size
    controller_max_workers = Settings.controller_max_workers

    if raw_max_size is not None:
        try:
            parsed = int(raw_max_size)
            if parsed > 0:
                memtable_max_size = parsed
        except ValueError:
            pass

    if raw_max_workers is not None:
        try:
            parsed = int(raw_max_workers)
            if parsed > 0:
                controller_max_workers = parsed
        except ValueError:
            pass

    return Settings(
        memtable_max_size=memtable_max_size,
        controller_max_workers=controller_max_workers,
    )


SETTINGS = load_settings()
