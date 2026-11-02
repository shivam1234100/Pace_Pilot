"""Environment-driven settings. Every value has a default so the app runs locally as-is."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

PAUSE_STREAM = "pp:pause_stream"
PAUSE_GROUP = "pausers"

CounterMode = Literal["atomic", "naive"]


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_mode() -> CounterMode:
    mode = os.environ.get("COUNTER_MODE", "atomic")
    if mode not in ("atomic", "naive"):
        raise ValueError(f"COUNTER_MODE must be 'atomic' or 'naive', got {mode!r}")
    return mode  # type: ignore[return-value]


@dataclass(frozen=True)
class Settings:
    database_url: str = field(
        default_factory=lambda: _env_str(
            "DATABASE_URL", "postgresql://pace:pace@localhost:5432/pace"
        )
    )
    redis_url: str = field(default_factory=lambda: _env_str("REDIS_URL", "redis://localhost:6379/0"))
    counter_mode: CounterMode = field(default_factory=_env_mode)
    pause_threshold: float = field(default_factory=lambda: _env_float("PAUSE_THRESHOLD", 0.98))
    dedupe_ttl_s: int = field(default_factory=lambda: _env_int("DEDUPE_TTL_S", 172800))
    pacing_interval_s: float = field(default_factory=lambda: _env_float("PACING_INTERVAL_S", 30))
    overpace_factor: float = field(default_factory=lambda: _env_float("OVERPACE_FACTOR", 1.25))
    mock_failure_rate: float = field(default_factory=lambda: _env_float("MOCK_FAILURE_RATE", 0.1))


def get_settings() -> Settings:
    return Settings()
