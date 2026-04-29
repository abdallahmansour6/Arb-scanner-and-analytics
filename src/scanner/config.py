from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    raw: dict
    root: Path

    @property
    def data_dir(self) -> Path:
        return self.root / self.raw["runtime"]["data_dir"]

    @property
    def poll_interval_s(self) -> float:
        return float(self.raw["runtime"]["poll_interval_s"])

    @property
    def analytics_max_age_min(self) -> int:
        return int(self.raw["runtime"]["analytics_max_age_min"])

    @property
    def exchanges(self) -> list[str]:
        return list(self.raw["exchanges"]["enabled"])

    def taker_fee(self, exchange: str) -> float:
        overrides = self.raw["breakeven"].get("taker_fee_overrides", {})
        return float(overrides.get(exchange, self.raw["breakeven"]["default_taker_fee"]))


def load(path: str | Path = "config.toml") -> Config:
    p = Path(path).resolve()
    with p.open("rb") as f:
        raw = tomllib.load(f)
    return Config(raw=raw, root=p.parent)
