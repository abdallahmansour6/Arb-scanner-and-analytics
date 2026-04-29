"""Entrypoint. Spawns one async collector per exchange and blocks forever."""
from __future__ import annotations

import argparse
import asyncio
import logging

from . import config as cfg_mod
from .collectors import run_all


def run() -> None:
    parser = argparse.ArgumentParser(prog="scanner")
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()

    cfg = cfg_mod.load(args.config)
    logging.basicConfig(
        level=cfg.raw["runtime"]["log_level"],
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

    asyncio.run(run_all(cfg.exchanges, cfg.data_dir, cfg.poll_interval_s))


if __name__ == "__main__":
    run()
