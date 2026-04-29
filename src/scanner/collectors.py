"""Async REST pollers, one task per exchange.

Thin loop on top of `snapshot.snapshot_venue`. The fetch + canonicalize
logic is shared with the dashboard's live radar — this module only owns
the persistence loop + the long-lived client lifecycle.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from .snapshot import make_client, snapshot_venue
from .storage import append_rows

log = logging.getLogger(__name__)


async def run_collector(exchange_id: str, data_root: Path, interval_s: float) -> None:
    """Single venue's lifecycle. Long-lived client; isolated failures."""
    client = make_client(exchange_id)
    try:
        while True:
            t0 = time.monotonic()
            try:
                rows = await snapshot_venue(client, exchange_id)
                n = append_rows(data_root, rows)
                log.info("collector.tick exchange=%s rows=%d", exchange_id, n)
            except Exception as e:
                log.warning("collector.error exchange=%s err=%s", exchange_id, e)
            elapsed = time.monotonic() - t0
            await asyncio.sleep(max(0.0, interval_s - elapsed))
    finally:
        await client.close()


async def run_all(exchanges: list[str], data_root: Path, interval_s: float) -> None:
    data_root.mkdir(parents=True, exist_ok=True)
    tasks = [
        asyncio.create_task(run_collector(ex, data_root, interval_s), name=f"collector:{ex}")
        for ex in exchanges
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
