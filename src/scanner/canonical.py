"""Canonical row schema. Normalized at the collector boundary.

Every observation produced by any collector MUST conform to CANONICAL_SCHEMA
before reaching storage. Exchange-specific idiosyncrasies must not leak past
this module.
"""
from __future__ import annotations

from typing import Any

import pyarrow as pa

HOURS_PER_YEAR = 8760

CANONICAL_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.timestamp("ms", tz="UTC")),
    pa.field("exchange", pa.string()),
    pa.field("symbol_canonical", pa.string()),     # CCXT unified, e.g. BTC/USDT:USDT
    pa.field("funding_rate", pa.float64()),         # raw per-epoch
    pa.field("funding_interval_h", pa.int8()),      # 1 | 4 | 8
    pa.field("predicted_rate", pa.float64()),
    pa.field("next_funding_ts", pa.timestamp("ms", tz="UTC")),
    pa.field("mark_price", pa.float64()),
    pa.field("index_price", pa.float64()),
    pa.field("open_interest_usd", pa.float64()),
    pa.field("volume_24h_usd", pa.float64()),
    pa.field("apy_norm", pa.float64()),             # r * 8760 / T_epoch
])


def apy_norm(funding_rate: float, interval_h: int) -> float:
    return funding_rate * (HOURS_PER_YEAR / interval_h)


def normalize(
    *,
    ts_utc_ms: int,
    exchange: str,
    symbol_canonical: str,
    funding_rate: float,
    funding_interval_h: int,
    predicted_rate: float | None,
    next_funding_ts_ms: int | None,
    mark_price: float | None,
    index_price: float | None,
    open_interest_usd: float | None,
    volume_24h_usd: float | None,
) -> dict[str, Any]:
    """Produce one canonical row. Returns a plain dict for batch construction."""
    return {
        "ts_utc": ts_utc_ms,
        "exchange": exchange,
        "symbol_canonical": symbol_canonical,
        "funding_rate": float(funding_rate),
        "funding_interval_h": int(funding_interval_h),
        "predicted_rate": float(predicted_rate) if predicted_rate is not None else None,
        "next_funding_ts": next_funding_ts_ms,
        "mark_price": float(mark_price) if mark_price is not None else None,
        "index_price": float(index_price) if index_price is not None else None,
        "open_interest_usd": float(open_interest_usd) if open_interest_usd is not None else None,
        "volume_24h_usd": float(volume_24h_usd) if volume_24h_usd is not None else None,
        "apy_norm": apy_norm(float(funding_rate), int(funding_interval_h)),
    }
