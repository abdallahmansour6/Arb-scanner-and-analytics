"""Live snapshot primitive. Single source of truth for "fetch one venue,
canonicalize the rows."

Two consumers:
  - collectors.run_collector  -> persists the rows to Parquet on a loop
  - dashboard live radar      -> renders the rows ephemerally, no persistence

Per-venue tick budget: 1x funding-rate batch (or per-symbol fallback for
venues without the unified batch endpoint) + 1x fetch_tickers
(+ 1x fetchOpenInterests where supported). All API calls are guarded so
a single venue's failure cannot orphan aiohttp sessions on its siblings.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import Iterable

import ccxt.async_support as ccxt
import pandas as pd

from .canonical import normalize

log = logging.getLogger(__name__)

# Windows + aiohttp: ProactorEventLoop (the 3.8+ default) leaks "Unclosed
# client session" warnings on shutdown. The Selector loop is the documented
# remedy and has no downside for our usage (no subprocess piping).
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Bound the per-venue per-symbol fallback fan-out. ccxt's enableRateLimit
# already throttles, but this caps wall-time on venues with hundreds of
# listings (kucoin, mexc) and avoids socket exhaustion on the laptop.
_PER_SYMBOL_CONCURRENCY = 5


def _interval_hours(exchange_id: str, symbol: str, info: dict) -> int:
    """Best-effort funding interval extraction. Falls back to 8h with a
    warning so unmatched payloads can be harvested in production logs and
    turned into deterministic per-exchange overrides over time."""
    candidates = (
        info.get("interval"),
        info.get("fundingIntervalHours"),
        info.get("fundingInterval"),
        info.get("funding_interval_hours"),
    )
    for c in candidates:
        if c is None:
            continue
        try:
            v = int(c)
            if v in (1, 2, 4, 8):
                return v
        except (TypeError, ValueError):
            pass
        s = str(c).lower()
        for h in (1, 2, 4, 8):
            if f"{h}h" in s or f"{h} hour" in s:
                return h
    log.warning(
        "interval.fallback exchange=%s symbol=%s info=%r",
        exchange_id, symbol, info,
    )
    return 8


def _row_from_ccxt(
    exchange_id: str,
    fr: dict,
    now_ms: int,
    ticker: dict | None,
    oi: dict | None,
) -> dict | None:
    symbol = fr.get("symbol")
    rate = fr.get("fundingRate")
    if symbol is None or rate is None:
        return None
    info = fr.get("info") or {}

    volume_24h_usd = ticker.get("quoteVolume") if ticker is not None else None

    open_interest_usd = None
    if oi is not None:
        open_interest_usd = oi.get("openInterestValue")
        if open_interest_usd is None:
            base_amt = oi.get("openInterestAmount")
            mark = fr.get("markPrice")
            if base_amt is not None and mark:
                open_interest_usd = float(base_amt) * float(mark)

    return normalize(
        ts_utc_ms=now_ms,
        exchange=exchange_id,
        symbol_canonical=symbol,
        funding_rate=rate,
        funding_interval_h=_interval_hours(exchange_id, symbol, info),
        predicted_rate=fr.get("nextFundingRate") or fr.get("predictedRate"),
        next_funding_ts_ms=fr.get("fundingTimestamp") or fr.get("nextFundingTimestamp"),
        mark_price=fr.get("markPrice"),
        index_price=fr.get("indexPrice"),
        open_interest_usd=open_interest_usd,
        volume_24h_usd=volume_24h_usd,
    )


async def _safe_fetch_tickers(client: ccxt.Exchange) -> dict:
    try:
        return await client.fetch_tickers()
    except Exception as e:
        log.warning("tickers.error exchange=%s err_type=%s err=%s",
                    client.id, type(e).__name__, e)
        return {}


async def _safe_fetch_open_interests(client: ccxt.Exchange) -> dict:
    if not client.has.get("fetchOpenInterests"):
        return {}
    try:
        return await client.fetch_open_interests()
    except Exception as e:
        log.warning("oi.error exchange=%s err_type=%s err=%s",
                    client.id, type(e).__name__, e)
        return {}


async def _per_symbol_funding_fallback(
    client: ccxt.Exchange, exchange_id: str
) -> dict:
    """For venues without unified batch funding, fan out per-symbol over the
    USDT-margined linear swap subset. Bounded concurrency; isolated failures."""
    try:
        await client.load_markets()
    except Exception as e:
        log.warning("markets.error exchange=%s err_type=%s err=%s",
                    exchange_id, type(e).__name__, e)
        return {}

    symbols = [
        m["symbol"] for m in client.markets.values()
        if m.get("swap") and m.get("linear")
        and m.get("settle") == "USDT" and m.get("active", True)
    ]
    if not symbols:
        return {}

    sem = asyncio.Semaphore(_PER_SYMBOL_CONCURRENCY)

    async def one(sym: str):
        async with sem:
            try:
                return sym, await client.fetch_funding_rate(sym)
            except Exception as e:
                log.debug("funding.symbol_error exchange=%s sym=%s err_type=%s err=%s",
                          exchange_id, sym, type(e).__name__, e)
                return sym, None

    pairs = await asyncio.gather(*[one(s) for s in symbols])
    out = {s: fr for s, fr in pairs if fr is not None}
    log.info("funding.fallback exchange=%s symbols_attempted=%d symbols_ok=%d",
             exchange_id, len(symbols), len(out))
    return out


async def _safe_fetch_funding_rates(
    client: ccxt.Exchange, exchange_id: str
) -> dict:
    if client.has.get("fetchFundingRates"):
        try:
            return await client.fetch_funding_rates()
        except Exception as e:
            log.warning("funding.batch_error exchange=%s err_type=%s err=%s",
                        exchange_id, type(e).__name__, e)
            return {}
    if client.has.get("fetchFundingRate"):
        return await _per_symbol_funding_fallback(client, exchange_id)
    log.warning("funding.unsupported exchange=%s — no batch nor per-symbol API",
                exchange_id)
    return {}


async def snapshot_venue(client: ccxt.Exchange, exchange_id: str) -> list[dict]:
    """One venue's full canonical snapshot. Caller owns the client lifecycle.
    All three sub-fetches are individually guarded; this function does not
    raise for venue-side failures (returns [] on total failure)."""
    now_ms = int(time.time() * 1000)
    rates, tickers, ois = await asyncio.gather(
        _safe_fetch_funding_rates(client, exchange_id),
        _safe_fetch_tickers(client),
        _safe_fetch_open_interests(client),
    )
    rows: list[dict] = []
    for symbol, fr in rates.items():
        row = _row_from_ccxt(
            exchange_id, fr, now_ms,
            ticker=tickers.get(symbol),
            oi=ois.get(symbol),
        )
        if row is not None:
            rows.append(row)
    return rows


def make_client(exchange_id: str) -> ccxt.Exchange:
    """Build a CCXT async client. Honors `CCXT_AIOHTTP_PROXY` env var
    (e.g. `socks5://127.0.0.1:1080`) so the radar can be tunneled through
    an SSH SOCKS proxy without code changes."""
    cls = getattr(ccxt, exchange_id)
    opts: dict = {"enableRateLimit": True, "options": {"defaultType": "swap"}}
    proxy = os.environ.get("CCXT_AIOHTTP_PROXY")
    if proxy:
        opts["aiohttp_proxy"] = proxy
    return cls(opts)


async def _venue_snapshot_lifecycle(exchange_id: str) -> list[dict]:
    """Open client, snapshot, close. Used by the laptop dashboard's live
    fan-out where there's no long-lived event loop to keep clients warm."""
    client = make_client(exchange_id)
    try:
        return await snapshot_venue(client, exchange_id)
    except Exception as e:
        log.warning("snapshot.error exchange=%s err_type=%s err=%s",
                    exchange_id, type(e).__name__, e)
        return []
    finally:
        try:
            await client.close()
        except Exception as e:
            log.debug("close.error exchange=%s err=%s", exchange_id, e)


async def snapshot_all(exchanges: Iterable[str]) -> pd.DataFrame:
    """Fan out across all venues concurrently. Returns one canonical
    DataFrame; venues that fail return zero rows but don't break siblings."""
    results = await asyncio.gather(
        *[_venue_snapshot_lifecycle(ex) for ex in exchanges]
    )
    flat = [row for venue_rows in results for row in venue_rows]
    if not flat:
        return pd.DataFrame()
    df = pd.DataFrame(flat)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], unit="ms", utc=True)
    return df
