"""Live radar. Localhost-bound, single-operator.

Two decoupled data sources:
  - LIVE: direct CCXT fan-out from this laptop. Ephemeral, no persistence.
          Drives the cross-exchange spread table — the "trade right now" view.
  - HISTORY: local Parquet mirror (rsynced from the VPS). Drives the per-symbol
             history line chart. Empty if the mirror has not been hydrated yet.

Run:  streamlit run dashboard/app.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from scanner import config as cfg_mod, snapshot, storage  # noqa: E402

st.set_page_config(page_title="Funding Scanner", layout="wide")

cfg = cfg_mod.load(ROOT / "config.toml")


# ---------------------------------------------------------------------------
# Live snapshot — cached for 30s. Manual refresh button busts the cache.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30, show_spinner="Fetching live snapshot across venues…")
def fetch_live(exchanges: tuple[str, ...]) -> pd.DataFrame:
    df = asyncio.run(snapshot.snapshot_all(exchanges))
    return df


def compute_spreads(
    live: pd.DataFrame,
    *,
    min_volume_24h_usd: float,
    min_open_interest_usd: float,
    min_abs_apy: float,
    min_delta_apy: float,
) -> pd.DataFrame:
    """Per-symbol cross-exchange spread, filtered by per-leg liquidity."""
    if live.empty:
        return live

    legs = live.copy()
    if min_volume_24h_usd > 0:
        legs = legs[legs["volume_24h_usd"].fillna(0) >= min_volume_24h_usd]
    if min_open_interest_usd > 0:
        legs = legs[legs["open_interest_usd"].fillna(0) >= min_open_interest_usd]
    if min_abs_apy > 0:
        legs = legs[legs["apy_norm"].abs() >= min_abs_apy]

    if legs.empty:
        return legs

    # Per symbol, the long leg is the venue with the *lowest* APY (you receive
    # funding when long if rate is negative, or pay least when positive); the
    # short leg is the venue with the *highest* APY. delta_apy = APY captured.
    grouped = legs.groupby("symbol_canonical", as_index=False)
    rows = []
    for sym, g in grouped:
        if len(g) < 2:
            continue
        hi = g.loc[g["apy_norm"].idxmax()]
        lo = g.loc[g["apy_norm"].idxmin()]
        rows.append({
            "symbol": sym,
            "short_venue": hi["exchange"],
            "long_venue": lo["exchange"],
            "apy_short": hi["apy_norm"],
            "apy_long": lo["apy_norm"],
            "delta_apy": hi["apy_norm"] - lo["apy_norm"],
            "vol24h_short": hi["volume_24h_usd"],
            "vol24h_long": lo["volume_24h_usd"],
            "oi_short": hi["open_interest_usd"],
            "oi_long": lo["open_interest_usd"],
            "interval_short_h": hi["funding_interval_h"],
            "interval_long_h": lo["funding_interval_h"],
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    if min_delta_apy > 0:
        out = out[out["delta_apy"] >= min_delta_apy]
    return out.sort_values("delta_apy", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Sidebar — filters and refresh.
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Live filters")
    selected_exchanges = st.multiselect(
        "Exchanges", options=cfg.exchanges, default=cfg.exchanges,
    )
    min_vol = st.number_input(
        "min 24h volume (USD)", min_value=0, value=2_000_000, step=500_000,
    )
    min_oi = st.number_input(
        "min open interest (USD)", min_value=0, value=1_000_000, step=500_000,
    )
    min_abs_apy_pct = st.slider(
        "min |APY| per leg (%)", min_value=0.0, max_value=500.0, value=30.0, step=5.0,
        help="Per-leg gate. A leg must clear this before it can pair into a spread.",
    )
    min_delta_apy_pct = st.slider(
        "min Δ APY (%)", min_value=0.0, max_value=1000.0, value=50.0, step=10.0,
        help="Pair-level gate. Spread between the two legs.",
    )

    st.divider()
    if st.button("↻ Refresh live data", use_container_width=True):
        fetch_live.clear()
        st.rerun()


# ---------------------------------------------------------------------------
# Live data fetch.
# ---------------------------------------------------------------------------
if not selected_exchanges:
    st.warning("Select at least one exchange.")
    st.stop()

live = fetch_live(tuple(selected_exchanges))

if live.empty:
    st.error("No live data returned. Check network / venue availability.")
    st.stop()

snap_age_s = (datetime.now(timezone.utc) - live["ts_utc"].max().to_pydatetime()).total_seconds()
st.caption(
    f"Live snapshot: {len(live)} rows across {live['exchange'].nunique()} venues "
    f"· {snap_age_s:.0f}s old · cache TTL 30s"
)


# ---------------------------------------------------------------------------
# Tabs.
# ---------------------------------------------------------------------------
tab_spread, tab_history, tab_research = st.tabs(
    ["Cross-Exchange Spread", "Per-Symbol History", "Research"]
)

with tab_spread:
    spreads = compute_spreads(
        live,
        min_volume_24h_usd=min_vol,
        min_open_interest_usd=min_oi,
        min_abs_apy=min_abs_apy_pct / 100,
        min_delta_apy=min_delta_apy_pct / 100,
    )
    if spreads.empty:
        st.info("No spreads pass the current filters. Loosen the filters or refresh.")
    else:
        # Display APY as percent for readability.
        disp = spreads.copy()
        for c in ("apy_short", "apy_long", "delta_apy"):
            disp[c] = (disp[c] * 100).round(1)
        st.dataframe(
            disp,
            use_container_width=True,
            column_config={
                "apy_short": st.column_config.NumberColumn("apy_short %", format="%.1f"),
                "apy_long": st.column_config.NumberColumn("apy_long %", format="%.1f"),
                "delta_apy": st.column_config.NumberColumn("Δ APY %", format="%.1f"),
                "vol24h_short": st.column_config.NumberColumn("vol_short", format="$%.0f"),
                "vol24h_long": st.column_config.NumberColumn("vol_long", format="$%.0f"),
                "oi_short": st.column_config.NumberColumn("OI_short", format="$%.0f"),
                "oi_long": st.column_config.NumberColumn("OI_long", format="$%.0f"),
            },
        )

with tab_history:
    # Hydrate from the local Parquet mirror. Mirror may be empty if the user
    # hasn't run the rsync command yet (or if the VPS collector isn't running).
    mirror_has_data = cfg.data_dir.exists() and any(cfg.data_dir.rglob("*.parquet"))
    if not mirror_has_data:
        st.info(
            "Local history mirror is empty. Hydrate it from the VPS:\n\n"
            "```\nrsync -avz --append-verify <vps>:~/scanner/data/funding/ ./data/funding/\n```"
        )
    else:
        symbols_in_live = sorted(live["symbol_canonical"].unique().tolist())
        sym = st.selectbox("Symbol", options=symbols_in_live, index=0)

        con = storage.connect(cfg.data_dir)
        venues_with_history = con.execute(
            "SELECT DISTINCT exchange FROM funding WHERE symbol_canonical = ? ORDER BY exchange",
            [sym],
        ).df()["exchange"].tolist()

        if not venues_with_history:
            st.info(f"No captured history for {sym} yet.")
        else:
            picks = st.multiselect(
                "Exchanges to plot",
                options=venues_with_history,
                default=venues_with_history,
            )
            if picks:
                placeholders = ",".join(["?"] * len(picks))
                hist = con.execute(
                    f"SELECT ts_utc, exchange, apy_norm, funding_rate "
                    f"FROM funding "
                    f"WHERE symbol_canonical = ? AND exchange IN ({placeholders}) "
                    f"ORDER BY ts_utc",
                    [sym, *picks],
                ).df()
                if hist.empty:
                    st.info("No rows for the current selection.")
                else:
                    hist["apy_pct"] = hist["apy_norm"] * 100
                    fig = px.line(
                        hist, x="ts_utc", y="apy_pct", color="exchange",
                        labels={"apy_pct": "APY (%)", "ts_utc": "time (UTC)"},
                        title=f"{sym} — normalized funding APY across venues",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    with st.expander("Raw rows"):
                        st.dataframe(hist, use_container_width=True)

with tab_research:
    st.caption(
        "Anomaly persistence, breakeven epochs, and the freshness diagnostic "
        "live here. Driven off the local Parquet mirror only — empty until "
        "the mirror is hydrated."
    )
    if not (cfg.data_dir.exists() and any(cfg.data_dir.rglob("*.parquet"))):
        st.info("Hydrate the mirror to populate this tab.")
    else:
        from scanner import analytics  # local import; not load-bearing

        con = storage.connect(cfg.data_dir)
        analytics.install_views(con)
        max_age_min = st.number_input(
            "max_age_min (analytics recency)",
            min_value=1, max_value=720,
            value=cfg.analytics_max_age_min,
        )

        with st.expander("Freshness by exchange", expanded=True):
            df = analytics.freshness_by_exchange(con)
            st.dataframe(df, use_container_width=True)

        with st.expander("Anomaly candidates (persisted)"):
            a = cfg.raw["anomaly"]
            df = analytics.anomaly_candidates(
                con,
                abs_apy=a["abs_apy_threshold"],
                min_vol=a["min_volume_24h_usd"],
                persist_n=a["persistence_ticks"],
                max_age_min=max_age_min,
            )
            st.dataframe(df, use_container_width=True)

        with st.expander("Breakeven epochs"):
            b = cfg.raw["breakeven"]
            fee_total = b["default_taker_fee"] * 4
            basis_cost = b["basis_cost_bps_default"] / 10_000
            df = analytics.breakeven_epochs(
                con, taker_fee_total=fee_total, basis_cost=basis_cost,
                max_age_min=max_age_min,
            )
            st.dataframe(df, use_container_width=True)
