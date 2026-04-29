"""Thin Python facade over analytics.sql. SQL stays the source of truth."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

_SQL_PATH = Path(__file__).with_name("analytics.sql")


def install_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_SQL_PATH.read_text())


def latest_funding(con: duckdb.DuckDBPyConnection, *, max_age_min: int) -> pd.DataFrame:
    return con.execute("SELECT * FROM latest_funding(?)", [max_age_min]).df()


def anomaly_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    abs_apy: float,
    min_vol: float,
    persist_n: int,
    max_age_min: int,
) -> pd.DataFrame:
    return con.execute(
        "SELECT * FROM anomaly_candidates(?, ?, ?, ?)",
        [abs_apy, min_vol, persist_n, max_age_min],
    ).df()


def cross_exchange_delta(
    con: duckdb.DuckDBPyConnection, *, max_age_min: int
) -> pd.DataFrame:
    return con.execute(
        "SELECT * FROM cross_exchange_delta(?)", [max_age_min]
    ).df()


def breakeven_epochs(
    con: duckdb.DuckDBPyConnection,
    *,
    taker_fee_total: float,
    basis_cost: float,
    max_age_min: int,
) -> pd.DataFrame:
    return con.execute(
        "SELECT * FROM breakeven_epochs(?, ?, ?)",
        [taker_fee_total, basis_cost, max_age_min],
    ).df()


def freshness_by_exchange(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("SELECT * FROM freshness_by_exchange").df()
