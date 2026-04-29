"""Parquet append + DuckDB read. Append-only, partitioned by year/month/day."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .canonical import CANONICAL_SCHEMA


def _partition_path(root: Path, ts: datetime) -> Path:
    return root / f"year={ts.year}" / f"month={ts.month:02d}" / f"day={ts.day:02d}"


def append_rows(root: Path, rows: Iterable[dict]) -> int:
    """Append a batch of canonical rows. Rows from the same UTC day land in
    the same partition. Mixed days are split."""
    rows = list(rows)
    if not rows:
        return 0

    buckets: dict[tuple[int, int, int], list[dict]] = {}
    for r in rows:
        # ts_utc is ms epoch
        ts = datetime.fromtimestamp(r["ts_utc"] / 1000, tz=timezone.utc)
        buckets.setdefault((ts.year, ts.month, ts.day), []).append(r)

    written = 0
    for (y, m, d), bucket in buckets.items():
        part_dir = root / f"year={y}" / f"month={m:02d}" / f"day={d:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(bucket, schema=CANONICAL_SCHEMA)
        # One file per append batch; many small files are fine — DuckDB
        # globs and merges at query time. Compact later if needed.
        fname = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
        pq.write_table(table, part_dir / fname, compression="zstd")
        written += len(bucket)
    return written


def connect(root: Path) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with a `funding` view over the parquet tree."""
    con = duckdb.connect(":memory:")
    glob = (root / "year=*" / "month=*" / "day=*" / "*.parquet").as_posix()
    con.execute(
        f"CREATE OR REPLACE VIEW funding AS "
        f"SELECT * FROM read_parquet('{glob}', hive_partitioning = true)"
    )
    return con
