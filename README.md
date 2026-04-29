# Scanner

Funding-rate & basis anomaly scanner. Decoupled from the execution engine.

## Layout

    src/scanner/
      canonical.py       # locked row schema + APY normalization
      collectors.py      # async CCXT pollers (one task per venue)
      storage.py         # parquet append + duckdb view
      analytics.sql      # parameterized analytical views (source of truth)
      analytics.py       # python facade over analytics.sql
      alerts.py          # pushover hook
      main.py            # entrypoint
    dashboard/app.py     # streamlit live radar
    notebooks/           # research layer (jupyter / marimo)
    config.toml          # runtime config

## Run

    pip install -e .
    scanner --config config.toml          # collectors -> parquet
    streamlit run dashboard/app.py        # live radar

## Data

Append-only Parquet at `data/funding/year=YYYY/month=MM/day=DD/*.parquet`,
queried via DuckDB through a `funding` view.
