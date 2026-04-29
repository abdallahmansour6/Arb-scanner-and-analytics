-- Parameterized analytical views over the `funding` parquet tree.
-- Loaded by analytics.py and executed against an existing DuckDB connection.
-- All views are read-only and re-derivable; nothing here mutates state.
--
-- Recency contract: every analytical macro takes `max_age_min` and discards
-- observations older than `current_timestamp - INTERVAL max_age_min MINUTE`.
-- This prevents a crashed collector or a delisted symbol from leaving stale
-- "latest" rows that would otherwise skew anomaly / delta / breakeven views.
-- ts_utc is stored as TIMESTAMPTZ(ms, UTC); current_timestamp is TIMESTAMPTZ;
-- comparison is timezone-correct.

-- The most recent fresh observation per (exchange, symbol).
CREATE OR REPLACE MACRO latest_funding(max_age_min) AS TABLE
SELECT * EXCLUDE rn FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY exchange, symbol_canonical
               ORDER BY ts_utc DESC
           ) AS rn
    FROM funding
    WHERE ts_utc >= current_timestamp - (INTERVAL '1 minute') * max_age_min
)
WHERE rn = 1;

-- Anomaly candidates: |APY_norm| above threshold, sufficient volume, and
-- persisted across N consecutive fresh observations.
CREATE OR REPLACE MACRO anomaly_candidates(abs_apy, min_vol, persist_n, max_age_min) AS TABLE
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY exchange, symbol_canonical
               ORDER BY ts_utc DESC
           ) AS rn
    FROM funding
    WHERE ts_utc >= current_timestamp - (INTERVAL '1 minute') * max_age_min
),
recent AS (
    SELECT * FROM ranked WHERE rn <= persist_n
),
qualifying AS (
    SELECT exchange, symbol_canonical
    FROM recent
    GROUP BY exchange, symbol_canonical
    HAVING count(*) = persist_n
       AND min(abs(apy_norm)) >= abs_apy
       AND min(coalesce(volume_24h_usd, 0)) >= min_vol
)
SELECT l.*
FROM latest_funding(max_age_min) l
JOIN qualifying q USING (exchange, symbol_canonical)
ORDER BY abs(l.apy_norm) DESC;

-- Cross-exchange delta: for each symbol listed on >= 2 fresh venues, max-min APY.
CREATE OR REPLACE MACRO cross_exchange_delta(max_age_min) AS TABLE
WITH per_symbol AS (
    SELECT symbol_canonical,
           count(*)               AS venue_count,
           max(apy_norm)          AS apy_max,
           min(apy_norm)          AS apy_min,
           arg_max(exchange, apy_norm) AS venue_max,
           arg_min(exchange, apy_norm) AS venue_min
    FROM latest_funding(max_age_min)
    GROUP BY symbol_canonical
)
SELECT symbol_canonical,
       venue_max,
       venue_min,
       apy_max,
       apy_min,
       (apy_max - apy_min) AS delta_apy
FROM per_symbol
WHERE venue_count >= 2
ORDER BY delta_apy DESC;

-- Breakeven epochs: E_BE = (basis_cost + fees) / epoch_yield
CREATE OR REPLACE MACRO breakeven_epochs(taker_fee_total, basis_cost, max_age_min) AS TABLE
WITH lf AS (SELECT * FROM latest_funding(max_age_min)),
delta AS (SELECT * FROM cross_exchange_delta(max_age_min)),
pairs AS (
    SELECT
        d.symbol_canonical,
        d.venue_max,
        d.venue_min,
        d.apy_max,
        d.apy_min,
        l_max.funding_rate         AS r_max,
        l_min.funding_rate         AS r_min,
        l_max.funding_interval_h   AS interval_max_h,
        l_min.funding_interval_h   AS interval_min_h,
        least(l_max.funding_interval_h, l_min.funding_interval_h) AS binding_interval_h,
        ((l_max.funding_rate / l_max.funding_interval_h)
         - (l_min.funding_rate / l_min.funding_interval_h))
            * least(l_max.funding_interval_h, l_min.funding_interval_h)
            AS epoch_yield
    FROM delta d
    JOIN lf l_max
      ON l_max.symbol_canonical = d.symbol_canonical AND l_max.exchange = d.venue_max
    JOIN lf l_min
      ON l_min.symbol_canonical = d.symbol_canonical AND l_min.exchange = d.venue_min
)
SELECT
    symbol_canonical,
    venue_max AS short_venue,
    venue_min AS long_venue,
    epoch_yield,
    binding_interval_h,
    (taker_fee_total + basis_cost) / nullif(epoch_yield, 0) AS breakeven_epochs
FROM pairs
WHERE epoch_yield > 0
ORDER BY breakeven_epochs ASC;

-- Diagnostic: per-exchange freshness. Use to empirically set max_age_min:
--   - lag_minutes near the poll cadence for healthy collectors
--   - large lag_minutes => crashed collector or rate-limit backoff
--   - missing exchange entirely => never produced rows since process start
-- Driven off ALL data (no recency filter) so it remains observable when
-- the analytical layer would have already filtered the venue out.
CREATE OR REPLACE VIEW freshness_by_exchange AS
SELECT
    exchange,
    count(*)                                                     AS rows_total,
    count(DISTINCT symbol_canonical)                             AS symbols_seen,
    max(ts_utc)                                                  AS last_ts,
    date_diff('minute', max(ts_utc), current_timestamp)          AS lag_minutes
FROM funding
GROUP BY exchange
ORDER BY lag_minutes ASC;
