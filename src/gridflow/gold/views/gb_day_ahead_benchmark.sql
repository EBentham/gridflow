-- Gold view: GB day-ahead benchmark (Elexon MID APXMIDP).
-- Programme ruling, 2026-09-05: use APXMIDP because ENTSO-E GB day-ahead
-- is empty post-Brexit and direct auction data requires paid access.
-- Enforce one row per settlement date/period: MID silver deduplicates within
-- each file only, so overlapping partitions can hold the same APXMIDP key.
-- Latest available_at wins; tied stamps sort by the remaining public values
-- (timestamp, price, volume, policy) for deterministic output.
-- Leakage: available_at is retained as provenance (MID has no published_at,
-- so it is an ingest-time stamp). The available_at <= as_of barrier lives
-- in gridflow_models; this view does not promise historical PIT reconstruction.
-- Row-to-JSON lookup tolerates the optional V-a vintage_policy column:
-- preserve its string value when present, otherwise expose SQL NULL.
CREATE OR REPLACE VIEW gold_gb_day_ahead_benchmark AS
SELECT
    mid.timestamp_utc,
    mid.settlement_date,
    mid.settlement_period,
    mid.market_index_price AS benchmark_price_gbp_mwh,
    mid.market_index_volume AS benchmark_volume_mwh,
    mid.data_provider_id,
    mid.available_at,
    to_json(mid)->>'vintage_policy' AS vintage_policy
FROM silver_elexon_mid mid
WHERE mid.data_provider_id = 'APXMIDP'
QUALIFY row_number() OVER (
    PARTITION BY mid.settlement_date, mid.settlement_period
    ORDER BY mid.available_at DESC NULLS LAST,
        mid.timestamp_utc DESC NULLS LAST,
        mid.market_index_price DESC NULLS LAST,
        mid.market_index_volume DESC NULLS LAST,
        to_json(mid)->>'vintage_policy' DESC NULLS LAST
) = 1
ORDER BY mid.timestamp_utc;

-- MID has no vendor publishTime; until V-a lands, availability is the ingest
-- clock. A historical cutoff cannot recover what was known before backfill.
COMMENT ON COLUMN gold_gb_day_ahead_benchmark.available_at IS
    'Provenance stamp of the winning MID row; MID has no vendor publishTime, so until V-a lands this is the ingest clock. A historical available_at <= as_of cutoff returns no backfilled rows ingested after as_of. The leakage barrier lives in gridflow_models; this view does not reconstruct historical point-in-time values.';
