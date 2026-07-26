-- Gold view: UK Imbalance Context
-- Combines Elexon system prices with NESO carbon intensity.
-- Provides half-hourly context for GB electricity imbalance analysis.
--
-- R1-A (F-01, v0.18): reads the vintage-aware silver_elexon_system_prices_latest
-- projection (ADR-025 §2) instead of the all-vintage base view, so this view
-- returns exactly one row per (settlement_date, settlement_period) even with
-- 2+ vintages on disk. Registration now depends on
-- silver_elexon_system_prices_latest existing — _register_views builds it
-- before _register_gold_views runs (storage/duckdb.py), so ordering is safe;
-- if the _latest view cannot be built (a key column drifts out of silver),
-- this view fails to register (raise under strict mode, WARNING in
-- production) rather than silently serving stacked vintages.
CREATE OR REPLACE VIEW gold_uk_imbalance_context AS
SELECT
    sp.timestamp_utc,
    sp.settlement_date,
    sp.settlement_period,
    sp.system_sell_price,
    sp.system_buy_price,
    sp.net_imbalance_volume,
    sp.price_derivation_code,
    sp.available_at,
    ci.forecast_gco2_kwh   AS carbon_intensity_forecast_gco2_kwh,
    ci.actual_gco2_kwh     AS carbon_intensity_actual_gco2_kwh,
    ci.intensity_index
FROM silver_elexon_system_prices_latest sp
LEFT JOIN silver_neso_carbon_intensity ci
    ON sp.timestamp_utc = ci.timestamp_utc
ORDER BY sp.timestamp_utc, sp.price_derivation_code;

-- Leakage foot-gun warning: carbon_intensity_actual_gco2_kwh is the REALISED
-- carbon intensity, published AFTER the settlement period it describes. It is
-- joined here on delivery time only, so it is NOT available at delivery time.
-- A model predicting the same period must NOT use it as a feature (use the
-- forecast column instead). The downstream leakage barrier (TrainingSet,
-- available_at <= as_of) lives in gridflow_models; this view does not carry a
-- per-column available_at, so treat the actual as future-realised.
COMMENT ON COLUMN gold_uk_imbalance_context.carbon_intensity_actual_gco2_kwh IS
    'REALISED carbon intensity, published after the period — NOT available at delivery time; do not use as a same-period model feature (use the forecast column).';

-- available_at is the WINNING system-price vintage's provenance stamp
-- (coalesce(published_at, ingest_time), ADR-025 §3) — it does NOT gate the
-- carbon-intensity columns above, whose realised leg remains ex-post per the
-- leakage comment. Filtering it (available_at <= as_of) is a FAIL-CLOSED
-- CUTOFF, not historical point-in-time selection (ADR-025:117-120): an
-- as_of that falls between vintages returns no row for that key, not the
-- earlier vintage. Genuine historical PIT belongs against the all-vintage
-- base view (silver_elexon_system_prices) or its deprecated
-- silver_system_prices alias, both of which still return every vintage by
-- design; that primitive is consumer-side (gridflow_models), not built here.
COMMENT ON COLUMN gold_uk_imbalance_context.available_at IS
    'Winning system-price vintage''s provenance stamp (ADR-025 §3); does not gate the carbon-intensity columns. Filtering it is a fail-closed cutoff, NOT historical point-in-time selection — an as_of between vintages returns no row. Genuine PIT needs the all-vintage base view (silver_elexon_system_prices).';
