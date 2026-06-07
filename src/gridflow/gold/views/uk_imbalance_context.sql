-- Gold view: UK Imbalance Context
-- Combines Elexon system prices with NESO carbon intensity.
-- Provides half-hourly context for GB electricity imbalance analysis.
CREATE OR REPLACE VIEW gold_uk_imbalance_context AS
SELECT
    sp.timestamp_utc,
    sp.settlement_date,
    sp.settlement_period,
    sp.system_sell_price,
    sp.system_buy_price,
    sp.net_imbalance_volume,
    sp.run_type,
    ci.forecast_gco2_kwh   AS carbon_intensity_forecast_gco2_kwh,
    ci.actual_gco2_kwh     AS carbon_intensity_actual_gco2_kwh,
    ci.intensity_index
FROM silver_elexon_system_prices sp
LEFT JOIN silver_neso_carbon_intensity ci
    ON sp.timestamp_utc = ci.timestamp_utc
ORDER BY sp.timestamp_utc, sp.run_type;

-- Leakage foot-gun warning: carbon_intensity_actual_gco2_kwh is the REALISED
-- carbon intensity, published AFTER the settlement period it describes. It is
-- joined here on delivery time only, so it is NOT available at delivery time.
-- A model predicting the same period must NOT use it as a feature (use the
-- forecast column instead). The downstream leakage barrier (TrainingSet,
-- available_at <= as_of) lives in gridflow_models; this view does not carry a
-- per-column available_at, so treat the actual as future-realised.
COMMENT ON COLUMN gold_uk_imbalance_context.carbon_intensity_actual_gco2_kwh IS
    'REALISED carbon intensity, published after the period — NOT available at delivery time; do not use as a same-period model feature (use the forecast column).';
