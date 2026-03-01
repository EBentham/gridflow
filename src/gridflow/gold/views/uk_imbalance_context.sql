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
FROM silver_system_prices sp
LEFT JOIN silver_carbon_intensity ci
    ON sp.timestamp_utc = ci.timestamp_utc
ORDER BY sp.timestamp_utc, sp.run_type
