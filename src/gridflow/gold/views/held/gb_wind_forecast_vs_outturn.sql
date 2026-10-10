-- Gold view: GB day-ahead national wind forecast versus metered wind output (v0.22-G, ADR-041).
-- Held: unregistered; hold reasons are gridflow.gold.contracts.hold_reasons().
-- Forecast side: every complete vintage of da_wind_forecast_day_ahead, one row per silver
-- row of each capture whose completion record is populated with a matching row count.
-- No vintage is collapsed here; point-in-time is select_latest_vintage over the spec in
-- gridflow.gold.contracts (RULINGS 466), applied by the caller with as_of=.
-- Outturn side: ex-post, from metered_wind_output_monthly's _latest, which is published
-- after delivery and is not available at forecast time.
-- The join is on the settlement pair (settlement date, settlement period).
CREATE OR REPLACE VIEW gold_gb_wind_forecast_vs_outturn AS
SELECT
    f.date AS settlement_date,
    f.settlement_period,
    f.timestamp_utc,
    f.capacity AS forecast_capacity_mw,
    f.incentive_forecast AS incentive_forecast_mw,
    f.published_at,
    f.available_at,
    f.capture_written_at,
    f.bronze_capture_id,
    o.outturn_rows,
    o.outturn_total_mw,
    o.outturn_scottish_mw,
    o.outturn_england_wales_mw,
    o.outturn_resource_id,
    o.outturn_available_at
FROM (
    SELECT s.*, COUNT(*) OVER (PARTITION BY s.bronze_capture_id) AS capture_rows
    FROM silver_neso_data_portal_da_wind_forecast_day_ahead AS s
) AS f
LEFT JOIN (
    SELECT
        m.sett_date,
        m.sett_period,
        COUNT(*) AS outturn_rows,
        CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(m.total) END AS outturn_total_mw,
        CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(m.scottish_wind_output) END AS outturn_scottish_mw,
        CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(m.england_wales_wind_output) END AS outturn_england_wales_mw,
        CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(m.resource_id) END AS outturn_resource_id,
        CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(m.available_at) END AS outturn_available_at
    FROM silver_neso_data_portal_metered_wind_output_monthly_latest AS m
    GROUP BY m.sett_date, m.sett_period
) AS o ON o.sett_date = f.date AND o.sett_period = f.settlement_period
WHERE EXISTS (
    SELECT 1 FROM state_neso_data_portal_completion AS c
    WHERE c.family = 'da_wind_forecast_day_ahead'
      AND c.bronze_capture_id = f.bronze_capture_id
      AND c.outcome = 'populated'
      AND c.row_count = f.capture_rows
);

COMMENT ON COLUMN gold_gb_wind_forecast_vs_outturn.outturn_total_mw IS
    'EX-POST: NESO operational-metered GB wind output (Total, MW) from silver_neso_data_portal_metered_wind_output_monthly_latest, joined on the settlement pair. Published after delivery and not available at forecast time; not the settlement-metered outturn NESO scores its forecasts against. NULL when outturn_rows is not 1.';

COMMENT ON COLUMN gold_gb_wind_forecast_vs_outturn.outturn_scottish_mw IS
    'EX-POST: NESO operational-metered Scottish wind output (MW) from silver_neso_data_portal_metered_wind_output_monthly_latest, joined on the settlement pair. Published after delivery and not available at forecast time; not the settlement-metered outturn NESO scores its forecasts against. NULL when outturn_rows is not 1.';

COMMENT ON COLUMN gold_gb_wind_forecast_vs_outturn.outturn_england_wales_mw IS
    'EX-POST: NESO operational-metered England/Wales wind output (MW) from silver_neso_data_portal_metered_wind_output_monthly_latest, joined on the settlement pair. Published after delivery and not available at forecast time; not the settlement-metered outturn NESO scores its forecasts against. NULL when outturn_rows is not 1.';

COMMENT ON COLUMN gold_gb_wind_forecast_vs_outturn.outturn_available_at IS
    'Provenance of the single outturn row (its available_at in the outturn _latest). Filtering outturn_available_at <= as_of is a fail-closed cutoff, not historical point-in-time: the _latest keeps only the newest outturn vintage. NULL when the outturn is NULL.';

COMMENT ON COLUMN gold_gb_wind_forecast_vs_outturn.outturn_rows IS
    'Count of outturn _latest rows for the settlement pair. Above 1 means overlapping outturn resources publish the pair and NESO states no precedence, so every outturn value is NULL. NULL when no outturn row exists.';

COMMENT ON COLUMN gold_gb_wind_forecast_vs_outturn.available_at IS
    'The forecast vintage: CKAN last_modified of the captured file. The view carries every complete vintage; point-in-time is select_latest_vintage(lf, contract_for(''gold_gb_wind_forecast_vs_outturn'').point_in_time, as_of=...) from gridflow.gold.contracts.';
