-- Gold view: EU Gas Storage
-- Country-level gas storage levels from GIE AGSI+.
-- Provides a consolidated view of European gas storage by country and day.
CREATE OR REPLACE VIEW gold_eu_gas_storage AS
SELECT
    gas_day,
    country_code,
    country_name,
    gas_in_storage_gwh,
    withdrawal_gwh,
    injection_gwh,
    working_gas_volume_gwh,
    storage_pct_full,
    trend,
    data_provider,
    ingested_at
FROM silver_storage
ORDER BY gas_day DESC, country_code
