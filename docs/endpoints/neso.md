# NESO (National Energy System Operator) — Dataset Reference

**Source:** `neso`
**Base URL:** `https://api.carbonintensity.org.uk`
**Authentication:** None (public API)
**Coverage:** Great Britain (national average)
**Resolution:** 30 minutes (half-hourly settlement periods)

---

## carbon_intensity

Half-hourly carbon intensity of GB electricity generation, expressed in grams of CO₂ equivalent per kilowatt-hour (gCO₂eq/kWh). Provides both the modelled forecast (available up to 96 hours ahead) and, for past periods, the verified actual intensity. Also includes a qualitative index label. Used to assess the environmental impact of electricity consumption and time-shift flexible demand to low-carbon periods.

**API path:** `/intensity/{from_datetime}/{to_datetime}`
**Param style:** Timestamps in URL path (`YYYY-MM-DDTHH:MMZ` format)
**Chunk limit:** Max 14 days per request window
**Silver key columns:** `timestamp_utc`, `forecast_gco2_kwh`, `actual_gco2_kwh`, `intensity_index`

| timestamp_utc          | forecast_gco2_kwh | actual_gco2_kwh | intensity_index |
|------------------------|------------------:|----------------:|-----------------|
| 2024-06-15 00:00:00+00 |             185.0 |           192.0 | moderate        |
| 2024-06-15 00:30:00+00 |             182.0 |           188.0 | moderate        |
| 2024-06-15 01:00:00+00 |             178.0 |           181.0 | moderate        |
| 2024-06-15 01:30:00+00 |             175.0 |           177.0 | low             |
| 2024-06-15 02:00:00+00 |             172.0 |           174.0 | low             |

> Intensity in gCO₂eq/kWh. `actual_gco2_kwh` is null for future periods (forecast-only rows). `intensity_index` values: `very low`, `low`, `moderate`, `high`, `very high`. Deduplicated on `(timestamp_utc)`.
