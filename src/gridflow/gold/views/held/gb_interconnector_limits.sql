-- Gold view: GB interconnector capacity limits, one current family per link (v0.22-G, ADR-041).
-- Held: unregistered; hold reasons are gridflow.gold.contracts.hold_reasons().
-- Seven inputs: eleclink, ifa_itl, ifa2_ifa_itl, nemolink_ntc, nsl, viking_link_ntc and
-- brit_ned, positionally unioned in one column order. Every complete vintage is kept: one
-- row per silver row of each capture whose completion record is populated with a matching
-- row count. Point-in-time is select_latest_vintage over the spec in gridflow.gold.contracts
-- (RULINGS 466), applied by the caller with as_of=.
-- operational_period_start_gmt is the vendor target instant where one exists; the silver
-- timestamp_utc of a temporal-none family is its capture time and is never projected.
-- Flows are directional maxima in MW (to GB = import, from GB = export), not a signed flow.
-- BritNed flows stay vendor text, uncast. Dropped vendor columns: data_upload_time_gmt
-- (equal to issue_time) and version (meaning undocumented).
CREATE OR REPLACE VIEW gold_gb_interconnector_limits AS
SELECT
    u.link,
    u.family,
    u.resource_id,
    u.auction_type,
    u.operational_period_start_gmt,
    u.operational_date,
    u.hourly_time_period,
    u.operational_date_and_hour,
    u.flow_to_gb_mw,
    u.flow_from_gb_mw,
    u.flow_to_gb_mw_raw,
    u.flow_from_gb_mw_raw,
    u.reason_for_restriction_to_gb,
    u.reason_for_restriction_from_gb,
    u.reason_for_restriction,
    u.issue_time,
    u.published_at,
    u.available_at,
    u.available_at_basis,
    u.capture_written_at,
    u.bronze_capture_id
FROM (
    SELECT b.*, COUNT(*) OVER (PARTITION BY b.family, b.bronze_capture_id) AS capture_rows
    FROM (
        SELECT
            'eleclink' AS link,
            'eleclink' AS family,
            s.resource_id AS resource_id,
            s.auction_type AS auction_type,
            s.operational_period_start_gmt AS operational_period_start_gmt,
            s.operational_date AS operational_date,
            s.hourly_time_period AS hourly_time_period,
            CAST(NULL AS VARCHAR) AS operational_date_and_hour,
            s.flow_to_gb_mw AS flow_to_gb_mw,
            s.flow_from_gb_mw AS flow_from_gb_mw,
            CAST(NULL AS VARCHAR) AS flow_to_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS flow_from_gb_mw_raw,
            s.reason_for_restriction_to_gb AS reason_for_restriction_to_gb,
            s.reason_for_restriction_from_gb AS reason_for_restriction_from_gb,
            s.reason_for_restriction AS reason_for_restriction,
            s.issue_time AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'gridflow capture time' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_eleclink AS s
        UNION ALL
        SELECT
            'ifa' AS link,
            'ifa_itl' AS family,
            CAST(NULL AS VARCHAR) AS resource_id,
            s.auction_type AS auction_type,
            s.operational_period_start_gmt AS operational_period_start_gmt,
            CAST(NULL AS VARCHAR) AS operational_date,
            CAST(NULL AS VARCHAR) AS hourly_time_period,
            CAST(NULL AS VARCHAR) AS operational_date_and_hour,
            s.flow_to_gb_mw AS flow_to_gb_mw,
            s.flow_from_gb_mw AS flow_from_gb_mw,
            CAST(NULL AS VARCHAR) AS flow_to_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS flow_from_gb_mw_raw,
            s.reason_for_restriction_to_gb AS reason_for_restriction_to_gb,
            s.reason_for_restriction_from_gb AS reason_for_restriction_from_gb,
            CAST(NULL AS VARCHAR) AS reason_for_restriction,
            s.issue_time AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'gridflow capture time' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_ifa_itl AS s
        UNION ALL
        SELECT
            'ifa2' AS link,
            'ifa2_ifa_itl' AS family,
            CAST(NULL AS VARCHAR) AS resource_id,
            s.auction_type AS auction_type,
            s.operational_period_start_gmt AS operational_period_start_gmt,
            CAST(NULL AS VARCHAR) AS operational_date,
            CAST(NULL AS VARCHAR) AS hourly_time_period,
            CAST(NULL AS VARCHAR) AS operational_date_and_hour,
            s.flow_to_gb_mw AS flow_to_gb_mw,
            s.flow_from_gb_mw AS flow_from_gb_mw,
            CAST(NULL AS VARCHAR) AS flow_to_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS flow_from_gb_mw_raw,
            s.reason_for_restriction_to_gb AS reason_for_restriction_to_gb,
            s.reason_for_restriction_from_gb AS reason_for_restriction_from_gb,
            CAST(NULL AS VARCHAR) AS reason_for_restriction,
            s.issue_time AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'gridflow capture time' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_ifa2_ifa_itl AS s
        UNION ALL
        SELECT
            'nemolink' AS link,
            'nemolink_ntc' AS family,
            s.resource_id AS resource_id,
            s.auction_type AS auction_type,
            s.operational_period_start_gmt AS operational_period_start_gmt,
            s.operational_date AS operational_date,
            s.hourly_time_period AS hourly_time_period,
            CAST(NULL AS VARCHAR) AS operational_date_and_hour,
            s.flow_to_gb_mw AS flow_to_gb_mw,
            s.flow_from_gb_mw AS flow_from_gb_mw,
            CAST(NULL AS VARCHAR) AS flow_to_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS flow_from_gb_mw_raw,
            s.reason_for_restriction_to_gb AS reason_for_restriction_to_gb,
            s.reason_for_restriction_from_gb AS reason_for_restriction_from_gb,
            s.reason_for_restriction AS reason_for_restriction,
            s.issue_time AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'gridflow capture time' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_nemolink_ntc AS s
        UNION ALL
        SELECT
            'nsl' AS link,
            'nsl' AS family,
            s.resource_id AS resource_id,
            s.auction_type AS auction_type,
            s.operational_period_start_gmt AS operational_period_start_gmt,
            s.operational_date AS operational_date,
            s.hourly_time_period AS hourly_time_period,
            CAST(NULL AS VARCHAR) AS operational_date_and_hour,
            s.flow_to_gb_mw AS flow_to_gb_mw,
            s.flow_from_gb_mw AS flow_from_gb_mw,
            CAST(NULL AS VARCHAR) AS flow_to_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS flow_from_gb_mw_raw,
            s.reason_for_restriction_to_gb AS reason_for_restriction_to_gb,
            s.reason_for_restriction_from_gb AS reason_for_restriction_from_gb,
            s.reason_for_restriction AS reason_for_restriction,
            s.issue_time AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'gridflow capture time' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_nsl AS s
        UNION ALL
        SELECT
            'viking' AS link,
            'viking_link_ntc' AS family,
            CAST(NULL AS VARCHAR) AS resource_id,
            s.auction_type AS auction_type,
            s.operational_period_start_gmt AS operational_period_start_gmt,
            CAST(NULL AS VARCHAR) AS operational_date,
            CAST(NULL AS VARCHAR) AS hourly_time_period,
            CAST(NULL AS VARCHAR) AS operational_date_and_hour,
            s.flow_to_gb_mw AS flow_to_gb_mw,
            s.flow_from_gb_mw AS flow_from_gb_mw,
            CAST(NULL AS VARCHAR) AS flow_to_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS flow_from_gb_mw_raw,
            s.reason_for_restriction_to_gb AS reason_for_restriction_to_gb,
            s.reason_for_restriction_from_gb AS reason_for_restriction_from_gb,
            CAST(NULL AS VARCHAR) AS reason_for_restriction,
            s.issue_time AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'gridflow capture time' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_viking_link_ntc AS s
        UNION ALL
        SELECT
            'brit-ned' AS link,
            'brit_ned' AS family,
            s.resource_id AS resource_id,
            CAST(NULL AS VARCHAR) AS auction_type,
            CAST(NULL AS TIMESTAMPTZ) AS operational_period_start_gmt,
            CAST(NULL AS VARCHAR) AS operational_date,
            CAST(NULL AS VARCHAR) AS hourly_time_period,
            s.operational_date_and_hour AS operational_date_and_hour,
            CAST(NULL AS DOUBLE) AS flow_to_gb_mw,
            CAST(NULL AS DOUBLE) AS flow_from_gb_mw,
            s.flow_to_gb_mw_raw AS flow_to_gb_mw_raw,
            s.flow_from_gb_mw_raw AS flow_from_gb_mw_raw,
            CAST(NULL AS VARCHAR) AS reason_for_restriction_to_gb,
            CAST(NULL AS VARCHAR) AS reason_for_restriction_from_gb,
            s.reason_for_restriction AS reason_for_restriction,
            CAST(NULL AS TIMESTAMPTZ) AS issue_time,
            s.published_at AS published_at,
            s.available_at AS available_at,
            'CKAN last_modified of the captured file (ADR-030)' AS available_at_basis,
            s.capture_written_at AS capture_written_at,
            s.bronze_capture_id AS bronze_capture_id
        FROM silver_neso_data_portal_brit_ned AS s
    ) AS b
) AS u
WHERE EXISTS (
    SELECT 1 FROM state_neso_data_portal_completion AS c
    WHERE c.family = u.family
      AND c.bronze_capture_id = u.bronze_capture_id
      AND c.outcome = 'populated'
      AND c.row_count = u.capture_rows
);

COMMENT ON COLUMN gold_gb_interconnector_limits.available_at IS
    'When the row became available to gridflow. For the six dump families (eleclink, ifa_itl, ifa2_ifa_itl, nemolink_ntc, nsl, viking_link_ntc) available_at = gridflow capture time, not when NESO issued the row; for brit_ned it is the CKAN last_modified of the captured file. available_at_basis names the clock per row.';

COMMENT ON COLUMN gold_gb_interconnector_limits.available_at_basis IS
    'The clock behind available_at for this row: the eligibility ledger''s published-clock label of its family''s record vintage.';

COMMENT ON COLUMN gold_gb_interconnector_limits.operational_period_start_gmt IS
    'The vendor target instant (UTC) where the vendor gives one; NULL for label-only rows (archive Operational Date / Hourly Time Period, BritNed hourly labels). Never a capture time.';

COMMENT ON COLUMN gold_gb_interconnector_limits.flow_to_gb_mw IS
    'Directional maximum into GB (import), MW. A separate magnitude from flow_from_gb_mw, not a signed flow.';

COMMENT ON COLUMN gold_gb_interconnector_limits.flow_from_gb_mw IS
    'Directional maximum out of GB (export), MW. A separate magnitude from flow_to_gb_mw, not a signed flow.';

COMMENT ON COLUMN gold_gb_interconnector_limits.flow_to_gb_mw_raw IS
    'BritNed vendor text of the maximum into GB (import), uncast; NULL for the other links.';

COMMENT ON COLUMN gold_gb_interconnector_limits.flow_from_gb_mw_raw IS
    'BritNed vendor text of the maximum out of GB (export), uncast; NULL for the other links.';
