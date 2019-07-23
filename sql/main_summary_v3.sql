
--
-- Query generated by: templates/unnest_parquet_view.sql.py main_summary_v3 telemetry_raw.main_summary_v3
CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.main_summary_v3` AS
SELECT
  submission_date AS submission_date_s3,
  * REPLACE (
    ARRAY(SELECT * FROM UNNEST(active_addons.list)) AS active_addons,
    ARRAY(SELECT AS STRUCT _0.element.* REPLACE (_0.element.map_values.key_value AS map_values) FROM UNNEST(events.list) AS _0) AS events,
    popup_notification_stats.key_value AS popup_notification_stats,
    ARRAY(SELECT * FROM UNNEST(search_counts.list)) AS search_counts,
    ssl_handshake_result.key_value AS ssl_handshake_result
  )
FROM
  `moz-fx-data-derived-datasets.telemetry_raw.main_summary_v3`