CREATE TEMP FUNCTION udf_round_timestamp_to_minute(timestamp_expression TIMESTAMP, minute INT64) AS (
  TIMESTAMP_SECONDS(
    CAST((FLOOR(UNIX_SECONDS(timestamp_expression) / (minute * 60)) * minute * 60) AS INT64)
  )
);
--
WITH crash_ping_agg AS (
  SELECT
    udf_round_timestamp_to_minute(submission_timestamp, 5) AS window_start,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    application.build_id,
    normalized_app_name AS app_name,
    normalized_os AS os,
    normalized_os_version AS os_version,
    application.architecture,
    normalized_country_code AS country,
    IF(
      payload.process_type = 'main'
      OR payload.process_type IS NULL,
      1,
      0
    ) AS main_crash,
    IF(
      REGEXP_CONTAINS(payload.process_type, 'content'),
      1,
      0
    ) AS content_crash,
    IF(payload.metadata.startup_crash = '1', 1, 0) AS startup_crash,
    IF(
      REGEXP_CONTAINS(payload.metadata.ipc_channel_error, 'ShutDownKill'),
      1,
      0
    ) AS content_shutdown_crash,
    -- 0 columns to match main pings
    0 AS usage_hours,
    0 AS gpu_crashes,
    0 AS plugin_crashes,
    0 AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry_live.crash_v4`
  WHERE
    DATE(submission_timestamp) = "2019-11-04" -- TODO: replace with parameter
    AND DATE_DIFF(
      CURRENT_DATE(),
      PARSE_DATE('%Y%m%d', SUBSTR(application.build_id, 0, 8)),
      MONTH
    ) <= 6
),
main_ping_agg AS (
  SELECT
    udf_round_timestamp_to_minute(submission_timestamp, 5) AS window_start,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    application.build_id,
    normalized_app_name AS app_name,
    normalized_os AS os,
    normalized_os_version AS os_version,
    application.architecture,
    normalized_country_code AS country,
    -- 0 columns to match crash ping
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    LEAST(GREATEST(payload.info.subsession_length / 3600, 0), 25) AS usage_hours,
    COALESCE(get_histogram_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gpu'), 0) AS gpu_crashes,
    COALESCE(get_histogram_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin'), 0) AS plugin_crashes,
    COALESCE(get_histogram_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin'), 0) AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`
  WHERE
    DATE(submission_timestamp) = '2019-11-04' -- TODO: USE param
),
combined_crashes AS (
  SELECT
    *
  FROM
    crash_ping_agg
  UNION ALL
  SELECT
    *
  FROM
    main_ping_agg
)

SELECT
  window_start,
  channel,
  version,
  display_version,
  build_id,
  app_name,
  os,
  os_version,
  architecture,
  country,
  COUNT(*) AS count,
  SUM(main_crash) AS main_crashes,
  SUM(content_crash) AS content_crashes,
  SUM(startup_crash) AS startup_crashes,
  SUM(content_shutdown_crash) AS content_shutdown_crashes,
  SUM(gpu_crashes) AS gpu_crashes,
  SUM(plugin_crashes) AS plugin_crashes,
  SUM(gmplugin_crashes) as gmplugin_crashes,
  SUM(usage_hours) AS usage_hours
FROM
  combined_crashes
GROUP BY
  window_start,
  channel,
  version,
  display_version,
  build_id,
  app_name,
  os,
  os_version,
  architecture,
  country
