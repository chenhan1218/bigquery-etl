WITH crash_ping_agg AS (
  SELECT
    udf_round_timestamp_to_minute(submission_timestamp, 5) AS window_start,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    environment.build.build_id,
    metadata.uri.app_name,
    environment.system.os.name AS os,
    environment.system.os.version AS os_version,
    environment.build.architecture,
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
      PARSE_DATE('%Y%m%d', SUBSTR(environment.build.build_id, 0, 8)),
      MONTH
    ) <= 6
),
main_ping_agg AS (
  SELECT
    udf_round_timestamp_to_minute(submission_timestamp, 5) AS window_start,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    environment.build.build_id,
    metadata.uri.app_name,
    environment.system.os.name AS os,
    environment.system.os.version AS os_version,
    environment.build.architecture,
    normalized_country_code AS country,
    -- 0 columns to match crash ping
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    LEAST(GREATEST(payload.info.subsession_length / 3600, 0), 25) AS usage_hours,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gpu'), 0) AS gpu_crashes,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin'), 0) AS plugin_crashes,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin'), 0) AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`
  WHERE
    DATE(submission_timestamp) = '2019-11-04' -- TODO: USE param
    AND DATE_DIFF(
      CURRENT_DATE(),
      PARSE_DATE('%Y%m%d', SUBSTR(environment.build.build_id, 0, 8)),
      MONTH
    ) <= 6
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
