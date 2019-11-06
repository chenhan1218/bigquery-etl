SELECT
  COUNT(*) AS count,
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
  SUM(
    IF(
      payload.process_type = 'main'
      OR payload.process_type IS NULL,
      1,
      0
    )
  ) AS main_crash,
  SUM(
    IF(
      REGEXP_CONTAINS(payload.process_type, 'content'),
      1,
      0
    )
  ) AS content_crash,
  SUM(IF(payload.metadata.startup_crash = '1', 1, 0)) AS startup_crash,
  SUM(
    IF(
      REGEXP_CONTAINS(
        payload.metadata.ipc_channel_error,
        'ShutDownKill'
      ),
      1,
      0
    )
  ) AS content_shutdown_crash
FROM
  `moz-fx-data-shared-prod.telemetry_live.crash_v4`
WHERE
  DATE(submission_timestamp) = "2019-11-04" -- TODO: replace with parameter
  AND DATE_DIFF(
    CURRENT_DATE(),
    PARSE_DATE('%Y%m%d', SUBSTR(application.build_id, 0, 8)),
    MONTH
  ) <= 6
GROUP BY
  window_start,
  normalized_channel,
  environment.build.version,
  application.display_version,
  application.build_id,
  normalized_app_name,
  normalized_os,
  normalized_os_version,
  application.architecture,
  normalized_country_code
