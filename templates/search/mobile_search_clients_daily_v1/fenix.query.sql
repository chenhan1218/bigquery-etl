SELECT
  EXTRACT(DATE FROM submission_timestamp) AS submission_date,
  client_info.client_id,
  metrics.labeled_counter.metrics_search_count as searches,
  metrics.string.search_default_engine_code as default_search_engine,
  normalized_country_code as country,
  client_info.app_display_version AS app_version,
  normalized_channel AS channel,
  normalized_os AS os,
  client_info.android_sdk_version AS os_version,
  client_info.first_run_date AS profile_creation_date -- change to profile age,
  'PLACEHOLDER' AS profile_age_in_days,
  sample_id
FROM
  `moz-fx-data-derived-datasets.org_mozilla_fenix.metrics`
WHERE
  EXTRACT(DATE
  FROM
    submission_timestamp) = '2019-10-30'
