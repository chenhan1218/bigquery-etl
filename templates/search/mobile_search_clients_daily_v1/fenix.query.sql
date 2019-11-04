CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));

CREATE TEMP FUNCTION normalize_search_key(key STRING) AS ((
  SELECT IF(ARRAY_LENGTH(SPLIT(key, '_')) = 2, REPLACE(key, '_', '.'), key)
));

CREATE TEMP FUNCTION udf_iso8601_str_to_date(date_str STRING) RETURNS DATE AS (
  COALESCE(
    SAFE.PARSE_DATE('%F', SAFE.SUBSTR(date_str, 0, 10)),
    SAFE.PARSE_DATE('%Y%m%d', SAFE.SUBSTR(date_str, 0, 8))
  )
);

-- TODO: default search engine (in metrics, not baseline)
WITH searches AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
    'Fenix' AS app_name, -- normalized_app_name is always null
    UNIX_DATE(udf_iso8601_str_to_date(client_info.first_run_date)) AS profile_creation_date,
    SAFE.DATE_DIFF(udf_iso8601_str_to_date(ping_info.end_time), udf_iso8601_str_to_date(client_info.first_run_date), DAY) AS profile_age_in_days,
    SPLIT(normalize_search_key(searches.key), '.')[SAFE_OFFSET(0)] AS engine,
    SPLIT(normalize_search_key(searches.key), '.')[SAFE_OFFSET(1)] AS source,
    searches.value AS search_count
  FROM
    `moz-fx-data-derived-datasets.org_mozilla_fenix.baseline`
  CROSS JOIN
    UNNEST(metrics.labeled_counter.metrics_search_count) searches
  WHERE
    DATE(submission_timestamp) = '2019-10-30'
)

SELECT
  submission_date,
  client_info.client_id AS client_id,
  engine,
  source,
  SUM(search_count) AS search_count,
  udf_mode_last(ARRAY_AGG(normalized_country_code)) AS country,
  udf_mode_last(ARRAY_AGG(metrics.string.glean_baseline_locale)) AS locale,
  udf_mode_last(ARRAY_AGG(app_name)) AS app_name,
  udf_mode_last(ARRAY_AGG(client_info.app_display_version)) AS app_version,
  udf_mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  udf_mode_last(ARRAY_AGG(normalized_os)) AS os,
  udf_mode_last(ARRAY_AGG(client_info.android_sdk_version)) AS os_version,
  udf_mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
  udf_mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
  udf_mode_last(ARRAY_AGG(sample_id)) AS sample_id
FROM
  searches
GROUP BY
  client_id,
  submission_date,
  engine,
  source
ORDER BY
  client_id