/*
SELECT
  metadata.uri.app_version,
  flattened_searches.key
FROM
  `moz-fx-data-derived-datasets.telemetry.core`
CROSS JOIN UNNEST(searches) as flattened_searches
WHERE
  EXTRACT(DATE FROM submission_timestamp) = '2019-10-29'
  AND sample_id = 13
  AND metadata.uri.app_version >= '3'
ORDER BY app_version
 */

SELECT
  EXTRACT(DATE FROM submission_timestamp) AS submission_date,
  client_id,
  searches,
  default_search as default_search_engine,
  normalized_country_code AS country,
  display_version AS app_version,
  normalized_channel AS channel,
  normalized_os as os,
  osversion AS os_version,
  profile_date AS profile_creation_date,
  SAFE_SUBTRACT(UNIX_DATE(DATE(SAFE.TIMESTAMP(EXTRACT(DATE FROM submission_timestamp)))), profile_date) AS profile_age_in_days,
  sample_id
FROM
  `moz-fx-data-derived-datasets.telemetry.core`
WHERE
  EXTRACT(DATE FROM submission_timestamp) = @submission_date
