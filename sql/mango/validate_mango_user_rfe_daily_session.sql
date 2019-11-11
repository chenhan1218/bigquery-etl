WITH
  rfe AS (
  SELECT
    feature_type AS feature_type__filter,
    feature_name AS feature_name__filter,
    submission_date,
    SUM(app_link_install) AS app_link_install,
    SUM(app_link_open) AS app_link_open,
    AVG(session_time)/1000 AS session_time,
    SUM(show_keyboard) AS show_keyboard,
    SUM(url_counts) AS url_counts,
    COUNT(1) AS events,
    COUNT(DISTINCT client_id) AS users
  FROM
    `mango_staging.mango_user_rfe_daily_session`
  WHERE
    submission_date >= DATE '2019-11-04'
  GROUP BY
    1,
    2,
    3)
SELECT
  *,
  url_counts/users AS url_count_per_user
FROM
  RFE
WHERE
  session_time > 100
ORDER BY
  submission_date DESC,
  session_time DESC