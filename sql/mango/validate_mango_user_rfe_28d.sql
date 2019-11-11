WITH
  rfe AS (
  SELECT
    feature_type AS feature_type__filter,
    feature_name AS feature_name__filter,
    execution_date,
    AVG(active_days) AS active_days,
    AVG(age) AS age,
    SUM(app_link_install) AS app_link_install,
    SUM(app_link_open) AS app_link_open,
    AVG(frequency_days) AS frequency_days,
    AVG(recency) AS recency,
    AVG(session_time) AS session_time,
    SUM(show_keyboard) AS show_keyboard,
    SUM(url_counts) AS url_counts,
    AVG(value_event_count) AS value_event_count,
    COUNT(1) AS events,
    COUNT(DISTINCT client_id) AS users
  FROM
    mango_staging.mango_user_rfe_28d
  WHERE
    execution_date >= DATE '2019-11-04'
    AND feature_name IN ('Shopping',
      'feature: tab_swipe',
      'feature: visit_shopping_content_tab')
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
  session_time > 100000
ORDER BY
  execution_date DESC,
  session_time DESC