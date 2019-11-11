SELECT
  COUNT(DISTINCT client_id ) AS num_users,
  SUM(show_keyboard) AS show_keyboard,
  SUM(session_time ) AS total_session_time,
  feature_name,
  feature_type,
  event_method,
  event_object,
  submission_date
FROM
  `mango_staging.mango_events_feature_mapping`
WHERE
  submission_date >=DATE('2019-10-30')
GROUP BY
  feature_name,
  feature_type,
  event_method,
  event_object,
  submission_date
ORDER BY
  submission_date DESC,
  num_users DESC