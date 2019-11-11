SELECT
  cohort_date,
  cohort_name,
  d1_retained_users,
  d7_retained_users,
  d14_retained_users,
  d28_retained_users,
  w1_retained_users,
  w2_retained_users,
  w3_retained_users,
  w4_retained_users
FROM
  `mango_staging.mango_cohort_retained_users`
WHERE
  cohort_name IN ("Browser",
    "feature: launch_app",
    "tags: launch_app_from_launcher",
    "feature: pre_search",
    "feature: search",
    "partner: true",
    "tags: keyword_search",
    "source: google",
    "feature: remove_tab",
    "feature: change_tab",
    "feature: give_feedback",
    "feature: visit_topsite",
    "feature: browse",
    "tags: launch_app_from_external",
    "feature: add_tab",
    "feature: private_mode" )
ORDER BY
  cohort_date DESC,
  cohort_name DESC