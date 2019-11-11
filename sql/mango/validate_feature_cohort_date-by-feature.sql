SELECT
  COUNT(*) AS c,
  cohort_date,
  cohort_name
FROM
  `mango_staging.mango_feature_cohort_date`
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
GROUP BY
  cohort_date,
  cohort_name
ORDER BY
  cohort_date DESC,
  cohort_name DESC