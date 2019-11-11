SELECT
  COUNT(*) AS c,
  cohort_date,
  cohort_level,
  cohort_name
FROM
  `mango_staging.mango_feature_cohort_date`
GROUP BY
  cohort_date,
  cohort_level,
  cohort_name
ORDER BY
  c DESC,
  cohort_date DESC,
  cohort_level DESC,
  cohort_name DESC,