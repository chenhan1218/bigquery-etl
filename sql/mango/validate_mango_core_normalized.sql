SELECT
  DISTINCT DATE_DIFF(submission_date, DATE_FROM_UNIX_DATE(normalized_profile_date), day ) AS d
FROM
  `mango_dev3.mango_core_normalized`
WHERE
  normalized_profile_date IS NOT NULL
ORDER BY
  d ASC