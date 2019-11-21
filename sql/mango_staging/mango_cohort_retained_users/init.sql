CREATE TABLE
  mango_cohort_retained_users
PARTITION BY
  execution_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_cohort_retained_users`
WHERE
  execution_date = @submission_date