CREATE TABLE
  mango_feature_cohort_date
PARTITION BY
  execution_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_feature_cohort_date`
WHERE
  execution_date = @submission_date