CREATE TABLE
  mango_user_rfe_28d
PARTITION BY
  execution_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_user_rfe_28d`
WHERE
  execution_date = @submission_date