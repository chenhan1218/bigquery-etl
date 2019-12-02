CREATE TABLE
  mango_user_channels
PARTITION BY
  execution_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_user_channels`
WHERE
  execution_date = @submission_date