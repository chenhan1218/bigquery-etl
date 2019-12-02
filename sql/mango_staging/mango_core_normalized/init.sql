CREATE TABLE
  mango_core_normalized
PARTITION BY
  submission_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_core_normalized`
WHERE
  submission_date = @submission_date