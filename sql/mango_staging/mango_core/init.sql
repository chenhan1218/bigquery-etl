CREATE TABLE
  mango_core
PARTITION BY
  submission_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_core`
WHERE
  submission_date = @submission_date