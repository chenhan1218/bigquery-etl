CREATE TABLE
  mango_feature_roi
PARTITION BY
  execution_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_feature_roi`
WHERE
  execution_date = @submission_date