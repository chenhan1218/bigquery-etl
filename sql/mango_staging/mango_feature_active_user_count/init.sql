CREATE TABLE
  mango_feature_active_user_count
PARTITION BY
  submission_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_feature_active_user_count`
WHERE
  submission_date = @submission_date