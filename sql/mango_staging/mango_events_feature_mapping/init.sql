CREATE TABLE
  mango_events_feature_mapping
PARTITION BY
  submission_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_events_feature_mapping`
WHERE
  submission_date = @submission_date