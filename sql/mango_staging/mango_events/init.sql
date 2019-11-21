CREATE TABLE
  mango_events
PARTITION BY
  submission_date AS
SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_events`
WHERE
  submission_date = @submission_date