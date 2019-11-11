SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_events_unnested`
WHERE
  submission_date = @submission_date