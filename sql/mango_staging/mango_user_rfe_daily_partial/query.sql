SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_user_rfe_daily_partial`
WHERE
  submission_date = @submission_date