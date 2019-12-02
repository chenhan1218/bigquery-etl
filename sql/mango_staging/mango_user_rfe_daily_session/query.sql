SELECT
  *
FROM
  `taipei-bi.mango_staging.mango_user_rfe_daily_session`
WHERE
  submission_date = @submission_date