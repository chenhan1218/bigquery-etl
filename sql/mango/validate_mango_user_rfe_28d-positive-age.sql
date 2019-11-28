SELECT
  DISTINCT age,
  execution_date
FROM
  `mango_dev3.mango_user_rfe_28d`
WHERE
  execution_date >= DATE('2019-11-13')
  AND age IS NOT NULL
ORDER BY
  age ASC,
  execution_date DESC