-- One-off job to copy data from search_aggregates_v6 between 2019-05-04 and 2019-05-11 (armag-addon)
SELECT
  submission_date_s3 AS submission_date,
  app_version,
  country,
  distribution_id,
  engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  addon_version,
  os,
  os_version,
  client_count,
  IFNULL(organic, 0) AS organic,
  IFNULL(tagged_sap, 0) AS tagged_sap,
  IFNULL(tagged_follow_on, 0) AS tagged_follow_on,
  IFNULL(sap, 0) AS sap,
  IFNULL(ad_click, 0) AS ad_click,
  IFNULL(search_with_ads, 0) AS search_with_ads,
  IFNULL(unknown, 0) AS unknown
FROM
  `moz-fx-data-derived-datasets.search.search_aggregates_v6`
WHERE
  submission_date_s3 >= '2019-05-04' AND submission_date_s3 <= '2019-05-11'
