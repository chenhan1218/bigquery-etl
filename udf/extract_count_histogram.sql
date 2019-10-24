CREATE TEMP FUNCTION udf_extract_count_histogram(histogram STRING) AS (SAFE_CAST(JSON_EXTRACT(histogram, '$.values.0') AS INT64));
