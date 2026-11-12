-- DuckDB does not use Oracle-style stored procedures. python/warehouse.py performs
-- the SCD Type 2 dimension updates and idempotent fact upsert in a transaction.
-- The clean and deduplicated source view is created by sql/01_setup.sql.

SELECT
  source_name,
  COUNT(*) AS cleaned_launches,
  COUNT(*) FILTER (WHERE launch_timestamp IS NULL) AS missing_launch_dates,
  COUNT(*) FILTER (WHERE success_flag IS NULL) AS unclassified_outcomes
FROM CURATED.VW_LAUNCH_CLEAN
GROUP BY source_name
ORDER BY source_name;

-- After loading data with python .\python\ingest.py --load-warehouse, inspect the fact.
SELECT
  source_name,
  COUNT(*) AS fact_launches,
  COUNT(*) FILTER (WHERE success_flag) AS successful_launches,
  COUNT(*) FILTER (WHERE NOT success_flag) AS failed_launches
FROM CURATED.FACT_LAUNCH
GROUP BY source_name
ORDER BY source_name;
