-- Raw files are loaded locally by:
-- python .\python\ingest.py --skip-apis --load-warehouse
-- These queries verify the raw batch counts in the local DuckDB database.
SELECT run_id, source_name, COUNT(*) AS api_payload_rows, MAX(loaded_at) AS latest_load
FROM RAW.API_PAYLOADS
GROUP BY run_id, source_name
ORDER BY run_id DESC, source_name;

SELECT run_id, source_name, COUNT(*) AS launch_rows, MAX(loaded_at) AS latest_load
FROM RAW.LAUNCHES
GROUP BY run_id, source_name
ORDER BY run_id DESC, source_name;
