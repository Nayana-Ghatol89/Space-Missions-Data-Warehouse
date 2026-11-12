-- DuckDB is an embedded local database, so it has no server-side task scheduler.
-- Schedule the ingestion command with Windows Task Scheduler if automation is wanted:
-- python .\python\ingest.py --skip-apis --skip-csv --load-warehouse
-- The command reprocesses existing raw rows idempotently; new API/CSV loads can also
-- invoke the same warehouse loader as part of ingestion.

SELECT
  run_id,
  pipeline_name,
  source_object,
  status,
  source_rows,
  target_rows,
  started_at,
  completed_at,
  error_message
FROM OPS.ETL_AUDIT
ORDER BY started_at DESC
LIMIT 50;
