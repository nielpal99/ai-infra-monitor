/*
  snowflake/streams.sql
  ─────────────────────────────────────────────────────────────────────────────
  Snowflake Change Data Capture (CDC) streams for the ai-infra-monitor pipeline.

  Architecture
  ────────────
  Streams act as lightweight change logs on top of staging tables. They record
  INSERT/UPDATE/DELETE offsets so downstream tasks can consume only net-new rows
  without scanning the full table on every run.

  Both streams are APPEND_ONLY because the ingestion pipeline only ever inserts
  and upserts (MERGE) into the staging tables — we never issue bare DELETEs.
  APPEND_ONLY streams are more efficient and only capture INSERT activity, which
  is exactly what the downstream tasks need to detect new filings and new facts.

  Consumption model
  ─────────────────
  A stream's offset advances automatically when a task (or any DML statement
  within the same transaction) successfully queries it. If the consuming task
  fails, the offset is NOT advanced and the same rows are re-presented on the
  next run — giving us at-least-once delivery with Snowflake's built-in retry.

  Streams defined here
  ─────────────────────
    filings_stream  — watches raw_filings_log for newly detected SEC filings
    xbrl_stream     — watches raw_xbrl_facts for newly ingested XBRL fact rows

  See tasks.sql for the tasks that consume these streams.
*/

-- ── 1. Filings stream ─────────────────────────────────────────────────────────
--
-- Watches staging.raw_filings_log for new rows written by the EDGAR monitor
-- (edgar_monitor.py / check_new_filings task). When the stream is non-empty,
-- the check_new_filings task knows new SEC filings have been detected and
-- triggers the load_new_xbrl child task to kick off XBRL ingestion.

CREATE OR REPLACE STREAM financial_intelligence.monitoring.filings_stream
    ON TABLE financial_intelligence.staging.raw_filings_log
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on raw_filings_log. Non-empty when new SEC filings have been logged by the EDGAR monitor. Consumed by the load_new_xbrl task.';


-- ── 2. XBRL facts stream ──────────────────────────────────────────────────────
--
-- Watches staging.raw_xbrl_facts for new rows written by the XBRL loader
-- (snowflake_loader.py). Downstream consumers (e.g. a dbt refresh task or an
-- alert evaluation task) can query this stream to process only newly arrived
-- fact rows without re-scanning the full raw_xbrl_facts table.
--
-- Note: MERGE statements do advance the stream offset for matched (updated)
-- rows as well as inserted rows, so any dbt or alert task consuming this
-- stream will see both net-new periods and amended/restated periods.

CREATE OR REPLACE STREAM financial_intelligence.monitoring.xbrl_stream
    ON TABLE financial_intelligence.staging.raw_xbrl_facts
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream on raw_xbrl_facts. Non-empty when new XBRL facts have been loaded. Consumed by downstream dbt refresh and alert evaluation tasks.';
