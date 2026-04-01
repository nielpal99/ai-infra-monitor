/*
  snowflake/tasks.sql
  ─────────────────────────────────────────────────────────────────────────────
  Snowflake Tasks for the ai-infra-monitor automated ingestion pipeline.

  Architecture
  ────────────
  Tasks are Snowflake-native scheduled jobs. The pipeline uses a two-task DAG:

    check_new_filings  (scheduled, runs daily at 06:00 ET / 10:00 UTC)
          │
          └──▶  load_new_xbrl  (triggered, runs after parent completes)

  1. check_new_filings  — root task. Runs on a cron schedule. Calls the stored
     procedure that polls the EDGAR EDGAR RSS / full-text search feed for each
     ticker in the watchlist and writes any newly discovered filings into
     staging.raw_filings_log. This populates the filings_stream.

  2. load_new_xbrl  — child task. Fires automatically when check_new_filings
     completes successfully (AFTER clause). Calls the stored procedure that
     reads new rows from filings_stream and, for each newly logged filing,
     fetches the EDGAR companyfacts JSON and upserts fact rows into
     staging.raw_xbrl_facts via the MERGE logic in snowflake_loader.py.

  Stream-task coupling
  ─────────────────────
  Tasks that reference a stream in their WHEN clause only execute if the
  stream is non-empty at runtime, saving warehouse credits on quiet days.
  load_new_xbrl uses this pattern: it only runs when filings_stream has rows.

  Stored procedures
  ─────────────────
  The tasks call stored procedures that are expected to exist in the monitoring
  schema. Procedure bodies are maintained separately and encapsulate the Python
  logic from edgar_monitor.py and snowflake_loader.py. The stored procedures
  must be created before tasks are resumed (see RESUME statements below).

  Task lifecycle
  ──────────────
  Tasks are created in SUSPENDED state by default. Run the RESUME statements
  at the bottom of this file after verifying the stored procedures exist.
  To pause the pipeline without dropping tasks, use the SUSPEND statements.

  See streams.sql for the stream definitions consumed by load_new_xbrl.
*/

-- ── 1. Root task: daily EDGAR filing check ────────────────────────────────────
--
-- Runs at 10:00 UTC (06:00 ET) every day. Calls log_new_filings() which polls
-- the EDGAR API for each watchlist ticker and inserts newly detected filings
-- into staging.raw_filings_log. This write populates filings_stream, which
-- in turn triggers load_new_xbrl on the next step of the DAG.
--
-- The task is created SUSPENDED. Run the RESUME statement below to activate it.

CREATE OR REPLACE TASK financial_intelligence.monitoring.check_new_filings
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 10 * * * UTC'
    COMMENT   = 'Root task. Polls EDGAR for new 10-K/10-Q/20-F filings across all watchlist tickers and writes results to raw_filings_log. Fires daily at 06:00 ET (10:00 UTC).'
AS
    CALL financial_intelligence.monitoring.log_new_filings();


-- ── 2. Child task: XBRL loader triggered by new filings ───────────────────────
--
-- Fires automatically after check_new_filings completes (AFTER clause creates
-- the DAG dependency). Only executes when filings_stream is non-empty (WHEN
-- clause) — if today's check found no new filings, no warehouse time is spent.
--
-- Calls load_xbrl_for_new_filings() which:
--   • Reads new rows from filings_stream
--   • For each (ticker, accession_number), fetches the EDGAR companyfacts JSON
--   • Parses and deduplicates XBRL facts via xbrl_parser.py logic
--   • Upserts rows into staging.raw_xbrl_facts via MERGE
--   • Writes a summary record to monitoring.alert_history
--
-- After the procedure commits, the filings_stream offset advances and
-- xbrl_stream becomes non-empty — signalling downstream tasks (dbt refresh,
-- alert evaluation) that new fact data is available.

CREATE OR REPLACE TASK financial_intelligence.monitoring.load_new_xbrl
    WAREHOUSE = COMPUTE_WH
    AFTER     financial_intelligence.monitoring.check_new_filings
    WHEN      SYSTEM$STREAM_HAS_DATA('financial_intelligence.monitoring.filings_stream')
    COMMENT   = 'Child task. Fires after check_new_filings when filings_stream is non-empty. Fetches XBRL facts from EDGAR and upserts into raw_xbrl_facts. Triggers xbrl_stream for downstream consumers.'
AS
    CALL financial_intelligence.monitoring.load_xbrl_for_new_filings();


-- ── Task lifecycle management ─────────────────────────────────────────────────
--
-- Tasks are created SUSPENDED above. Run the statements in this section
-- after confirming the stored procedures exist in the monitoring schema.
--
-- IMPORTANT: Resume child tasks before the root task. Snowflake requires
-- all downstream tasks in a DAG to be resumed before the root is activated.

-- Step 1 — Resume child task first
-- ALTER TASK financial_intelligence.monitoring.load_new_xbrl RESUME;

-- Step 2 — Resume root task (activates the full DAG)
-- ALTER TASK financial_intelligence.monitoring.check_new_filings RESUME;

-- To pause the pipeline without dropping tasks:
-- ALTER TASK financial_intelligence.monitoring.check_new_filings SUSPEND;
-- ALTER TASK financial_intelligence.monitoring.load_new_xbrl SUSPEND;

-- To inspect task run history:
-- SELECT *
-- FROM TABLE(financial_intelligence.information_schema.task_history(
--     task_name => 'check_new_filings',
--     scheduled_time_range_start => dateadd('day', -7, current_timestamp)
-- ))
-- ORDER BY scheduled_time DESC;
