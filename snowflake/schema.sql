-- =============================================================================
-- financial_intelligence — full schema DDL
--
-- Databases and schemas:
--   financial_intelligence
--   ├── staging      raw ingest from EDGAR (XBRL facts, filing metadata)
--   ├── marts        dbt-built analytical tables (metrics, benchmarks)
--   └── monitoring   alert rules and alert history
--
-- Run order: database → schemas → staging → marts → monitoring
-- All objects use IF NOT EXISTS so this file is safe to re-run.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- database & schemas
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS financial_intelligence
    COMMENT = 'AI infrastructure financial intelligence — EDGAR XBRL, metrics, and alerting';

CREATE SCHEMA IF NOT EXISTS financial_intelligence.staging
    COMMENT = 'Raw ingest layer. Tables here are append/upsert targets for the Python ingestion pipeline. Never queried directly by analysts.';

CREATE SCHEMA IF NOT EXISTS financial_intelligence.marts
    COMMENT = 'Analytical mart layer. Built by dbt transforms on top of staging. One clear grain per table.';

CREATE SCHEMA IF NOT EXISTS financial_intelligence.monitoring
    COMMENT = 'Alert configuration and history. Decoupled from marts so rules can be edited without touching financial data.';


-- =============================================================================
-- STAGING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- staging.raw_xbrl_facts
--
-- One row per (ticker, concept, period_end, form) after deduplication.
-- Loaded by ingestion/snowflake_loader.py via MERGE from the EDGAR
-- companyfacts API.  Only USD-denominated values are stored; non-USD rows
-- (e.g. TSM TWD) are filtered in the loader before insert.
--
-- Canonical concept names (the `concept` column) are the keys of CONCEPT_MAP
-- in ingestion/xbrl_parser.py — not the raw XBRL tag, which is in xbrl_concept.
-- This lets dbt transforms join on a stable name regardless of which XBRL tag
-- a company happened to use in a given filing year.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.staging.raw_xbrl_facts (

    -- identity
    ticker          VARCHAR(10)    NOT NULL COMMENT 'Exchange ticker symbol (e.g. NVDA)',
    cik             VARCHAR(10)    NOT NULL COMMENT 'EDGAR CIK, zero-padded to 10 digits',
    taxonomy        VARCHAR(20)    NOT NULL COMMENT 'us-gaap or ifrs-full',
    concept         VARCHAR(100)   NOT NULL COMMENT 'Canonical concept name from CONCEPT_MAP in xbrl_parser.py',
    xbrl_concept    VARCHAR(200)   NOT NULL COMMENT 'Actual XBRL tag reported by the filer (may differ from concept)',

    -- value
    value           FLOAT          NOT NULL COMMENT 'Reported value in USD',
    unit            VARCHAR(20)    NOT NULL COMMENT 'Unit of measure — always USD in this table',

    -- period
    period_start    DATE                    COMMENT 'Start of reporting period (NULL for instant/balance-sheet facts)',
    period_end      DATE           NOT NULL COMMENT 'End of reporting period — fiscal quarter or year end',
    form            VARCHAR(10)    NOT NULL COMMENT 'SEC form type: 10-K, 10-Q, 20-F, or 6-K',
    filed           DATE           NOT NULL COMMENT 'Date the filing was submitted to EDGAR',
    accession       VARCHAR(25)             COMMENT 'EDGAR accession number (e.g. 0001045810-25-000029)',

    -- fiscal period metadata
    fy              INTEGER                 COMMENT 'Fiscal year as reported by the filer',
    fp              VARCHAR(5)              COMMENT 'Fiscal period code: FY, Q1, Q2, Q3',

    -- housekeeping
    loaded_at       TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
                                   COMMENT 'UTC timestamp of last upsert into this table'
)
COMMENT = 'Raw XBRL financial facts from the EDGAR companyfacts API. One row per (ticker, concept, period_end, form) after USD filtering and deduplication. Source of truth for all downstream metric transforms.'
;


-- -----------------------------------------------------------------------------
-- staging.raw_filings_log
--
-- One row per filing detected by ingestion/edgar_monitor.py.
-- Appended on each monitor run; `processed` is flipped to TRUE once the
-- XBRL facts for this filing have been loaded into raw_xbrl_facts.
-- The Snowflake Stream on this table (see streams.sql) triggers the Task
-- that kicks off downstream XBRL ingestion automatically.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.staging.raw_filings_log (

    -- identity
    ticker          VARCHAR(10)    NOT NULL COMMENT 'Exchange ticker symbol',
    cik             VARCHAR(10)    NOT NULL COMMENT 'EDGAR CIK, zero-padded to 10 digits',
    form_type       VARCHAR(10)    NOT NULL COMMENT 'SEC form type: 10-K, 10-Q, 8-K, 20-F, or 6-K',
    accession_number VARCHAR(25)   NOT NULL COMMENT 'EDGAR accession number — globally unique filing identifier',

    -- filing date
    filed_date      DATE           NOT NULL COMMENT 'Date the filing was submitted to EDGAR',

    -- pipeline state
    processed       BOOLEAN        NOT NULL DEFAULT FALSE
                                   COMMENT 'TRUE once XBRL facts for this filing have been loaded downstream',

    -- housekeeping
    detected_at     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
                                   COMMENT 'UTC timestamp when edgar_monitor.py first saw this filing',
    processed_at    TIMESTAMP_NTZ           COMMENT 'UTC timestamp when XBRL ingestion completed for this filing'
)
COMMENT = 'Log of every SEC filing detected by edgar_monitor.py. Drives the ingestion pipeline — a Snowflake Stream on this table triggers XBRL loading for any row where processed = FALSE.'
;


-- =============================================================================
-- MARTS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- marts.fct_company_metrics
--
-- One row per (ticker, period_end, form).
-- Built by dbt from raw_xbrl_facts by pivoting canonical concepts into columns.
-- Gross margin is computed here (gross_profit / revenue) so every downstream
-- consumer gets a consistent definition.
-- Do not edit directly — managed by dbt.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.marts.fct_company_metrics (

    -- grain
    ticker              VARCHAR(10)  NOT NULL COMMENT 'Exchange ticker symbol',
    period_end          DATE         NOT NULL COMMENT 'End of the reporting period (fiscal quarter or year end)',
    form                VARCHAR(10)  NOT NULL COMMENT 'SEC form type that sourced this row: 10-K, 10-Q, 20-F, or 6-K',
    fy                  INTEGER               COMMENT 'Fiscal year as reported',
    fp                  VARCHAR(5)            COMMENT 'Fiscal period code: FY, Q1, Q2, Q3',

    -- income statement
    revenue             FLOAT                 COMMENT 'Net revenue / net sales (USD)',
    gross_profit        FLOAT                 COMMENT 'Gross profit (USD)',
    gross_margin_pct    FLOAT                 COMMENT 'Gross profit / revenue × 100 (percentage points)',
    operating_income    FLOAT                 COMMENT 'Operating income / loss (USD)',
    net_income          FLOAT                 COMMENT 'Net income / loss attributable to common shareholders (USD)',

    -- expense detail
    r_and_d             FLOAT                 COMMENT 'Research and development expense (USD)',
    capex               FLOAT                 COMMENT 'Capital expenditures (USD, positive = spending)',

    -- balance sheet (period-end snapshot)
    assets              FLOAT                 COMMENT 'Total assets (USD)',
    cash                FLOAT                 COMMENT 'Cash and cash equivalents (USD)',

    -- earnings per share
    eps_basic           FLOAT                 COMMENT 'Basic earnings per share (USD per share)',

    -- housekeeping
    dbt_updated_at      TIMESTAMP_NTZ         COMMENT 'UTC timestamp of last dbt run that touched this row'
)
COMMENT = 'One row per (ticker, period_end, form). Core analytical table — pivoted and enriched from raw_xbrl_facts by dbt. Gross margin is computed here for consistency. Primary input for peer benchmarks and alert evaluation.'
;


-- -----------------------------------------------------------------------------
-- marts.fct_peer_benchmarks
--
-- One row per (peer_group, period_end, metric).
-- Built by dbt from fct_company_metrics by grouping companies into peer sets
-- and computing distribution statistics across those sets.
-- Used for z-score ranking and relative positioning in alerts.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.marts.fct_peer_benchmarks (

    -- grain
    peer_group          VARCHAR(50)  NOT NULL COMMENT 'Analyst-defined peer group (e.g. gpu_manufacturers, hyperscalers, semi_equipment)',
    period_end          DATE         NOT NULL COMMENT 'End of the reporting period',
    form                VARCHAR(10)  NOT NULL COMMENT 'SEC form type for this cohort snapshot',
    metric              VARCHAR(100) NOT NULL COMMENT 'Metric name matching a column in fct_company_metrics (e.g. gross_margin_pct)',

    -- distribution
    n_companies         INTEGER               COMMENT 'Number of companies with non-NULL values for this metric in this period',
    p25                 FLOAT                 COMMENT '25th percentile value across the peer group',
    median              FLOAT                 COMMENT '50th percentile (median) value across the peer group',
    p75                 FLOAT                 COMMENT '75th percentile value across the peer group',
    mean                FLOAT                 COMMENT 'Arithmetic mean across the peer group',
    stddev              FLOAT                 COMMENT 'Population standard deviation across the peer group',

    -- housekeeping
    dbt_updated_at      TIMESTAMP_NTZ         COMMENT 'UTC timestamp of last dbt run that touched this row'
)
COMMENT = 'Peer group distribution statistics per metric per period. Built by dbt from fct_company_metrics. Supports z-score ranking of any company against its peer group. Peer group membership is defined in dbt seeds.'
;


-- =============================================================================
-- MONITORING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- monitoring.alert_rules
--
-- One row per (ticker, metric, threshold_type) alert configuration.
-- Managed manually or via a future admin UI.  Evaluated by the alert engine
-- (alerts/alert_engine.py) whenever fct_company_metrics is refreshed.
--
-- threshold_type values:
--   qoq_drop_bps    gross_margin_pct dropped by more than threshold_value basis points QoQ
--   yoy_drop_bps    same, year-over-year
--   pct_miss        value is more than threshold_value % below the trailing 4Q average
--   pct_increase    value increased more than threshold_value % YoY (e.g. capex surge)
--   abs_below       value is below threshold_value in absolute terms
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.monitoring.alert_rules (

    rule_id             INTEGER      NOT NULL AUTOINCREMENT PRIMARY KEY
                                     COMMENT 'Surrogate key',
    ticker              VARCHAR(10)           COMMENT 'Ticker to monitor. NULL means apply to all tickers.',
    metric              VARCHAR(100) NOT NULL COMMENT 'Metric column in fct_company_metrics to evaluate',
    threshold_type      VARCHAR(30)  NOT NULL COMMENT 'Comparison type: qoq_drop_bps | yoy_drop_bps | pct_miss | pct_increase | abs_below',
    threshold_value     FLOAT        NOT NULL COMMENT 'Numeric threshold — interpretation depends on threshold_type',
    form_filter         VARCHAR(10)           COMMENT 'Restrict to a specific form type (e.g. 10-K). NULL = any form.',
    enabled             BOOLEAN      NOT NULL DEFAULT TRUE
                                     COMMENT 'Inactive rules are retained for history but never evaluated',

    -- housekeeping
    created_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Alert rule configuration. One row per monitored (ticker, metric, threshold_type) combination. Evaluated by alerts/alert_engine.py after every dbt run. Ticker = NULL means the rule applies across all tickers in fct_company_metrics.'
;


-- -----------------------------------------------------------------------------
-- monitoring.alert_history
--
-- One row per alert firing.
-- Written by alerts/alert_engine.py when a rule threshold is breached.
-- slack_sent tracks whether the Slack webhook call succeeded so the engine
-- can retry failed deliveries without re-evaluating rules.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.monitoring.alert_history (

    alert_id            INTEGER      NOT NULL AUTOINCREMENT PRIMARY KEY
                                     COMMENT 'Surrogate key',
    rule_id             INTEGER               COMMENT 'FK to monitoring.alert_rules.rule_id',
    ticker              VARCHAR(10)  NOT NULL COMMENT 'Ticker that triggered the alert',
    metric              VARCHAR(100) NOT NULL COMMENT 'Metric that breached the threshold',
    period_end          DATE         NOT NULL COMMENT 'Period end of the data point that triggered the alert',
    form                VARCHAR(10)           COMMENT 'SEC form type of the triggering data point',

    -- breach values
    triggered_value     FLOAT        NOT NULL COMMENT 'Actual metric value that breached the threshold',
    threshold_value     FLOAT        NOT NULL COMMENT 'Threshold value from the rule at time of firing',
    prior_value         FLOAT                 COMMENT 'Prior period value (QoQ or YoY comparator), if applicable',
    change_amount       FLOAT                 COMMENT 'Absolute change from prior_value to triggered_value',
    change_pct          FLOAT                 COMMENT 'Percentage change from prior_value to triggered_value',

    -- delivery
    alerted_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                                      COMMENT 'UTC timestamp when the alert engine detected the breach',
    slack_sent          BOOLEAN       NOT NULL DEFAULT FALSE
                                      COMMENT 'TRUE once the Slack webhook call returned 200',
    slack_sent_at       TIMESTAMP_NTZ          COMMENT 'UTC timestamp of successful Slack delivery',
    slack_message_ts    VARCHAR(50)            COMMENT 'Slack message timestamp — enables threading follow-up messages'
)
COMMENT = 'History of every alert firing. Written by alerts/alert_engine.py. Tracks both the breach values and Slack delivery status. slack_sent = FALSE rows are retried on the next engine run.'
;


-- =============================================================================
-- indexes / clustering (optional — add once data volume warrants it)
-- =============================================================================

-- ALTER TABLE financial_intelligence.staging.raw_xbrl_facts
--     CLUSTER BY (ticker, concept);

-- ALTER TABLE financial_intelligence.marts.fct_company_metrics
--     CLUSTER BY (ticker, period_end);

-- ALTER TABLE financial_intelligence.monitoring.alert_history
--     CLUSTER BY (ticker, alerted_at);
