-- =============================================================================
-- financial_intelligence — bootstrap script
--
-- Run this once in a Snowflake worksheet to initialize the database from
-- scratch.  All statements use IF NOT EXISTS and are safe to re-run.
--
-- Order matters: database → schemas → staging tables → mart tables →
-- monitoring tables (alert_rules before alert_history for the FK reference).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS financial_intelligence
    COMMENT = 'AI infrastructure financial intelligence — EDGAR XBRL, metrics, and alerting';

CREATE SCHEMA IF NOT EXISTS financial_intelligence.staging
    COMMENT = 'Raw ingest layer. Append/upsert targets for the Python ingestion pipeline.';

CREATE SCHEMA IF NOT EXISTS financial_intelligence.marts
    COMMENT = 'Analytical mart layer. Built by dbt transforms on top of staging.';

CREATE SCHEMA IF NOT EXISTS financial_intelligence.monitoring
    COMMENT = 'Alert configuration and history.';

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.staging.raw_xbrl_facts (
    ticker          VARCHAR(10)    NOT NULL COMMENT 'Exchange ticker symbol (e.g. NVDA)',
    cik             VARCHAR(10)    NOT NULL COMMENT 'EDGAR CIK, zero-padded to 10 digits',
    taxonomy        VARCHAR(20)    NOT NULL COMMENT 'us-gaap or ifrs-full',
    concept         VARCHAR(100)   NOT NULL COMMENT 'Canonical concept name from CONCEPT_MAP in xbrl_parser.py',
    xbrl_concept    VARCHAR(200)   NOT NULL COMMENT 'Actual XBRL tag reported by the filer (may differ from concept)',
    value           FLOAT          NOT NULL COMMENT 'Reported value in USD (or USD/shares for EPS)',
    unit            VARCHAR(20)    NOT NULL COMMENT 'Unit of measure: USD or USD/shares',
    period_start    DATE                    COMMENT 'Start of reporting period (NULL for instant/balance-sheet facts)',
    period_end      DATE           NOT NULL COMMENT 'End of reporting period — fiscal quarter or year end',
    form            VARCHAR(10)    NOT NULL COMMENT 'SEC form type: 10-K, 10-Q, 20-F, or 6-K',
    filed           DATE           NOT NULL COMMENT 'Date the filing was submitted to EDGAR',
    accession       VARCHAR(25)             COMMENT 'EDGAR accession number',
    fy              INTEGER                 COMMENT 'Fiscal year as reported by the filer',
    fp              VARCHAR(5)              COMMENT 'Fiscal period code: FY, Q1, Q2, Q3',
    loaded_at       TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw XBRL financial facts from the EDGAR companyfacts API. One row per (ticker, concept, period_end, form) after USD filtering and deduplication.';

CREATE TABLE IF NOT EXISTS financial_intelligence.staging.raw_filings_log (
    ticker           VARCHAR(10)   NOT NULL COMMENT 'Exchange ticker symbol',
    cik              VARCHAR(10)   NOT NULL COMMENT 'EDGAR CIK, zero-padded to 10 digits',
    form_type        VARCHAR(10)   NOT NULL COMMENT 'SEC form type: 10-K, 10-Q, 8-K, 20-F, or 6-K',
    accession_number VARCHAR(25)   NOT NULL COMMENT 'EDGAR accession number — globally unique filing identifier',
    filed_date       DATE          NOT NULL COMMENT 'Date the filing was submitted to EDGAR',
    processed        BOOLEAN       NOT NULL DEFAULT FALSE COMMENT 'TRUE once XBRL facts for this filing have been loaded downstream',
    detected_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when edgar_monitor.py first saw this filing',
    processed_at     TIMESTAMP_NTZ          COMMENT 'UTC timestamp when XBRL ingestion completed for this filing'
)
COMMENT = 'Log of every SEC filing detected by edgar_monitor.py. A Snowflake Stream on this table triggers XBRL loading for rows where processed = FALSE.';

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.marts.fct_company_metrics (
    ticker           VARCHAR(10)  NOT NULL COMMENT 'Exchange ticker symbol',
    period_end       DATE         NOT NULL COMMENT 'End of the reporting period',
    form             VARCHAR(10)  NOT NULL COMMENT 'SEC form type: 10-K, 10-Q, 20-F, or 6-K',
    fy               INTEGER               COMMENT 'Fiscal year as reported',
    fp               VARCHAR(5)            COMMENT 'Fiscal period code: FY, Q1, Q2, Q3',
    revenue          FLOAT                 COMMENT 'Net revenue / net sales (USD)',
    gross_profit     FLOAT                 COMMENT 'Gross profit (USD)',
    gross_margin_pct FLOAT                 COMMENT 'Gross profit / revenue × 100 (percentage points)',
    operating_income FLOAT                 COMMENT 'Operating income / loss (USD)',
    net_income       FLOAT                 COMMENT 'Net income / loss (USD)',
    r_and_d          FLOAT                 COMMENT 'Research and development expense (USD)',
    capex            FLOAT                 COMMENT 'Capital expenditures (USD, positive = spending)',
    assets           FLOAT                 COMMENT 'Total assets (USD)',
    cash             FLOAT                 COMMENT 'Cash and cash equivalents (USD)',
    eps_basic        FLOAT                 COMMENT 'Basic earnings per share (USD per share)',
    dbt_updated_at   TIMESTAMP_NTZ         COMMENT 'UTC timestamp of last dbt run that touched this row'
)
COMMENT = 'One row per (ticker, period_end, form). Pivoted from raw_xbrl_facts by dbt. Gross margin computed here for consistency.';

CREATE TABLE IF NOT EXISTS financial_intelligence.marts.fct_peer_benchmarks (
    peer_group       VARCHAR(50)  NOT NULL COMMENT 'Analyst-defined peer group (e.g. gpu_manufacturers, hyperscalers)',
    period_end       DATE         NOT NULL COMMENT 'End of the reporting period',
    form             VARCHAR(10)  NOT NULL COMMENT 'SEC form type for this cohort snapshot',
    metric           VARCHAR(100) NOT NULL COMMENT 'Metric name matching a column in fct_company_metrics',
    n_companies      INTEGER               COMMENT 'Number of companies with non-NULL values for this metric',
    p25              FLOAT                 COMMENT '25th percentile value across the peer group',
    median           FLOAT                 COMMENT '50th percentile (median) value across the peer group',
    p75              FLOAT                 COMMENT '75th percentile value across the peer group',
    mean             FLOAT                 COMMENT 'Arithmetic mean across the peer group',
    stddev           FLOAT                 COMMENT 'Population standard deviation across the peer group',
    dbt_updated_at   TIMESTAMP_NTZ         COMMENT 'UTC timestamp of last dbt run that touched this row'
)
COMMENT = 'Peer group distribution statistics per metric per period. Built by dbt from fct_company_metrics.';

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS financial_intelligence.monitoring.alert_rules (
    rule_id          INTEGER      NOT NULL AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    ticker           VARCHAR(10)           COMMENT 'Ticker to monitor. NULL = apply to all tickers.',
    metric           VARCHAR(100) NOT NULL COMMENT 'Metric column in fct_company_metrics to evaluate',
    threshold_type   VARCHAR(30)  NOT NULL COMMENT 'qoq_drop_bps | yoy_drop_bps | pct_miss | pct_increase | abs_below',
    threshold_value  FLOAT        NOT NULL COMMENT 'Numeric threshold — interpretation depends on threshold_type',
    form_filter      VARCHAR(10)           COMMENT 'Restrict to a specific form type. NULL = any form.',
    enabled          BOOLEAN      NOT NULL DEFAULT TRUE COMMENT 'Inactive rules are retained for history but never evaluated',
    created_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Alert rule configuration. Ticker = NULL means the rule applies to all tickers. Evaluated by alerts/alert_engine.py after every dbt run.';

CREATE TABLE IF NOT EXISTS financial_intelligence.monitoring.alert_history (
    alert_id         INTEGER      NOT NULL AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    rule_id          INTEGER               COMMENT 'FK to monitoring.alert_rules.rule_id',
    ticker           VARCHAR(10)  NOT NULL COMMENT 'Ticker that triggered the alert',
    metric           VARCHAR(100) NOT NULL COMMENT 'Metric that breached the threshold',
    period_end       DATE         NOT NULL COMMENT 'Period end of the data point that triggered the alert',
    form             VARCHAR(10)           COMMENT 'SEC form type of the triggering data point',
    triggered_value  FLOAT        NOT NULL COMMENT 'Actual metric value that breached the threshold',
    threshold_value  FLOAT        NOT NULL COMMENT 'Threshold value from the rule at time of firing',
    prior_value      FLOAT                 COMMENT 'Prior period value (QoQ or YoY comparator)',
    change_amount    FLOAT                 COMMENT 'Absolute change from prior_value to triggered_value',
    change_pct       FLOAT                 COMMENT 'Percentage change from prior_value to triggered_value',
    alerted_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the alert engine detected the breach',
    slack_sent       BOOLEAN       NOT NULL DEFAULT FALSE COMMENT 'TRUE once the Slack webhook call returned 200',
    slack_sent_at    TIMESTAMP_NTZ          COMMENT 'UTC timestamp of successful Slack delivery',
    slack_message_ts VARCHAR(50)            COMMENT 'Slack message timestamp — enables threading follow-up messages'
)
COMMENT = 'History of every alert firing. slack_sent = FALSE rows are retried on the next engine run.';
