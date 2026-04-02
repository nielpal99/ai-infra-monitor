# ai-infra-monitor

Real-time financial intelligence for AI infrastructure. Monitors SEC EDGAR filings for 50 AI infrastructure companies, parses XBRL financial data into Snowflake, runs dbt transforms for peer benchmarks and trend metrics, and fires Slack alerts when significant changes are detected — gross margin compression, revenue misses, capex shifts.

Built for investors and analysts who need to know when something changes before the market does.

> Builds on [finance-rag](https://github.com/nielpal99/finance-rag) — the RAG pipeline for querying these same filings with natural language.

---

## Architecture

```
SEC EDGAR API
(XBRL, filings)
      │
      ▼
┌─────────────────────┐
│   Modal Ingestor    │  ← scheduled daily, 50 tickers
│   (Python)          │
└────────┬────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│                   Snowflake                          │
│                                                      │
│  financial_intelligence.staging                      │
│  ├── raw_xbrl_facts        (EDGAR XBRL API)          │
│  └── raw_filings_log       (filing metadata)         │
│                                                      │
│  financial_intelligence.marts  ← dbt transforms      │
│  ├── fct_financials        (normalized metrics)      │
│  ├── fct_qoq_changes       (quarter-over-quarter)    │
│  ├── fct_yoy_changes       (year-over-year)          │
│  └── fct_peer_benchmarks   (cross-company z-scores)  │
│                                                      │
│  Snowflake Streams + Tasks                           │
│  └── detect_threshold_breaches()  (runs on new rows) │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
             ┌─────────────────┐
             │  Alert Engine   │
             │  (Python)       │
             └────────┬────────┘
                      │
                      ▼
             ┌─────────────────┐
             │  Slack Webhook  │
             │  (formatted     │
             │   alert cards)  │
             └─────────────────┘
                      │
                      ▼
             ┌─────────────────┐
             │  Braintrust     │
             │  Eval Suite     │
             │  (extraction    │
             │   accuracy)     │
             └─────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data warehouse | Snowflake |
| Transforms | dbt Core |
| Ingestion compute | Modal (serverless Python) |
| Filing source | SEC EDGAR XBRL API |
| Change detection | Snowflake Streams + Tasks |
| Alerting | Slack webhooks |
| Eval tracking | Braintrust |
| Language | Python 3.11 |

---

## Planned Features

### Daily EDGAR Filing Monitor
A Snowflake Task runs on a daily schedule, calling the EDGAR submissions API for each of the 50 tracked tickers. New filings trigger downstream ingestion automatically — no polling required.

### XBRL Parser (50 Companies)
Parses US-GAAP and IFRS XBRL facts from EDGAR's structured data API. Covers the full AI infrastructure universe: hyperscalers, GPU manufacturers, networking, memory, foundries, and GPU cloud providers.

### dbt Models — Metrics and Benchmarks
- **`fct_financials`** — normalized per-company metrics (revenue, gross margin, operating income, R&D spend, capex) with consistent fiscal period alignment
- **`fct_qoq_changes`** — quarter-over-quarter deltas and growth rates for every metric
- **`fct_yoy_changes`** — year-over-year comparisons with rolling 4-quarter averages
- **`fct_peer_benchmarks`** — z-score ranking of each company against its peer group, updated on every new filing

### Threshold-Based Alerting
Configurable per-metric thresholds trigger alerts when:
- Gross margin compresses more than X bps quarter-over-quarter
- Revenue misses trailing 4-quarter average by more than X%
- Capex increases or decreases by more than X% year-over-year
- R&D spend as a % of revenue moves outside historical range

### Slack Integration
Alerts are delivered as formatted Slack messages with:
- Company name, ticker, and filing date
- Metric name and the specific change detected
- Historical context (prior 4 quarters inline)
- Link to the source EDGAR filing

### Braintrust Eval Suite
Tracks XBRL extraction accuracy over time — verifying that parsed metric values match ground-truth financials from a curated test set. Experiments are logged per ingestion run so regressions are caught before they reach production alerts.

---

## Status

**Live and running daily.**

| Component | Status |
|---|---|
| Snowflake schema | ✅ Complete |
| EDGAR XBRL ingestor | ✅ Complete — 25,000+ rows across 47 companies |
| dbt models | ✅ Complete — stg_xbrl_facts, fct_company_metrics, fct_peer_benchmarks |
| Modal daily scheduler | ✅ Complete — runs 9am ET |
| 8-K item filtering | ✅ Complete |
| Slack alert engine | ✅ Complete |
| Braintrust eval suite | 🚧 In progress |
| Snowflake Streams/Tasks | ⏭ Deferred — replaced by Modal scheduler |

---

## Related Projects

- **[finance-rag](https://github.com/nielpal99/finance-rag)** — Natural language querying of SEC filings and earnings transcripts for 7 AI infrastructure companies. Uses the same EDGAR data source; ai-infra-monitor extends coverage to 50 companies with structured metric tracking and automated alerting.
