"""
Streamlit dashboard for ai-infra-monitor.

Sections:
  1. Latest Metrics Table  — fct_company_metrics, most recent annual row per ticker
  2. Peer Benchmarks Chart — fct_peer_benchmarks, gross_margin_pct medians by peer group
  3. Recent Filings        — edgar_monitor.run_monitor() in dry-run mode
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st

# Make ingestion/ importable
sys.path.insert(0, str(Path(__file__).parent / "ingestion"))
from edgar_monitor import run_monitor  # noqa: E402


# ── Snowflake connection ───────────────────────────────────────────────────────

@st.cache_resource
def _get_conn():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
    )


@st.cache_data(ttl=3600)
def _query(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    return pd.read_sql(sql, conn)


# ── queries ───────────────────────────────────────────────────────────────────

_METRICS_SQL = """
with ranked as (
    select
        ticker,
        period_end,
        revenue,
        gross_margin_pct,
        operating_margin_pct,
        gross_margin_qoq_chg,
        row_number() over (
            partition by ticker
            order by period_end desc
        ) as rn
    from financial_intelligence.marts.fct_company_metrics
    where is_annual = true
)
select
    ticker,
    period_end,
    round(revenue / 1e9, 2)          as revenue_b,
    round(gross_margin_pct, 1)        as gross_margin_pct,
    round(operating_margin_pct, 1)    as operating_margin_pct,
    round(gross_margin_qoq_chg, 2)    as gross_margin_qoq_chg
from ranked
where rn = 1
order by revenue_b desc nulls last
"""

_BENCHMARKS_SQL = """
select
    peer_group,
    round(median, 1) as median_gross_margin_pct
from financial_intelligence.marts.fct_peer_benchmarks
where fiscal_year = 2024
  and metric = 'gross_margin_pct'
order by median_gross_margin_pct desc
"""


# ── color coding for gross_margin_qoq_chg ─────────────────────────────────────

def _color_qoq(val):
    if pd.isna(val):
        return ""
    if val < -2:
        return "color: #e05c5c"
    if val > 2:
        return "color: #4caf7d"
    return ""


# ── page layout ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="AI Infrastructure Monitor",
    page_icon="📡",
    layout="wide",
)

st.title("AI Infrastructure Monitor")
st.caption("Real-time financial intelligence across 47 companies")

st.divider()

# ── section 1: latest metrics ─────────────────────────────────────────────────

st.subheader("Latest Metrics")
st.caption("Most recent annual filing per company, sorted by revenue")

with st.spinner("Loading metrics…"):
    try:
        metrics_df = _query(_METRICS_SQL)
        metrics_df.columns = [
            "Ticker", "Period End", "Revenue ($B)",
            "Gross Margin %", "Operating Margin %", "Gross Margin QoQ Chg (pp)",
        ]

        styled = metrics_df.style.applymap(
            _color_qoq, subset=["Gross Margin QoQ Chg (pp)"]
        ).format({
            "Revenue ($B)": "{:.2f}",
            "Gross Margin %": "{:.1f}",
            "Operating Margin %": "{:.1f}",
            "Gross Margin QoQ Chg (pp)": lambda v: f"{v:+.2f}" if pd.notna(v) else "—",
        })

        st.dataframe(styled, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not load metrics: {e}")

st.divider()

# ── section 2: peer benchmarks chart ─────────────────────────────────────────

st.subheader("Peer Benchmarks — Gross Margin % (FY 2024)")
st.caption("Median gross margin by peer group, annual filings only")

with st.spinner("Loading benchmarks…"):
    try:
        bench_df = _query(_BENCHMARKS_SQL)
        bench_df.columns = ["Peer Group", "Median Gross Margin %"]

        fig = px.bar(
            bench_df,
            x="Peer Group",
            y="Median Gross Margin %",
            text="Median Gross Margin %",
            color="Median Gross Margin %",
            color_continuous_scale="Blues",
        )
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(
            coloraxis_showscale=False,
            yaxis_title="Median Gross Margin (%)",
            xaxis_title=None,
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load benchmarks: {e}")

st.divider()

# ── section 3: recent filings ─────────────────────────────────────────────────

st.subheader("Recent Filings")
st.caption("New filings detected since last run (dry-run — state not updated)")

with st.spinner("Checking EDGAR…"):
    try:
        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            filings = run_monitor(dry_run=True)

        if filings:
            filings_df = pd.DataFrame(filings)[
                ["filed_date", "ticker", "form_type", "accession_number", "url"]
            ]
            filings_df.columns = ["Filed", "Ticker", "Form", "Accession", "URL"]
            filings_df = filings_df.sort_values("Filed", ascending=False)
            st.dataframe(
                filings_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "URL": st.column_config.LinkColumn("URL", display_text="View on SEC"),
                },
            )
        else:
            st.info("No new filings detected since last run.")
    except Exception as e:
        st.error(f"Could not check EDGAR: {e}")
