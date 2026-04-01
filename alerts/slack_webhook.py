"""
Slack alert engine for the ai-infra-monitor pipeline.

Queries fct_company_metrics and fct_peer_benchmarks for a given ticker,
evaluates threshold rules, and posts a formatted alert to Slack when any
rule fires.

Threshold rules
───────────────
  MARGIN_SWING    Gross margin QoQ change exceeds ±2 percentage points
  REVENUE_SURGE   Revenue QoQ change exceeds +15%
  REVENUE_DROP    Revenue QoQ change falls below -15%
  MARGIN_LAGGARD  Company gross margin is ≥5pp below its peer group median

Usage:
    python3 alerts/slack_webhook.py --ticker NVDA
    python3 alerts/slack_webhook.py --ticker AMD --dry-run
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

# Make ingestion/ importable when run from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from ingestion.edgar_monitor import WATCHLIST

# ── constants ──────────────────────────────────────────────────────────────────

METRICS_TABLE   = "financial_intelligence.marts.fct_company_metrics"
BENCHMARKS_TABLE = "financial_intelligence.marts.fct_peer_benchmarks"

# Peer group lookup — mirrors the CASE in fct_peer_benchmarks.sql
PEER_GROUP: dict[str, str] = {
    "NVDA": "GPU & Compute",   "AMD":  "GPU & Compute",
    "INTC": "GPU & Compute",   "QCOM": "GPU & Compute",
    "MRVL": "GPU & Compute",   "ARM":  "GPU & Compute",
    "CRWV": "GPU & Compute",   "SMCI": "GPU & Compute",
    "TSM":  "Foundry & Equipment", "AMAT": "Foundry & Equipment",
    "LRCX": "Foundry & Equipment", "KLAC": "Foundry & Equipment",
    "TER":  "Foundry & Equipment", "ENTG": "Foundry & Equipment",
    "ONTO": "Foundry & Equipment", "ASML": "Foundry & Equipment",
    "MU":   "Memory",   "WDC":  "Memory",  "STX":  "Memory",
    "ANET": "Networking", "CSCO": "Networking",
    "CIEN": "Networking", "INFN": "Networking",
    "MSFT": "Hyperscalers", "GOOGL": "Hyperscalers",
    "AMZN": "Hyperscalers", "META":  "Hyperscalers",
    "ORCL": "Hyperscalers",
    "SNOW": "AI Software",  "DDOG": "AI Software",
    "MDB":  "AI Software",  "NET":  "AI Software",
    "CFLT": "AI Software",  "GTLB": "AI Software",
    "AVGO": "Custom Silicon", "MCHP": "Custom Silicon",
    "SWKS": "Custom Silicon", "QRVO": "Custom Silicon",
    "MTSI": "Custom Silicon",
}

# Alert thresholds
GROSS_MARGIN_SWING_PP   = 2.0   # percentage points QoQ
REVENUE_CHANGE_PCT      = 15.0  # percent QoQ (absolute value)
PEER_LAGGARD_PP         = 5.0   # pp below peer median


# ── Snowflake connection ───────────────────────────────────────────────────────

def _connect():
    """Open a Snowflake connection using credentials from .env or environment."""
    import snowflake.connector
    load_dotenv()
    required = [
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"Missing Snowflake credentials in .env: {', '.join(missing)}"
        )
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
        database  = os.environ["SNOWFLAKE_DATABASE"],
        schema    = os.environ["SNOWFLAKE_SCHEMA"],
    )


# ── Slack ──────────────────────────────────────────────────────────────────────

def _webhook_url() -> str:
    load_dotenv()
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        raise EnvironmentError("SLACK_WEBHOOK_URL not set in environment or .env")
    return url


def send_alert(
    ticker: str,
    form: str,
    filed_date: str,
    alert_type: str,
    details: dict,
    dry_run: bool = False,
) -> None:
    """Post a filing alert to Slack.

    Args:
        ticker:     Ticker symbol (e.g. "NVDA").
        form:       SEC form type (e.g. "10-Q").
        filed_date: ISO date string of the filing date.
        alert_type: Short rule label (e.g. "MARGIN_SWING").
        details:    Dict with metric values — see _build_message() for keys.
        dry_run:    If True, prints the payload instead of posting.
    """
    message = _build_message(ticker, form, filed_date, alert_type, details)
    payload = {"text": message}

    if dry_run:
        print("── Slack payload (dry run) " + "─" * 34)
        print(message)
        print("─" * 60)
        return

    resp = requests.post(
        _webhook_url(),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10,
    )
    resp.raise_for_status()


def _build_message(
    ticker: str,
    form: str,
    filed_date: str,
    alert_type: str,
    details: dict,
) -> str:
    """Render the Slack message string from metric details."""
    cik        = WATCHLIST.get(ticker, "")
    peer_group = details.get("peer_group", PEER_GROUP.get(ticker, "peers"))

    revenue_b          = (details.get("revenue") or 0) / 1e9
    revenue_qoq        = details.get("revenue_qoq_pct") or 0.0
    gross_margin       = details.get("gross_margin_pct") or 0.0
    gross_margin_qoq   = details.get("gross_margin_qoq_chg") or 0.0
    operating_margin   = details.get("operating_margin_pct") or 0.0
    peer_median        = details.get("peer_median_gross_margin") or 0.0

    filing_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type={form}"
    )

    return (
        f":rotating_light: *{ticker}* filed {form} ({filed_date})\n"
        f"*Alert:* {alert_type}\n"
        f"\n"
        f":bar_chart: *Key Metrics:*\n"
        f"- Revenue: ${revenue_b:.2f}B ({revenue_qoq:+.1f}% QoQ)\n"
        f"- Gross Margin: {gross_margin:.1f}% ({gross_margin_qoq:+.1f}pp QoQ)\n"
        f"- Operating Margin: {operating_margin:.1f}%\n"
        f"\n"
        f":factory: *vs {peer_group} peers:*\n"
        f"- Gross Margin: {gross_margin:.1f}% vs {peer_median:.1f}% median\n"
        f"\n"
        f":page_facing_up: View filing: {filing_url}"
    )


# ── Threshold checks ───────────────────────────────────────────────────────────

def check_thresholds(
    ticker: str,
    conn=None,
) -> Optional[tuple[str, dict]]:
    """Query Snowflake and evaluate alert thresholds for *ticker*.

    Fetches the two most recent annual-or-quarterly rows from
    fct_company_metrics and the current-year peer median gross margin from
    fct_peer_benchmarks.

    Returns:
        (alert_type, details) if any threshold is breached, else None.
        When multiple thresholds fire, the first match in priority order is
        returned: MARGIN_SWING → REVENUE_SURGE → REVENUE_DROP → MARGIN_LAGGARD.
    """
    owned_conn = conn is None
    if owned_conn:
        conn = _connect()

    try:
        cur = conn.cursor()

        # Latest two periods for this ticker (any form type, ordered newest first)
        cur.execute(
            f"""
            SELECT
                period_end,
                form,
                revenue,
                gross_profit,
                gross_margin_pct,
                gross_margin_qoq_chg,
                operating_margin_pct,
                net_margin_pct,
                revenue_qoq_chg
            FROM {METRICS_TABLE}
            WHERE ticker = %s
            ORDER BY period_end DESC
            LIMIT 2
            """,
            (ticker,),
        )
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]
        if not rows:
            print(f"[{ticker}] No rows found in fct_company_metrics.")
            return None

        latest = dict(zip(cols, rows[0]))
        form       = latest["form"]
        period_end = str(latest["period_end"])
        fiscal_year = int(period_end[:4])

        # Peer median gross margin for the ticker's peer group + fiscal year
        peer_group = PEER_GROUP.get(ticker, "Other")
        cur.execute(
            f"""
            SELECT median
            FROM {BENCHMARKS_TABLE}
            WHERE peer_group = %s
              AND fiscal_year = %s
              AND metric = 'gross_margin_pct'
            """,
            (peer_group, fiscal_year),
        )
        peer_row = cur.fetchone()
        peer_median = float(peer_row[0]) if peer_row and peer_row[0] is not None else None

        cur.close()

    finally:
        if owned_conn:
            conn.close()

    # ── Build details dict for message rendering ───────────────────────────────

    gross_margin     = latest.get("gross_margin_pct")
    gross_margin_qoq = latest.get("gross_margin_qoq_chg")
    revenue          = latest.get("revenue")
    revenue_qoq_abs  = latest.get("revenue_qoq_chg")

    # Revenue QoQ as a percentage (requires prior period revenue)
    revenue_qoq_pct: Optional[float] = None
    if revenue is not None and revenue_qoq_abs is not None and revenue != 0:
        prior_revenue = revenue - revenue_qoq_abs
        if prior_revenue != 0:
            revenue_qoq_pct = (revenue_qoq_abs / abs(prior_revenue)) * 100

    details = {
        "peer_group":               peer_group,
        "revenue":                  revenue,
        "revenue_qoq_pct":          revenue_qoq_pct,
        "gross_margin_pct":         gross_margin,
        "gross_margin_qoq_chg":     gross_margin_qoq,
        "operating_margin_pct":     latest.get("operating_margin_pct"),
        "net_margin_pct":           latest.get("net_margin_pct"),
        "peer_median_gross_margin": peer_median,
    }

    # ── Evaluate thresholds in priority order ──────────────────────────────────

    # 1. Gross margin swing (expansion or compression)
    if gross_margin_qoq is not None and abs(gross_margin_qoq) >= GROSS_MARGIN_SWING_PP:
        direction = "expansion" if gross_margin_qoq > 0 else "compression"
        alert_type = f"MARGIN_SWING ({direction}: {gross_margin_qoq:+.1f}pp QoQ)"
        return alert_type, details

    # 2. Revenue surge
    if revenue_qoq_pct is not None and revenue_qoq_pct >= REVENUE_CHANGE_PCT:
        alert_type = f"REVENUE_SURGE ({revenue_qoq_pct:+.1f}% QoQ)"
        return alert_type, details

    # 3. Revenue drop
    if revenue_qoq_pct is not None and revenue_qoq_pct <= -REVENUE_CHANGE_PCT:
        alert_type = f"REVENUE_DROP ({revenue_qoq_pct:+.1f}% QoQ)"
        return alert_type, details

    # 4. Peer laggard
    if (
        gross_margin is not None
        and peer_median is not None
        and (peer_median - gross_margin) >= PEER_LAGGARD_PP
    ):
        gap = peer_median - gross_margin
        alert_type = f"MARGIN_LAGGARD ({gap:.1f}pp below {peer_group} median)"
        return alert_type, details

    return None


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check alert thresholds and post to Slack for a given ticker."
    )
    parser.add_argument("--ticker", required=True, help="Ticker symbol (e.g. NVDA)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print Slack payload without posting",
    )
    args = parser.parse_args()
    ticker = args.ticker.upper()

    print(f"Checking thresholds for {ticker} …")
    result = check_thresholds(ticker)

    if result is None:
        print(f"[{ticker}] No thresholds breached. No alert sent.")
        return

    alert_type, details = result

    # Reconstruct filing context from the details for the message
    # (check_thresholds returns the most recent period)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT period_end, form, filed
            FROM financial_intelligence.staging.stg_xbrl_facts
            WHERE ticker = %s
            ORDER BY period_end DESC
            LIMIT 1
            """,
            (ticker,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if row:
        period_end, form, filed = str(row[0]), row[1], str(row[2])
    else:
        period_end, form, filed = "unknown", "10-K", "unknown"

    print(f"[{ticker}] Alert: {alert_type}")
    send_alert(
        ticker     = ticker,
        form       = form,
        filed_date = filed,
        alert_type = alert_type,
        details    = details,
        dry_run    = args.dry_run,
    )

    if not args.dry_run:
        print(f"[{ticker}] Alert posted to Slack.")


if __name__ == "__main__":
    main()
