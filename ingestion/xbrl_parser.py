"""
Parse XBRL financial facts from the SEC EDGAR companyfacts API.

Extracts key financial metrics (revenue, net income, gross profit, etc.)
for any company in the EDGAR database. Supports both US-GAAP and IFRS
taxonomies for domestic and foreign filers.

Usage:
    python3 ingestion/xbrl_parser.py --ticker NVDA --cik 0001045810
    python3 ingestion/xbrl_parser.py --ticker TSM  --cik 0001046179
    python3 ingestion/xbrl_parser.py --ticker ASML --cik 0000937556 --forms 20-F
"""

import argparse
import requests
from typing import Optional

# ── constants ─────────────────────────────────────────────────────────────────

EDGAR_BASE  = "https://data.sec.gov"
USER_AGENT  = "nielpal niel@example.com"   # required by EDGAR fair-use policy
WATCH_FORMS = {"10-K", "10-Q", "20-F", "6-K"}

# ── concept map ───────────────────────────────────────────────────────────────
# Maps canonical concept name → ([us-gaap variants], [ifrs-full variants])
# Variants are tried in order; first one present in the company's facts wins.

CONCEPT_MAP: dict[str, tuple[list[str], list[str]]] = {
    "Revenues": (
        [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
        ],
        [
            "Revenue",
            "RevenueFromContractsWithCustomers",
            "RevenueFromSaleOfGoods",
        ],
    ),
    "NetIncomeLoss": (
        [
            "NetIncomeLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic",
        ],
        [
            "ProfitLoss",
            "ProfitLossAttributableToOwnersOfParent",
        ],
    ),
    "GrossProfit": (
        ["GrossProfit"],
        ["GrossProfit"],
    ),
    "OperatingIncomeLoss": (
        ["OperatingIncomeLoss"],
        [
            "ProfitLossFromOperatingActivities",
            "OperatingProfitLoss",
        ],
    ),
    "EarningsPerShareBasic": (
        ["EarningsPerShareBasic"],
        [
            "BasicEarningsLossPerShare",
            "EarningsLossPerShare",
        ],
    ),
    "Assets": (
        ["Assets"],
        ["Assets"],
    ),
    "CashAndCashEquivalentsAtCarryingValue": (
        [
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsAndShortTermInvestments",
        ],
        [
            "CashAndCashEquivalents",
            "CashAndBankBalancesAtCentralBanks",
        ],
    ),
    "ResearchAndDevelopmentExpense": (
        ["ResearchAndDevelopmentExpense"],
        ["ResearchAndDevelopmentExpense"],
    ),
    "CapitalExpendituresIncurringObligation": (
        [
            "CapitalExpendituresIncurringObligation",
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsForCapitalImprovements",
        ],
        [
            "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
            "AcquisitionOfPropertyPlantAndEquipment",
            "PurchaseOfPropertyPlantAndEquipment",
        ],
    ),
}


# ── EDGAR helpers ─────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}


def fetch_company_facts(cik: str) -> dict:
    """Fetch the XBRL companyfacts JSON for a given CIK.

    Args:
        cik: Zero-padded 10-digit CIK string (e.g. "0001045810").

    Returns:
        Parsed JSON response from the EDGAR companyfacts API.
    """
    url = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik}.json"
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── extraction ────────────────────────────────────────────────────────────────

def _extract_concept(
    ticker: str,
    cik: str,
    canonical: str,
    facts: dict,
    taxonomy: str,
    variants: list[str],
    watch_forms: set[str],
) -> list[dict]:
    """Extract qualifying facts for one canonical concept from one taxonomy.

    Collects rows from every variant that has matching data, then returns
    only the rows belonging to the variant whose most recent period_end is
    latest.  This ensures companies that switched XBRL tags over time (e.g.
    AMD moving from Revenues → RevenueFromContractWithCustomerExcludingAssessedTax)
    are represented by the tag that covers their current filings, not an older
    tag that may only have data from a decade ago.

    Skips entries whose form type is not in *watch_forms*.

    Returns:
        List of fact dicts. Empty if no variant or no matching entries found.
    """
    tax_data = facts.get(taxonomy, {})

    # Collect rows per variant, tracking each variant's most recent period_end
    best_variant: str | None = None
    best_max_date: str = ""
    rows_by_variant: dict[str, list[dict]] = {}

    for variant in variants:
        if variant not in tax_data:
            continue
        rows: list[dict] = []
        for unit, entries in tax_data[variant].get("units", {}).items():
            for e in entries:
                if e.get("form") not in watch_forms:
                    continue
                rows.append({
                    "ticker":        ticker,
                    "cik":           cik,
                    "taxonomy":      taxonomy,
                    "concept":       canonical,
                    "xbrl_concept":  variant,
                    "value":         e["val"],
                    "unit":          unit,
                    "period_start":  e.get("start"),   # None for instant facts
                    "period_end":    e["end"],
                    "form":          e["form"],
                    "filed":         e["filed"],
                    "accession":     e["accn"],
                    "fy":            e.get("fy"),
                    "fp":            e.get("fp"),
                })
        if not rows:
            continue
        rows_by_variant[variant] = rows
        max_date = max(r["period_end"] for r in rows)
        if max_date > best_max_date:
            best_max_date = max_date
            best_variant = variant

    return rows_by_variant.get(best_variant, []) if best_variant else []


def extract_facts(
    ticker: str,
    cik: str,
    facts: dict,
    watch_forms: Optional[set[str]] = None,
) -> list[dict]:
    """Extract all canonical concepts from the XBRL facts dict.

    Tries US-GAAP first for each concept; falls back to IFRS-full if no
    US-GAAP data exists (covers TSM, ASML, ARM and other foreign filers).

    Args:
        ticker:      Ticker symbol.
        cik:         Zero-padded 10-digit CIK.
        facts:       The ``facts`` sub-dict from the companyfacts JSON.
        watch_forms: Set of form types to include. Defaults to WATCH_FORMS.

    Returns:
        Flat list of fact dicts across all canonical concepts.
    """
    forms = watch_forms or WATCH_FORMS
    all_rows = []
    for canonical, (gaap_variants, ifrs_variants) in CONCEPT_MAP.items():
        rows = _extract_concept(ticker, cik, canonical, facts, "us-gaap", gaap_variants, forms)
        if not rows:
            rows = _extract_concept(ticker, cik, canonical, facts, "ifrs-full", ifrs_variants, forms)
        all_rows.extend(rows)
    return all_rows


def deduplicate_facts(rows: list[dict]) -> list[dict]:
    """For each (concept, period_end, form, unit), keep the most-recently filed entry.

    This removes older amendments and restatements of the same reporting period
    while preserving distinct periods across different form types.

    Returns:
        Sorted by (concept, period_end).
    """
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row["concept"], row["period_end"], row["form"], row["unit"])
        if key not in seen or row["filed"] > seen[key]["filed"]:
            seen[key] = row
    return sorted(seen.values(), key=lambda r: (r["concept"], r["period_end"]))


# ── public API ────────────────────────────────────────────────────────────────

def parse_ticker(
    ticker: str,
    cik: str,
    watch_forms: Optional[set[str]] = None,
    dedup: bool = True,
) -> list[dict]:
    """Full pipeline: fetch companyfacts, extract, optionally deduplicate.

    Args:
        ticker:      Ticker symbol.
        cik:         Zero-padded 10-digit CIK (e.g. "0001045810").
        watch_forms: Form types to include. Defaults to WATCH_FORMS.
        dedup:       If True, keep only the latest-filed entry per
                     (concept, period_end, form, unit).

    Returns:
        List of fact dicts with keys: ticker, cik, taxonomy, concept,
        xbrl_concept, value, unit, period_start, period_end, form, filed,
        accession, fy, fp.
    """
    data  = fetch_company_facts(cik)
    facts = data["facts"]
    rows  = extract_facts(ticker, cik, facts, watch_forms)
    if dedup:
        rows = deduplicate_facts(rows)
    return rows


# ── main ──────────────────────────────────────────────────────────────────────

def _fmt_value(val: float, unit: str) -> str:
    """Format a numeric value with M/B suffix for large USD amounts."""
    if unit in ("USD", "TWD", "EUR") and abs(val) >= 1_000_000:
        billions = val / 1_000_000_000
        if abs(billions) >= 1:
            return f"{billions:>12.2f}B"
        return f"{val / 1_000_000:>12.2f}M"
    return f"{val:>15,.4f}"


def _print_table(rows: list[dict], entity_name: str, ticker: str, cik: str) -> None:
    """Print extracted facts as a formatted table."""
    print(f"\n{entity_name}  ({ticker} / CIK {cik})")
    print(f"{len(rows)} facts after deduplication\n")

    # Header
    print(
        f"{'CONCEPT':<42} {'PERIOD_END':<12} {'FY':<6} {'FP':<5} "
        f"{'FORM':<6} {'FILED':<12} {'VALUE':>14}  {'UNIT'}"
    )
    print("─" * 120)

    prev_concept = None
    for r in rows:
        if r["concept"] != prev_concept:
            if prev_concept is not None:
                print()
            prev_concept = r["concept"]

        val_str = _fmt_value(r["value"], r["unit"])
        print(
            f"  {r['concept']:<40} {r['period_end']:<12} "
            f"{str(r.get('fy') or ''):<6} {str(r.get('fp') or ''):<5} "
            f"{r['form']:<6} {r['filed']:<12} "
            f"{val_str}  {r['unit']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract XBRL financial facts from SEC EDGAR for a given ticker."
    )
    parser.add_argument("--ticker", required=True, help="Ticker symbol (e.g. NVDA)")
    parser.add_argument("--cik",    required=True, help="EDGAR CIK, optionally zero-padded (e.g. 0001045810)")
    parser.add_argument(
        "--forms",
        default=None,
        help="Comma-separated form types to include (default: 10-K,10-Q,20-F,6-K)",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Disable deduplication — show all entries including older restatements",
    )
    args = parser.parse_args()

    ticker = args.ticker.upper()
    cik    = args.cik.zfill(10)
    forms  = set(args.forms.split(",")) if args.forms else None

    print(f"Fetching XBRL companyfacts for {ticker} (CIK {cik}) …")
    data   = fetch_company_facts(cik)
    entity = data.get("entityName", ticker)
    facts  = data["facts"]

    taxonomies = [k for k in facts if k not in ("dei",)]
    print(f"Taxonomies available: {', '.join(taxonomies)}")

    rows = extract_facts(ticker, cik, facts, forms)
    print(f"Raw facts extracted: {len(rows)}")

    if not args.no_dedup:
        rows = deduplicate_facts(rows)

    if not rows:
        print("No matching facts found.")
        return

    _print_table(rows, entity, ticker, cik)


if __name__ == "__main__":
    main()
