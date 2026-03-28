"""
Monitor SEC EDGAR for new 10-K, 10-Q, and 8-K filings from AI infrastructure companies.

Checks the EDGAR submissions API for each ticker in WATCHLIST, compares against
the last-seen filing date stored in data/last_seen.json, and returns any new filings.

Usage:
    python3 ingestion/edgar_monitor.py           # check and update last_seen.json
    python3 ingestion/edgar_monitor.py --dry-run # check only, no state written
"""

import json
import argparse
import requests
from datetime import date
from pathlib import Path
from typing import Optional

# ── constants ─────────────────────────────────────────────────────────────────

EDGAR_BASE  = "https://data.sec.gov"
USER_AGENT  = "nielpal niel@example.com"   # required by EDGAR fair-use policy
WATCH_FORMS = {"10-K", "10-Q", "8-K"}

LAST_SEEN_PATH = Path(__file__).parent.parent / "data" / "last_seen.json"

# ── watchlist ─────────────────────────────────────────────────────────────────

WATCHLIST = {
    # GPU & AI chip manufacturers
    "NVDA": "0001045810",   # NVIDIA
    "AMD":  "0000002488",   # Advanced Micro Devices
    "INTC": "0000050863",   # Intel
    "ARM":  "0001764046",   # Arm Holdings
    "MRVL": "0001058057",   # Marvell Technology
    # Networking & infrastructure
    "AVGO": "0001730168",   # Broadcom
    "ANET": "0001596532",   # Arista Networks
    # Memory & storage
    "MU":   "0000723125",   # Micron Technology
    # Foundry
    "TSM":  "0001046179",   # Taiwan Semiconductor (20-F)
    # GPU cloud
    "CRWV": "0001769628",   # CoreWeave
}

# ── EDGAR helpers ─────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}


def _fetch_submissions(cik: str) -> dict:
    """Fetch the submissions JSON for a given CIK."""
    url = f"{EDGAR_BASE}/submissions/CIK{cik}.json"
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def _accession_url(cik: str, accession: str) -> str:
    acc_path = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc_path}/"


# ── state helpers ─────────────────────────────────────────────────────────────

def _load_last_seen() -> dict:
    """Load last-seen filing dates from disk. Returns empty dict if not found."""
    if LAST_SEEN_PATH.exists():
        return json.loads(LAST_SEEN_PATH.read_text())
    return {}


def _save_last_seen(state: dict) -> None:
    LAST_SEEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    LAST_SEEN_PATH.write_text(json.dumps(state, indent=2))


# ── core logic ────────────────────────────────────────────────────────────────

def check_ticker(ticker: str, cik: str, last_seen_date: Optional[str]) -> list:
    """Return list of new filings for *ticker* since *last_seen_date*.

    Args:
        ticker:          Ticker symbol.
        cik:             EDGAR CIK (zero-padded to 10 digits).
        last_seen_date:  ISO date string (YYYY-MM-DD) of the last filing we
                         recorded, or None to treat everything as new.

    Returns:
        List of dicts with keys: ticker, form_type, filed_date, accession_number, url.
    """
    data    = _fetch_submissions(cik)
    filings = data["filings"]["recent"]
    forms   = filings["form"]
    dates   = filings["filingDate"]
    accnos  = filings["accessionNumber"]

    new_filings = []
    for form, filed, accno in zip(forms, dates, accnos):
        if form not in WATCH_FORMS:
            continue
        if last_seen_date and filed <= last_seen_date:
            continue
        new_filings.append({
            "ticker":           ticker,
            "form_type":        form,
            "filed_date":       filed,
            "accession_number": accno,
            "url":              _accession_url(cik, accno),
        })

    return new_filings


def run_monitor(dry_run: bool = False) -> list:
    """Check all tickers in WATCHLIST for new filings.

    Args:
        dry_run: If True, prints results but does not update last_seen.json.

    Returns:
        Flat list of new filing dicts across all tickers.
    """
    last_seen = _load_last_seen()
    all_new   = []
    new_state = dict(last_seen)

    for ticker, cik in WATCHLIST.items():
        last_date = last_seen.get(ticker)
        try:
            new = check_ticker(ticker, cik, last_date)
        except Exception as e:
            print(f"  [{ticker}] ERROR: {e}")
            continue

        if new:
            all_new.extend(new)
            latest = max(f["filed_date"] for f in new)
            new_state[ticker] = latest
            print(f"  [{ticker}] {len(new)} new filing(s) — latest: {latest}")
        else:
            print(f"  [{ticker}] no new filings since {last_date or 'beginning'}")

    if all_new and not dry_run:
        _save_last_seen(new_state)
        print(f"\n  Saved state to {LAST_SEEN_PATH}")

    return all_new


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Monitor EDGAR for new filings from AI infrastructure companies."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Check for new filings but do not update last_seen.json",
    )
    args = parser.parse_args()

    today = date.today().isoformat()
    print(f"EDGAR Monitor — {today}")
    print(f"Watching {len(WATCHLIST)} tickers for {', '.join(sorted(WATCH_FORMS))} filings")
    print(f"{'(dry run) ' if args.dry_run else ''}State file: {LAST_SEEN_PATH}\n")

    new_filings = run_monitor(dry_run=args.dry_run)

    print(f"\n{'═' * 60}")
    if not new_filings:
        print("No new filings found.")
        return

    print(f"Found {len(new_filings)} new filing(s):\n")
    for f in sorted(new_filings, key=lambda x: x["filed_date"], reverse=True):
        print(f"  {f['filed_date']}  {f['ticker']:<6} {f['form_type']:<6}  {f['accession_number']}")
        print(f"           {f['url']}")

    if args.dry_run:
        print("\n(dry run — last_seen.json not updated)")


if __name__ == "__main__":
    main()
