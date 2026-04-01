"""
Load XBRL financial facts for every ticker in the WATCHLIST into Snowflake.

Opens a single shared Snowflake connection for the full run, then calls
snowflake_loader.load_ticker() for each ticker in WATCHLIST. Failed tickers
are logged and skipped without stopping the run.

Usage:
    python3 ingestion/load_all_tickers.py
    python3 ingestion/load_all_tickers.py --dry-run
    python3 ingestion/load_all_tickers.py --tickers NVDA AMD MSFT
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from edgar_monitor import WATCHLIST
from snowflake_loader import _connect, load_ticker


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-load XBRL facts for all WATCHLIST tickers into Snowflake."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and stage rows but skip the MERGE into raw_xbrl_facts.",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Load only these tickers instead of the full WATCHLIST.",
    )
    args = parser.parse_args()

    # Build the target list — subset or full watchlist
    if args.tickers:
        unknown = [t for t in args.tickers if t.upper() not in WATCHLIST]
        if unknown:
            print(f"Unknown tickers (not in WATCHLIST): {', '.join(unknown)}")
            sys.exit(1)
        targets = {t.upper(): WATCHLIST[t.upper()] for t in args.tickers}
    else:
        targets = WATCHLIST

    started_at = datetime.utcnow()
    print(f"Load all tickers — {started_at.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'(dry run) ' if args.dry_run else ''}Tickers: {len(targets)}")
    print(f"Target: financial_intelligence.staging.raw_xbrl_facts\n")

    # ── open one shared connection for the full run ────────────────────────────
    conn = _connect()

    total_inserted = 0
    total_updated  = 0
    failed: list[tuple[str, str]] = []   # (ticker, error message)

    for i, (ticker, cik) in enumerate(targets.items(), 1):
        prefix = f"  [{i:>2}/{len(targets)}] {ticker:<6}"
        print(f"{prefix} loading …", end="", flush=True)
        t0 = time.monotonic()

        try:
            inserted, updated = load_ticker(ticker, cik, conn=conn, dry_run=args.dry_run)
            elapsed = time.monotonic() - t0
            total_inserted += inserted
            total_updated  += updated

            parts = []
            if inserted:
                parts.append(f"{inserted} inserted")
            if updated:
                parts.append(f"{updated} updated")
            if not parts:
                parts.append("0 rows (already up to date)")
            print(f"\r{prefix} {', '.join(parts)}  ({elapsed:.1f}s)")

        except Exception as e:
            elapsed = time.monotonic() - t0
            msg = str(e).splitlines()[0]   # first line only, keep output tidy
            print(f"\r{prefix} ERROR — {msg}  ({elapsed:.1f}s)")
            failed.append((ticker, msg))

    conn.close()

    # ── summary ───────────────────────────────────────────────────────────────
    duration = (datetime.utcnow() - started_at).total_seconds()
    print(f"\n{'═' * 60}")
    print(f"Finished in {duration:.0f}s")
    print(f"  Inserted : {total_inserted:>6}")
    print(f"  Updated  : {total_updated:>6}")
    print(f"  Total    : {total_inserted + total_updated:>6}")
    print(f"  Failed   : {len(failed):>6}")

    if failed:
        print(f"\nFailed tickers:")
        for ticker, msg in failed:
            print(f"  {ticker:<6}  {msg}")
        sys.exit(1)

    if args.dry_run:
        print("\n(dry run — raw_xbrl_facts not modified)")


if __name__ == "__main__":
    main()
