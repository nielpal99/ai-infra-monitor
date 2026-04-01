"""
Load XBRL financial facts from xbrl_parser.parse_ticker() into
financial_intelligence.staging.raw_xbrl_facts in Snowflake.

Filters to USD-denominated facts only for cross-company consistency, then
upserts via MERGE matching on (ticker, concept, period_end, form).

Usage:
    python3 ingestion/snowflake_loader.py --ticker NVDA --cik 0001045810
    python3 ingestion/snowflake_loader.py --ticker TSM  --cik 0001046179
    python3 ingestion/snowflake_loader.py --ticker NVDA --cik 0001045810 --dry-run
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import snowflake.connector
from dotenv import load_dotenv

# Make ingestion/ importable when run from project root
sys.path.insert(0, str(Path(__file__).parent))
from xbrl_parser import parse_ticker

# ── constants ─────────────────────────────────────────────────────────────────

TARGET_TABLE = "financial_intelligence.staging.raw_xbrl_facts"
STAGING_TABLE = "tmp_xbrl_stage"

# ── Snowflake connection ───────────────────────────────────────────────────────

def _connect() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection using credentials from the .env file."""
    load_dotenv()
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
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


# ── filtering ─────────────────────────────────────────────────────────────────

def filter_usd(rows: list[dict]) -> list[dict]:
    """Keep only USD-denominated facts.

    Drops TWD, EUR, and other non-USD entries so that every value in
    raw_xbrl_facts is directly comparable across companies.
    """
    return [r for r in rows if r.get("unit") == "USD"]


# ── loading ───────────────────────────────────────────────────────────────────

_CREATE_STAGE = f"""
CREATE TEMPORARY TABLE IF NOT EXISTS {STAGING_TABLE} (
    ticker        VARCHAR,
    cik           VARCHAR,
    taxonomy      VARCHAR,
    concept       VARCHAR,
    xbrl_concept  VARCHAR,
    value         FLOAT,
    unit          VARCHAR,
    period_start  DATE,
    period_end    DATE,
    form          VARCHAR,
    filed         DATE,
    accession     VARCHAR,
    fy            INTEGER,
    fp            VARCHAR
)
"""

_INSERT_STAGE = f"""
INSERT INTO {STAGING_TABLE}
    (ticker, cik, taxonomy, concept, xbrl_concept, value, unit,
     period_start, period_end, form, filed, accession, fy, fp)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_COUNT_MATCHES = f"""
SELECT COUNT(*)
FROM {TARGET_TABLE} t
JOIN {STAGING_TABLE} s
  ON  t.ticker     = s.ticker
  AND t.concept    = s.concept
  AND t.period_end = s.period_end
  AND t.form       = s.form
"""

_MERGE = f"""
MERGE INTO {TARGET_TABLE} AS t
USING {STAGING_TABLE} AS s
    ON  t.ticker     = s.ticker
    AND t.concept    = s.concept
    AND t.period_end = s.period_end
    AND t.form       = s.form
WHEN MATCHED THEN UPDATE SET
    t.cik          = s.cik,
    t.taxonomy     = s.taxonomy,
    t.xbrl_concept = s.xbrl_concept,
    t.value        = s.value,
    t.unit         = s.unit,
    t.period_start = s.period_start,
    t.filed        = s.filed,
    t.accession    = s.accession,
    t.fy           = s.fy,
    t.fp           = s.fp,
    t.loaded_at    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (ticker, cik, taxonomy, concept, xbrl_concept, value, unit,
     period_start, period_end, form, filed, accession, fy, fp, loaded_at)
VALUES
    (s.ticker, s.cik, s.taxonomy, s.concept, s.xbrl_concept, s.value, s.unit,
     s.period_start, s.period_end, s.form, s.filed, s.accession, s.fy, s.fp,
     CURRENT_TIMESTAMP())
"""


def _row_to_tuple(r: dict) -> tuple:
    """Convert a fact dict to the positional tuple expected by _INSERT_STAGE."""
    return (
        r["ticker"],
        r["cik"],
        r["taxonomy"],
        r["concept"],
        r["xbrl_concept"],
        float(r["value"]),
        r["unit"],
        r.get("period_start") or None,   # None → SQL NULL for instant facts
        r["period_end"],
        r["form"],
        r["filed"],
        r["accession"],
        int(r["fy"]) if r.get("fy") is not None else None,
        r.get("fp") or None,
    )


def load_facts(
    rows: list[dict],
    conn: Optional[snowflake.connector.SnowflakeConnection] = None,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Upsert *rows* into raw_xbrl_facts via a MERGE statement.

    Args:
        rows:    USD-filtered fact dicts from xbrl_parser.
        conn:    Existing Snowflake connection. Opens one from .env if None.
        dry_run: If True, skip the MERGE and just print what would happen.

    Returns:
        (inserted, updated) row counts.
    """
    if not rows:
        print("  No rows to load.")
        return 0, 0

    owned = conn is None
    if owned:
        conn = _connect()

    try:
        cur = conn.cursor()

        # ── stage ──────────────────────────────────────────────────────────────
        cur.execute(_CREATE_STAGE)
        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        cur.executemany(_INSERT_STAGE, [_row_to_tuple(r) for r in rows])
        print(f"  Staged {len(rows)} rows into {STAGING_TABLE}")

        # ── pre-count matches (= rows that will be updated) ────────────────────
        cur.execute(_COUNT_MATCHES)
        update_count = cur.fetchone()[0]
        insert_count = len(rows) - update_count

        if dry_run:
            print(f"  [dry-run] Would insert {insert_count}, update {update_count} rows.")
            print(f"  [dry-run] MERGE into {TARGET_TABLE} skipped.")
            return insert_count, update_count

        # ── merge ──────────────────────────────────────────────────────────────
        cur.execute(_MERGE)
        conn.commit()

        return insert_count, update_count

    finally:
        if owned:
            conn.close()


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load XBRL facts for a ticker into Snowflake raw_xbrl_facts."
    )
    parser.add_argument("--ticker",  required=True, help="Ticker symbol (e.g. NVDA)")
    parser.add_argument("--cik",     required=True, help="EDGAR CIK (e.g. 0001045810)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and stage rows, print counts, but skip the MERGE.",
    )
    args = parser.parse_args()

    ticker = args.ticker.upper()
    cik    = args.cik.zfill(10)

    # ── fetch & parse ──────────────────────────────────────────────────────────
    print(f"Fetching XBRL facts for {ticker} (CIK {cik}) …")
    all_rows = parse_ticker(ticker, cik)
    print(f"  Extracted: {len(all_rows)} facts (all currencies)")

    usd_rows = filter_usd(all_rows)
    dropped  = len(all_rows) - len(usd_rows)
    print(f"  USD only:  {len(usd_rows)} facts ({dropped} non-USD dropped)")

    if not usd_rows:
        print("  Nothing to load.")
        return

    # ── load ───────────────────────────────────────────────────────────────────
    print(f"\nConnecting to Snowflake …")
    inserted, updated = load_facts(usd_rows, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n  {TARGET_TABLE}")
        print(f"  Inserted: {inserted}")
        print(f"  Updated:  {updated}")
        print(f"  Total:    {inserted + updated}")


if __name__ == "__main__":
    main()
