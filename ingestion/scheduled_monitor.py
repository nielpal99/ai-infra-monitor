"""
Modal scheduled job for the ai-infra-monitor pipeline.

Runs daily at 06:00 ET (10:00 UTC). Orchestrates three steps:

  1. edgar_monitor.run_monitor()  — poll EDGAR for new filings across all 47
                                    watchlist tickers; returns list of new filings
  2. snowflake_loader.load_ticker() — for each ticker with new filings, refresh
                                       its XBRL facts in Snowflake
  3. dbt run                       — rebuild fct_company_metrics and
                                      fct_peer_benchmarks from the updated facts

Modal captures all stdout automatically; run history is visible in the
Modal dashboard under the "ai-infra-monitor" app.

Deploy:
    modal deploy ingestion/scheduled_monitor.py

Invoke manually (e.g. to backfill):
    modal run ingestion/scheduled_monitor.py
"""

import subprocess
import sys
from pathlib import Path

import modal

# ── Modal app ──────────────────────────────────────────────────────────────────

app = modal.App("ai-infra-monitor")

# ── Image ──────────────────────────────────────────────────────────────────────
#
# All runtime dependencies baked into the container image. dbt-snowflake pulls
# in dbt-core as a transitive dependency; listing both pins the combination.

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "requests",
        "python-dotenv",
        "snowflake-connector-python",
        "dbt-core",
        "dbt-snowflake",
    )
)

# ── Secret ─────────────────────────────────────────────────────────────────────
#
# The .env file is surfaced as a Modal Secret named "ai-infra-monitor-env".
# Create it once with:
#   modal secret create ai-infra-monitor-env \
#       SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... \
#       SNOWFLAKE_DATABASE=financial_intelligence SNOWFLAKE_SCHEMA=staging \
#       SNOWFLAKE_WAREHOUSE=COMPUTE_WH SNOWFLAKE_ROLE=ACCOUNTADMIN
#
# The function receives these as environment variables; python-dotenv's
# load_dotenv() is a no-op in Modal (no .env file present), but
# snowflake_loader._connect() falls back to os.environ automatically
# because snowflake-connector reads standard env vars directly.

env_secret = modal.Secret.from_name("ai-infra-monitor-env")

# ── Mount — project source ─────────────────────────────────────────────────────
#
# Mount the local repo into /app so all ingestion scripts and the dbt project
# are available at runtime without baking them into the image layer.

repo_mount = modal.Mount.from_local_dir(
    Path(__file__).parent.parent,   # ai-infra-monitor/
    remote_path="/app",
)

# ── Scheduled function ─────────────────────────────────────────────────────────

@app.function(
    image=image,
    secrets=[env_secret],
    mounts=[repo_mount],
    schedule=modal.Cron("0 10 * * *"),   # 10:00 UTC = 06:00 ET
    timeout=1800,                         # 30 min ceiling; full run is ~5 min
)
def run_daily_pipeline() -> None:
    """Daily orchestration: detect new filings → load XBRL → refresh dbt models."""

    import sys
    sys.path.insert(0, "/app")

    from ingestion.edgar_monitor   import run_monitor, WATCHLIST
    from ingestion.snowflake_loader import load_ticker, _connect

    # ── Step 1: EDGAR filing check ─────────────────────────────────────────────

    print("=" * 60)
    print("Step 1 — Checking EDGAR for new filings")
    print("=" * 60)

    new_filings = run_monitor(dry_run=False)

    if not new_filings:
        print("\nNo new filings detected. Pipeline complete.")
        return

    # Deduplicate to one load per ticker (a single filing day may have multiple
    # form types — e.g. both a 10-Q and an 8-K — but one XBRL load covers all).
    tickers_to_load = sorted({f["ticker"] for f in new_filings})
    print(f"\n{len(new_filings)} new filing(s) across {len(tickers_to_load)} ticker(s): "
          f"{', '.join(tickers_to_load)}")

    # ── Step 2: XBRL load for affected tickers ─────────────────────────────────

    print("\n" + "=" * 60)
    print("Step 2 — Loading XBRL facts into Snowflake")
    print("=" * 60)

    conn = _connect()
    load_errors: list[str] = []

    try:
        for ticker in tickers_to_load:
            cik = WATCHLIST[ticker]
            try:
                inserted, updated = load_ticker(ticker, cik, conn=conn)
                print(f"  [{ticker}]  {inserted:>4} inserted  {updated:>4} updated")
            except Exception as exc:
                print(f"  [{ticker}]  ERROR: {exc}", file=sys.stderr)
                load_errors.append(ticker)
    finally:
        conn.close()

    if load_errors:
        print(f"\nLoad failures: {', '.join(load_errors)}", file=sys.stderr)

    # ── Step 3: dbt model refresh ──────────────────────────────────────────────

    print("\n" + "=" * 60)
    print("Step 3 — Refreshing dbt models")
    print("=" * 60)

    dbt_cmd = [
        "dbt", "run",
        "--project-dir", "/app/dbt",
        "--profiles-dir", "/app/dbt",
        "--select", "fct_company_metrics", "fct_peer_benchmarks",
        "--no-version-check",
    ]

    print(f"Running: {' '.join(dbt_cmd)}\n")

    result = subprocess.run(
        dbt_cmd,
        capture_output=False,   # stream dbt's output directly to Modal logs
        text=True,
    )

    if result.returncode != 0:
        print(f"\ndbt run failed (exit {result.returncode})", file=sys.stderr)
        sys.exit(result.returncode)

    # ── Summary ────────────────────────────────────────────────────────────────

    print("\n" + "=" * 60)
    status = "with errors" if load_errors else "successfully"
    print(f"Pipeline complete {status}.")
    if load_errors:
        print(f"  Failed tickers: {', '.join(load_errors)}")
    print("=" * 60)


# ── Manual entry point ─────────────────────────────────────────────────────────
#
# Allows `modal run ingestion/scheduled_monitor.py` to trigger a single run
# outside the schedule — useful for backfills and testing deploys.

@app.local_entrypoint()
def main() -> None:
    run_daily_pipeline.remote()
