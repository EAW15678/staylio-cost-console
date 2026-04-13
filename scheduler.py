"""
Staylio Cost Control Console — Sync Scheduler

Runs connector syncs on a schedule. Deploy as a separate Railway service
or as a cron job in the same service.

Recommended Railway cron config:
  openai:      0 * * * *    (hourly — CSV import is manual, this handles any API sync)
  cloudflare:  */30 * * * * (every 30 min)
  railway:     */15 * * * * (every 15 min — once GraphQL connector is verified)

For Railway cron setup:
  railway cron add "sync_cloudflare" --schedule "*/30 * * * *" --command "python scheduler.py cloudflare"
  railway cron add "sync_railway" --schedule "*/15 * * * *" --command "python scheduler.py railway"
"""

from __future__ import annotations

import os
import sys

from repository import PostgresCostConsoleRepository


def sync_cloudflare() -> None:
    dsn = os.environ.get("COST_CONSOLE_DSN")
    if not dsn:
        raise SystemExit("COST_CONSOLE_DSN not set")

    cf_token = os.environ.get("CF_API_TOKEN")
    cf_account = os.environ.get("CF_ACCOUNT_ID")
    if not cf_token or not cf_account:
        raise SystemExit("CF_API_TOKEN or CF_ACCOUNT_ID not set")

    from connectors.cloudflare_r2 import CloudflareR2Connector
    repo = PostgresCostConsoleRepository(dsn)
    connector = CloudflareR2Connector(repo=repo, api_token=cf_token, account_id=cf_account)
    sync_run = connector.sync_r2_usage(days_back=1)
    print(f"[scheduler] cloudflare_r2: {sync_run.status} — {sync_run.rows_ingested} rows")


def sync_railway() -> None:
    """
    Placeholder until Railway GraphQL fields are verified.
    See connectors/railway.py for verification steps.
    """
    print("[scheduler] railway: SKIPPED — connector pending GraphQL field verification")
    print("[scheduler] Action required: verify field names in connectors/railway.py")


JOBS = {
    "cloudflare": sync_cloudflare,
    "railway": sync_railway,
}


if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else None
    if not job or job not in JOBS:
        print(f"Usage: python scheduler.py [{' | '.join(JOBS)}]")
        sys.exit(1)
    JOBS[job]()
