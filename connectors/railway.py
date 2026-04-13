"""
Staylio Cost Control Console — Railway Connector

STATUS: Gated — requires GraphQL schema verification before use.

The Railway API is GraphQL-based. The field names for project usage
need to be verified against your live account before this connector
will work. The API endpoint and auth are correct; only the query
field names are provisional.

HOW TO VERIFY (one-time setup):
  1. Go to railway.app and log in
  2. Open browser DevTools (F12) → Network tab
  3. Filter by "graphql"
  4. Navigate to your project's Usage/Metrics page
  5. Look at the GraphQL requests being made
  6. Find the query that returns usage/cost data
  7. Note the exact field names and update the USAGE_QUERY below

Until verified, this connector returns a clear error rather than
silently failing. The API endpoint is gated in main.py.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import requests

from repository import CostConsoleRepository, SyncRun

UTC = timezone.utc

RAILWAY_GRAPHQL_URL = "https://backboard.railway.app/graphql/v2"

# PROVISIONAL — verify field names against live Railway GraphQL schema
USAGE_QUERY = """
query GetUsage($after: String, $before: String) {
  me {
    usage(after: $after, before: $before) {
      estimated_cost
    }
  }
}
"""


class RailwayCostConnector:
    """
    Ingests Railway project usage from GraphQL API.
    Gated until GraphQL field names are verified.
    """

    def __init__(
        self,
        repo: CostConsoleRepository,
        api_token: str,
        vendor_account_id: str = "default",
    ) -> None:
        self._repo = repo
        self._api_token = api_token
        self._vendor_account_id = vendor_account_id

    def sync_project_usage(self, **kwargs) -> SyncRun:
        started_at = datetime.now(UTC)
        sync_run_id = str(uuid.uuid4())
        sync_run = SyncRun(
            sync_run_id=sync_run_id,
            vendor_id="railway",
            started_at=started_at,
            completed_at=datetime.now(UTC),
            status="failed",
            rows_ingested=0,
            error_summary=(
                "Railway connector is gated pending GraphQL schema verification. "
                "See connectors/railway.py for instructions on how to verify "
                "the correct field names before enabling this connector."
            ),
        )
        self._repo.insert_sync_run(sync_run)
        raise RuntimeError(sync_run.error_summary)
