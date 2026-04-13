"""
Anthropic Cost Connector — Uses the Admin API Cost Report endpoint.
Requires an Admin API key (sk-ant-admin...) set as ANTHROPIC_ADMIN_KEY env var.
Docs: https://docs.anthropic.com/en/api/usage-cost-api
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any

import requests

from repository import CostEvent, PostgresCostConsoleRepository, SyncRun, stable_id

UTC = timezone.utc

COST_REPORT_URL = "https://api.anthropic.com/v1/organizations/cost_report"


class AnthropicCostConnector:
    def __init__(self, repo: PostgresCostConsoleRepository, admin_api_key: str) -> None:
        self._repo = repo
        self._admin_api_key = admin_api_key

    def sync_cost_report(self, days_back: int = 7) -> SyncRun:
        sync_id = str(uuid.uuid4())
        sync_run = SyncRun(
            sync_run_id=sync_id,
            vendor_id="anthropic",
            started_at=datetime.now(UTC),
            completed_at=None,
            status="running",
            rows_ingested=0,
            error_summary=None,
        )
        self._repo.insert_sync_run(sync_run)

        try:
            now = datetime.now(UTC)
            starting_at = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
            ending_at = now.strftime("%Y-%m-%dT23:59:59Z")

            all_data = []
            page = None

            while True:
                params: dict[str, Any] = {
                    "starting_at": starting_at,
                    "ending_at": ending_at,
                    "group_by[]": ["workspace_id", "description"],
                    "bucket_width": "1d",
                }
                if page:
                    params["page"] = page

                resp = requests.get(
                    COST_REPORT_URL,
                    params=params,
                    headers={
                        "anthropic-version": "2023-06-01",
                        "x-api-key": self._admin_api_key,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                body = resp.json()

                data = body.get("data", [])
                all_data.extend(data)

                if body.get("has_more"):
                    page = body.get("next_page")
                else:
                    break

            cost_events = []
            for bucket in all_data:
                date_str = bucket.get("date", bucket.get("bucket_start_time", ""))
                description = bucket.get("description", "")
                workspace_id = bucket.get("workspace_id")

                # Costs are in cents as decimal strings
                token_cost_cents = Decimal(bucket.get("token_usage_cost", "0"))
                web_search_cost_cents = Decimal(bucket.get("web_search_cost", "0"))
                code_exec_cost_cents = Decimal(bucket.get("code_execution_cost", "0"))
                total_cents = token_cost_cents + web_search_cost_cents + code_exec_cost_cents

                if total_cents == 0:
                    continue

                total_usd = total_cents / Decimal("100")

                event_id = stable_id(
                    "anthropic", "cost_report",
                    date_str, description or "unknown",
                    workspace_id or "default",
                )

                cost_events.append(CostEvent(
                    cost_event_id=event_id,
                    vendor_id="anthropic",
                    vendor_account_id=workspace_id,
                    service_name=description or "claude_api",
                    cost_category="api_usage",
                    cost_usd=total_usd,
                    incurred_at=datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else datetime.now(UTC),
                    source_reference=f"admin_api/cost_report/{starting_at}_to_{ending_at}",
                    raw_payload_json=bucket,
                ))

            rows = self._repo.upsert_cost_events(cost_events)

            sync_run.completed_at = datetime.now(UTC)
            sync_run.status = "success"
            sync_run.rows_ingested = rows
            self._repo.update_sync_run(sync_run)
            return sync_run

        except Exception as exc:
            sync_run.completed_at = datetime.now(UTC)
            sync_run.status = "failed"
            sync_run.error_summary = str(exc)[:500]
            self._repo.update_sync_run(sync_run)
            return sync_run
