"""
Anthropic Cost Connector — Uses the Admin API Cost Report endpoint.
Requires an Admin API key (sk-ant-admin...) set as ANTHROPIC_ADMIN_KEY env var.

Official docs: https://docs.anthropic.com/en/api/admin-api/usage-cost/get-cost-report

IMPORTANT: The 'amount' field is returned in LOWEST CURRENCY UNITS (cents).
Example from docs: "123.45" in "USD" represents $1.23
All amounts must be divided by 100 before storing as USD.
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

            # Collect all daily buckets, handling pagination
            all_buckets: list[dict[str, Any]] = []
            page = None

            while True:
                params: dict[str, Any] = {
                    "starting_at": starting_at,
                    "ending_at": ending_at,
                    "bucket_width": "1d",
                    "group_by[]": "description",
                }
                if page:
                    params["page"] = page

                resp = requests.get(
                    COST_REPORT_URL,
                    params=params,
                    headers={
                        "anthropic-version": "2023-06-01",
                        "x-api-key": self._admin_api_key,
                        "User-Agent": "Staylio-Cost-Console/1.0",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                body = resp.json()

                all_buckets.extend(body.get("data", []))

                if body.get("has_more"):
                    page = body.get("next_page")
                else:
                    break

            # Parse buckets into cost events
            cost_events = []
            for bucket in all_buckets:
                bucket_start = bucket.get("starting_at", "")
                bucket_end = bucket.get("ending_at", "")
                results = bucket.get("results", [])

                for result in results:
                    # CRITICAL: amount is in CENTS per official docs
                    # "123.45" in "USD" represents $1.23
                    amount_cents = Decimal(result.get("amount", "0"))

                    if amount_cents == 0:
                        continue

                    amount_usd = amount_cents / Decimal("100")

                    # Fields populated when group_by[]=description is used
                    description = result.get("description")
                    model = result.get("model")
                    cost_type = result.get("cost_type")
                    service_tier = result.get("service_tier")
                    token_type = result.get("token_type")
                    context_window = result.get("context_window")
                    workspace_id = result.get("workspace_id")

                    # Build deterministic event ID from all identifying fields
                    event_id = stable_id(
                        "anthropic", "cost_report",
                        bucket_start,
                        description or "no_description",
                        model or "no_model",
                        cost_type or "no_cost_type",
                        token_type or "no_token_type",
                        service_tier or "no_tier",
                        workspace_id or "default",
                    )

                    # Build human-readable service name from available fields
                    service_name_parts = [p for p in [model, cost_type, token_type, service_tier] if p]
                    service_name = " / ".join(service_name_parts) if service_name_parts else (description or "claude_api")

                    cost_events.append(CostEvent(
                        cost_event_id=event_id,
                        vendor_id="anthropic",
                        vendor_account_id=workspace_id,
                        service_name=service_name,
                        cost_category=cost_type or "api_usage",
                        cost_usd=amount_usd,
                        incurred_at=datetime.fromisoformat(bucket_start.replace("Z", "+00:00")) if bucket_start else datetime.now(UTC),
                        source_reference=f"admin_api/cost_report/{starting_at}_to_{ending_at}",
                        raw_payload_json={
                            "bucket_start": bucket_start,
                            "bucket_end": bucket_end,
                            "amount_cents": str(amount_cents),
                            "amount_usd": str(amount_usd),
                            "description": description,
                            "model": model,
                            "cost_type": cost_type,
                            "service_tier": service_tier,
                            "token_type": token_type,
                            "context_window": context_window,
                        },
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
