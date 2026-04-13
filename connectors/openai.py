"""
Staylio Cost Control Console — OpenAI Connector

Ingests cost and usage data from OpenAI via CSV exports.

How to export from OpenAI:
  1. Go to platform.openai.com/usage
  2. Click Export
  3. Download both the Activity CSV and Cost CSV
  4. Upload each via the API endpoints:
     POST /sync/openai/activity
     POST /sync/openai/cost
"""

from __future__ import annotations

import csv
import io
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from repository import (
    CostEvent,
    CostConsoleRepository,
    SyncRun,
    UsageEvent,
    stable_id,
)

UTC = timezone.utc

# OpenAI pricing per 1M tokens (input, output) — update as pricing changes
MODEL_PRICING: dict[str, tuple[Decimal, Decimal]] = {
    "gpt-4o":                    (Decimal("2.50"),  Decimal("10.00")),
    "gpt-4o-mini":               (Decimal("0.15"),  Decimal("0.60")),
    "gpt-4-turbo":               (Decimal("10.00"), Decimal("30.00")),
    "gpt-4":                     (Decimal("30.00"), Decimal("60.00")),
    "gpt-3.5-turbo":             (Decimal("0.50"),  Decimal("1.50")),
    "claude-sonnet-4-20250514":  (Decimal("3.00"),  Decimal("15.00")),
}


def _parse_date(val: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(val.strip(), fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _safe_decimal(val: str) -> Decimal | None:
    try:
        return Decimal(val.strip().replace(",", "").replace("$", ""))
    except InvalidOperation:
        return None


class OpenAICostConnector:
    """Ingests OpenAI usage and cost data from CSV exports."""

    def __init__(
        self,
        repo: CostConsoleRepository,
        vendor_account_id: str = "default",
    ) -> None:
        self._repo = repo
        self._vendor_account_id = vendor_account_id

    def sync_activity_csv(
        self,
        csv_bytes: bytes,
        source_reference: str | None = None,
    ) -> SyncRun:
        """
        Import the Activity CSV from platform.openai.com/usage.
        Maps token usage into usage_events.
        """
        started_at = datetime.now(UTC)
        sync_run_id = str(uuid.uuid4())
        sync_run = SyncRun(
            sync_run_id=sync_run_id,
            vendor_id="openai",
            started_at=started_at,
            completed_at=None,
            status="running",
            rows_ingested=0,
            error_summary=None,
        )
        self._repo.insert_sync_run(sync_run)

        try:
            reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")))
            usage_events: list[UsageEvent] = []

            for row in reader:
                # OpenAI CSV columns vary — try common field names
                date_val = (
                    row.get("date") or row.get("Date") or
                    row.get("timestamp") or row.get("Timestamp") or ""
                )
                model = (
                    row.get("model") or row.get("Model") or "unknown"
                ).strip()
                input_tokens = _safe_decimal(
                    row.get("input_tokens") or row.get("Input tokens") or
                    row.get("prompt_tokens") or "0"
                ) or Decimal(0)
                output_tokens = _safe_decimal(
                    row.get("output_tokens") or row.get("Output tokens") or
                    row.get("completion_tokens") or "0"
                ) or Decimal(0)
                total_tokens = input_tokens + output_tokens

                event_date = _parse_date(date_val) if date_val else datetime.now(UTC)
                uid = stable_id("openai", "activity", date_val, model, str(total_tokens))

                usage_events.append(UsageEvent(
                    usage_event_id=uid,
                    vendor_id="openai",
                    vendor_account_id=self._vendor_account_id,
                    service_name=model,
                    metric_name="tokens",
                    metric_unit="tokens",
                    quantity=total_tokens,
                    event_start_at=event_date,
                    event_end_at=event_date,
                    source_reference=source_reference,
                    raw_payload_json={
                        "model": model,
                        "input_tokens": str(input_tokens),
                        "output_tokens": str(output_tokens),
                        "raw_row": dict(row),
                    },
                ))

            rows = self._repo.upsert_usage_events(usage_events)
            sync_run.status = "success"
            sync_run.rows_ingested = rows
            sync_run.completed_at = datetime.now(UTC)
            self._repo.update_sync_run(sync_run)
            return sync_run

        except Exception as exc:
            sync_run.status = "failed"
            sync_run.completed_at = datetime.now(UTC)
            sync_run.error_summary = str(exc)[:500]
            self._repo.update_sync_run(sync_run)
            raise

    def sync_cost_csv(
        self,
        csv_bytes: bytes,
        source_reference: str | None = None,
    ) -> SyncRun:
        """
        Import the Cost CSV from platform.openai.com/usage.
        Maps dollar amounts into cost_events.
        """
        started_at = datetime.now(UTC)
        sync_run_id = str(uuid.uuid4())
        sync_run = SyncRun(
            sync_run_id=sync_run_id,
            vendor_id="openai",
            started_at=started_at,
            completed_at=None,
            status="running",
            rows_ingested=0,
            error_summary=None,
        )
        self._repo.insert_sync_run(sync_run)

        try:
            reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")))
            cost_events: list[CostEvent] = []

            for row in reader:
                date_val = (
                    row.get("date") or row.get("Date") or
                    row.get("timestamp") or row.get("Timestamp") or ""
                )
                model = (
                    row.get("model") or row.get("Model") or "unknown"
                ).strip()
                cost = _safe_decimal(
                    row.get("cost") or row.get("Cost") or
                    row.get("amount") or row.get("Amount") or "0"
                ) or Decimal(0)

                event_date = _parse_date(date_val) if date_val else datetime.now(UTC)
                cid = stable_id("openai", "cost", date_val, model, str(cost))

                cost_events.append(CostEvent(
                    cost_event_id=cid,
                    vendor_id="openai",
                    vendor_account_id=self._vendor_account_id,
                    service_name=model,
                    cost_category="api_usage",
                    cost_usd=cost,
                    incurred_at=event_date,
                    source_reference=source_reference,
                    raw_payload_json={"model": model, "raw_row": dict(row)},
                ))

            rows = self._repo.upsert_cost_events(cost_events)
            sync_run.status = "success"
            sync_run.rows_ingested = rows
            sync_run.completed_at = datetime.now(UTC)
            self._repo.update_sync_run(sync_run)
            return sync_run

        except Exception as exc:
            sync_run.status = "failed"
            sync_run.completed_at = datetime.now(UTC)
            sync_run.error_summary = str(exc)[:500]
            self._repo.update_sync_run(sync_run)
            raise
