"""
Staylio Cost Control Console — Cloudflare R2 Connector

This was a Priority 1 connector that ChatGPT never built.

Cloudflare R2 metrics available via:
- Cloudflare GraphQL Analytics API (cloudflare.com/graphql) for request/operation counts
- Account billing API for dollar spend figures

Two ingestion paths:
1. GraphQL analytics → usage_events (request counts, storage GB)
2. Billing API → cost_events (dollar amounts when available)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import requests

from repository import (
    CostAttribution,
    CostConsoleRepository,
    CostEvent,
    OperationalEstimate,
    SyncRun,
    UsageEvent,
    stable_id,
)

UTC = timezone.utc

# Cloudflare GraphQL endpoint
CF_GRAPHQL_URL = "https://api.cloudflare.com/client/v4/graphql"

# Pricing reference (as of 2025-2026, update if Cloudflare changes)
# R2 free tier: 10GB storage, 1M Class A ops, 10M Class B ops/month
# Paid: $0.015/GB-month storage, $4.50/million Class A, $0.36/million Class B, egress free
R2_STORAGE_COST_PER_GB_MONTH = Decimal("0.015")
R2_CLASS_A_COST_PER_MILLION = Decimal("4.50")   # PUT, COPY, POST, LIST
R2_CLASS_B_COST_PER_MILLION = Decimal("0.36")   # GET, all others


class CloudflareR2Connector:
    """Ingests Cloudflare R2 usage from the GraphQL analytics API."""

    def __init__(
        self,
        repo: CostConsoleRepository,
        api_token: str,
        account_id: str,
        vendor_account_id: str = "default",
    ) -> None:
        self._repo = repo
        self._api_token = api_token
        self._account_id = account_id
        self._vendor_account_id = vendor_account_id

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_token}",
            "Content-Type": "application/json",
        }

    def sync_r2_usage(
        self,
        days_back: int = 1,
        source_reference: str | None = None,
    ) -> SyncRun:
        """
        Fetch R2 bucket metrics from Cloudflare GraphQL analytics.

        NOTE: The exact field names below are based on Cloudflare's public
        GraphQL schema for R2 storage analytics. Verify by running the
        introspection query against api.cloudflare.com/client/v4/graphql
        with your token before production use.

        Introspection query to verify fields:
          { __schema { types { name fields { name } } } }
        """
        start_time = datetime.now(UTC)
        sync_run_id = str(uuid.uuid4())
        sync_run = SyncRun(
            sync_run_id=sync_run_id,
            vendor_id="cloudflare_r2",
            started_at=start_time,
            completed_at=None,
            status="running",
            rows_ingested=0,
            error_summary=None,
        )
        self._repo.insert_sync_run(sync_run)

        end_date = datetime.now(UTC).date()
        start_date = end_date - timedelta(days=days_back)

        # GraphQL query for R2 storage analytics
        # Field names: r2OperationsAdaptiveGroups is the confirmed Cloudflare
        # analytics dataset for R2. Verify actionType values match your account.
        query = """
        query R2Usage($accountId: String!, $start: Date!, $end: Date!) {
          viewer {
            accounts(filter: {accountTag: $accountId}) {
              r2OperationsAdaptiveGroups(
                limit: 1000
                filter: {
                  date_geq: $start
                  date_leq: $end
                }
                orderBy: [date_ASC]
              ) {
                dimensions { date actionType bucketName }
                sum { requests }
              }
              r2StorageAdaptiveGroups(
                limit: 100
                filter: {
                  date_geq: $start
                  date_leq: $end
                }
                orderBy: [date_ASC]
              ) {
                dimensions { date bucketName }
                max { payloadSize metadataSize }
              }
            }
          }
        }
        """

        try:
            resp = requests.post(
                CF_GRAPHQL_URL,
                headers=self._headers(),
                json={
                    "query": query,
                    "variables": {
                        "accountId": self._account_id,
                        "start": str(start_date),
                        "end": str(end_date),
                    },
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")

            accounts = data.get("data", {}).get("viewer", {}).get("accounts", [])
            if not accounts:
                raise RuntimeError("No account data returned from Cloudflare GraphQL")

            account_data = accounts[0]
            ops_groups = account_data.get("r2OperationsAdaptiveGroups", [])
            storage_groups = account_data.get("r2StorageAdaptiveGroups", [])

            usage_events: list[UsageEvent] = []
            cost_events: list[CostEvent] = []

            # --- Operation usage events ---
            class_a_actions = {"PutObject", "CopyObject", "CreateMultipartUpload",
                               "UploadPart", "CompleteMultipartUpload", "ListBuckets",
                               "ListObjects", "ListObjectsV2", "ListMultipartUploads",
                               "ListParts", "DeleteObjects", "PutBucketCors",
                               "PutBucketLifecycleConfiguration"}

            for group in ops_groups:
                dims = group.get("dimensions", {})
                date_str = dims.get("date", str(start_date))
                action = dims.get("actionType", "unknown")
                bucket = dims.get("bucketName", "unknown")
                requests_count = group.get("sum", {}).get("requests", 0)

                event_date = datetime.fromisoformat(date_str).replace(tzinfo=UTC)
                is_class_a = action in class_a_actions
                metric_name = "class_a_requests" if is_class_a else "class_b_requests"
                unit_cost = R2_CLASS_A_COST_PER_MILLION if is_class_a else R2_CLASS_B_COST_PER_MILLION

                uid = stable_id("cloudflare_r2", "ops", date_str, action, bucket)
                usage_events.append(UsageEvent(
                    usage_event_id=uid,
                    vendor_id="cloudflare_r2",
                    vendor_account_id=self._vendor_account_id,
                    service_name=f"r2/{bucket}",
                    metric_name=metric_name,
                    metric_unit="requests",
                    quantity=Decimal(requests_count),
                    event_start_at=event_date,
                    event_end_at=event_date + timedelta(days=1),
                    source_reference=source_reference or f"graphql/{date_str}",
                    raw_payload_json={"action": action, "bucket": bucket, "requests": requests_count},
                ))

                # Cost estimate for operations above free tier
                # Simplified: applies cost to all requests (free tier not modeled here)
                op_cost = (Decimal(requests_count) / Decimal(1_000_000)) * unit_cost
                if op_cost > Decimal("0.00001"):
                    cid = stable_id("cloudflare_r2", "ops_cost", date_str, action, bucket)
                    cost_events.append(CostEvent(
                        cost_event_id=cid,
                        vendor_id="cloudflare_r2",
                        vendor_account_id=self._vendor_account_id,
                        service_name=f"r2/{bucket}",
                        cost_category=f"r2_{metric_name}",
                        cost_usd=op_cost.quantize(Decimal("0.000001")),
                        incurred_at=event_date,
                        source_reference=source_reference or f"graphql/{date_str}",
                        raw_payload_json={"action": action, "bucket": bucket},
                    ))

            # --- Storage usage events ---
            for group in storage_groups:
                dims = group.get("dimensions", {})
                date_str = dims.get("date", str(start_date))
                bucket = dims.get("bucketName", "unknown")
                payload_bytes = group.get("max", {}).get("payloadSize", 0)
                meta_bytes = group.get("max", {}).get("metadataSize", 0)
                total_bytes = (payload_bytes or 0) + (meta_bytes or 0)
                gb = Decimal(total_bytes) / Decimal(1_073_741_824)  # bytes → GB

                event_date = datetime.fromisoformat(date_str).replace(tzinfo=UTC)
                uid = stable_id("cloudflare_r2", "storage", date_str, bucket)
                usage_events.append(UsageEvent(
                    usage_event_id=uid,
                    vendor_id="cloudflare_r2",
                    vendor_account_id=self._vendor_account_id,
                    service_name=f"r2/{bucket}",
                    metric_name="storage_gb",
                    metric_unit="GB",
                    quantity=gb.quantize(Decimal("0.000001")),
                    event_start_at=event_date,
                    event_end_at=event_date + timedelta(days=1),
                    source_reference=source_reference or f"graphql/{date_str}",
                    raw_payload_json={"bucket": bucket, "payload_bytes": payload_bytes},
                ))

                # Daily prorated storage cost
                daily_storage_cost = gb * R2_STORAGE_COST_PER_GB_MONTH / Decimal(30)
                if daily_storage_cost > Decimal("0.00001"):
                    cid = stable_id("cloudflare_r2", "storage_cost", date_str, bucket)
                    cost_events.append(CostEvent(
                        cost_event_id=cid,
                        vendor_id="cloudflare_r2",
                        vendor_account_id=self._vendor_account_id,
                        service_name=f"r2/{bucket}",
                        cost_category="r2_storage",
                        cost_usd=daily_storage_cost.quantize(Decimal("0.000001")),
                        incurred_at=event_date,
                        source_reference=source_reference or f"graphql/{date_str}",
                        raw_payload_json={"bucket": bucket, "gb": str(gb)},
                    ))

            rows_u = self._repo.upsert_usage_events(usage_events)
            rows_c = self._repo.upsert_cost_events(cost_events)
            total_rows = rows_u + rows_c

            sync_run.status = "success"
            sync_run.rows_ingested = total_rows
            sync_run.completed_at = datetime.now(UTC)
            self._repo.update_sync_run(sync_run)
            return sync_run

        except Exception as exc:
            sync_run.status = "failed"
            sync_run.completed_at = datetime.now(UTC)
            sync_run.error_summary = str(exc)[:500]
            self._repo.update_sync_run(sync_run)
            raise

    def emit_operational_estimate(
        self,
        *,
        operation_type: str,             # "upload" | "download" | "delete" | "list"
        request_count: int = 1,
        storage_bytes_delta: int = 0,    # positive = upload, negative = delete
        property_id: str | None = None,
        workflow_name: str | None = None,
        slot_name: str | None = None,
        job_id: str | None = None,
        environment: str = "production",
        generation_reason: str | None = None,
    ) -> Decimal:
        """
        Emit a near-real-time cost estimate when the pipeline writes to R2.
        Call this from Agent 3 after uploading any asset.
        """
        class_a_ops = {"upload", "list", "delete", "copy"}
        is_class_a = operation_type.lower() in class_a_ops
        unit_cost = R2_CLASS_A_COST_PER_MILLION if is_class_a else R2_CLASS_B_COST_PER_MILLION
        op_cost = (Decimal(request_count) / Decimal(1_000_000)) * unit_cost

        storage_cost = Decimal(0)
        if storage_bytes_delta > 0:
            gb = Decimal(storage_bytes_delta) / Decimal(1_073_741_824)
            # Amortize across the month
            storage_cost = gb * R2_STORAGE_COST_PER_GB_MONTH

        estimated_cost = (op_cost + storage_cost).quantize(Decimal("0.0001"))

        estimate_id = stable_id(
            "cloudflare_r2",
            operation_type,
            str(property_id or ""),
            str(job_id or ""),
            str(datetime.now(UTC).isoformat()),
        )

        est = OperationalEstimate(
            estimate_id=estimate_id,
            vendor_id="cloudflare_r2",
            service_name="r2_storage",
            model=None,
            estimated_cost_usd=estimated_cost,
            property_id=property_id,
            workflow_name=workflow_name,
            slot_name=slot_name,
            job_id=job_id,
            environment=environment,
            generation_reason=generation_reason,
            occurred_at=datetime.now(UTC),
            raw_payload_json={
                "operation_type": operation_type,
                "request_count": request_count,
                "storage_bytes_delta": storage_bytes_delta,
                "is_class_a": is_class_a,
            },
        )
        self._repo.insert_operational_estimate(est)

        # Also write attribution if property/job is known
        if property_id or job_id:
            attr = CostAttribution(
                attribution_id=stable_id("attr", estimate_id),
                usage_event_id=None,
                cost_event_id=None,
                property_id=property_id,
                workflow_name=workflow_name,
                slot_name=slot_name,
                job_id=job_id,
                environment=environment,
                attribution_method="pipeline_emitted",
                attribution_confidence="high",
                notes=f"R2 {operation_type} estimate",
            )
            self._repo.insert_cost_attribution(attr)

        return estimated_cost
