"""
Staylio Cost Control Console — Repository Layer
Postgres/Supabase-backed. All tables from the approved data model.

All SQL references use explicit cost_console.<table> qualification.
Never rely on search_path — pooled connections do not inherit role-level settings.

Table name mapping:
  cost_console.vendors
  cost_console.vendor_accounts
  cost_console.usage_events          (was incorrectly 'cost_usage_events' in prior version)
  cost_console.cost_events
  cost_console.cost_attribution
  cost_console.operational_estimates
  cost_console.sync_runs
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Generator, Protocol

import psycopg
from psycopg.rows import dict_row

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

@dataclass
class UsageEvent:
    usage_event_id: str
    vendor_id: str
    vendor_account_id: str | None
    service_name: str | None
    metric_name: str | None
    metric_unit: str | None
    quantity: Decimal | None
    event_start_at: datetime | None
    event_end_at: datetime | None
    source_reference: str | None
    raw_payload_json: dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class CostEvent:
    cost_event_id: str
    vendor_id: str
    vendor_account_id: str | None
    service_name: str | None
    cost_category: str | None
    cost_usd: Decimal | None
    incurred_at: datetime | None
    source_reference: str | None
    raw_payload_json: dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class CostAttribution:
    attribution_id: str
    usage_event_id: str | None
    cost_event_id: str | None
    property_id: str | None
    workflow_name: str | None
    slot_name: str | None
    job_id: str | None
    environment: str
    attribution_method: str          # "pipeline_emitted" | "manual" | "vendor_api"
    attribution_confidence: str      # "high" | "medium" | "low"
    notes: str | None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class SyncRun:
    sync_run_id: str
    vendor_id: str
    started_at: datetime
    completed_at: datetime | None
    status: str                       # "success" | "failed" | "running"
    rows_ingested: int
    error_summary: str | None


@dataclass
class OperationalEstimate:
    estimate_id: str
    vendor_id: str
    service_name: str | None
    model: str | None
    estimated_cost_usd: Decimal
    property_id: str | None
    workflow_name: str | None
    slot_name: str | None
    job_id: str | None
    environment: str
    generation_reason: str | None
    occurred_at: datetime
    raw_payload_json: dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# Repository protocol
# ---------------------------------------------------------------------------

class CostConsoleRepository(Protocol):
    def upsert_usage_events(self, events: list[UsageEvent]) -> int: ...
    def upsert_cost_events(self, events: list[CostEvent]) -> int: ...
    def insert_cost_attribution(self, attr: CostAttribution) -> None: ...
    def insert_operational_estimate(self, est: OperationalEstimate) -> None: ...
    def insert_sync_run(self, sync_run: SyncRun) -> None: ...
    def update_sync_run(self, sync_run: SyncRun) -> None: ...
    def get_summary_metrics(self) -> dict[str, Any]: ...


# ---------------------------------------------------------------------------
# Postgres implementation
# ---------------------------------------------------------------------------

class PostgresCostConsoleRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @contextmanager
    def _conn(self) -> Generator[psycopg.Connection, None, None]:
        with psycopg.connect(self._dsn, row_factory=dict_row) as conn:
            yield conn

    # ------------------------------------------------------------------
    # Usage events — idempotent via usage_event_id ON CONFLICT DO NOTHING
    # ------------------------------------------------------------------

    def upsert_usage_events(self, events: list[UsageEvent]) -> int:
        if not events:
            return 0
        inserted = 0
        with self._conn() as conn, conn.cursor() as cur:
            for e in events:
                cur.execute(
                    """
                    INSERT INTO cost_console.usage_events (
                        usage_event_id, vendor_id, vendor_account_id,
                        service_name, metric_name, metric_unit, quantity,
                        event_start_at, event_end_at, source_reference,
                        raw_payload_json
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    ) ON CONFLICT (usage_event_id) DO NOTHING
                    """,
                    (
                        e.usage_event_id,
                        e.vendor_id,
                        e.vendor_account_id,
                        e.service_name,
                        e.metric_name,
                        e.metric_unit,
                        str(e.quantity) if e.quantity is not None else None,
                        e.event_start_at,
                        e.event_end_at,
                        e.source_reference,
                        json.dumps(e.raw_payload_json),
                    ),
                )
                inserted += cur.rowcount
        return inserted

    # ------------------------------------------------------------------
    # Cost events — idempotent via cost_event_id ON CONFLICT DO NOTHING
    # ------------------------------------------------------------------

    def upsert_cost_events(self, events: list[CostEvent]) -> int:
        if not events:
            return 0
        inserted = 0
        with self._conn() as conn, conn.cursor() as cur:
            for e in events:
                cur.execute(
                    """
                    INSERT INTO cost_console.cost_events (
                        cost_event_id, vendor_id, vendor_account_id,
                        service_name, cost_category, cost_usd,
                        incurred_at, source_reference, raw_payload_json
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s
                    ) ON CONFLICT (cost_event_id) DO NOTHING
                    """,
                    (
                        e.cost_event_id,
                        e.vendor_id,
                        e.vendor_account_id,
                        e.service_name,
                        e.cost_category,
                        str(e.cost_usd) if e.cost_usd is not None else None,
                        e.incurred_at,
                        e.source_reference,
                        json.dumps(e.raw_payload_json),
                    ),
                )
                inserted += cur.rowcount
        return inserted

    # ------------------------------------------------------------------
    # Cost attribution
    # ------------------------------------------------------------------

    def insert_cost_attribution(self, attr: CostAttribution) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cost_console.cost_attribution (
                    attribution_id, usage_event_id, cost_event_id,
                    property_id, workflow_name, slot_name, job_id,
                    environment, attribution_method, attribution_confidence, notes
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (attribution_id) DO NOTHING
                """,
                (
                    attr.attribution_id,
                    attr.usage_event_id,
                    attr.cost_event_id,
                    attr.property_id,
                    attr.workflow_name,
                    attr.slot_name,
                    attr.job_id,
                    attr.environment,
                    attr.attribution_method,
                    attr.attribution_confidence,
                    attr.notes,
                ),
            )

    # ------------------------------------------------------------------
    # Operational estimates (pipeline-emitted)
    # ------------------------------------------------------------------

    def insert_operational_estimate(self, est: OperationalEstimate) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cost_console.operational_estimates (
                    estimate_id, vendor_id, service_name, model,
                    estimated_cost_usd, property_id, workflow_name,
                    slot_name, job_id, environment, generation_reason,
                    occurred_at, raw_payload_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (estimate_id) DO NOTHING
                """,
                (
                    est.estimate_id,
                    est.vendor_id,
                    est.service_name,
                    est.model,
                    str(est.estimated_cost_usd),
                    est.property_id,
                    est.workflow_name,
                    est.slot_name,
                    est.job_id,
                    est.environment,
                    est.generation_reason,
                    est.occurred_at,
                    json.dumps(est.raw_payload_json),
                ),
            )

    # ------------------------------------------------------------------
    # Sync runs
    # ------------------------------------------------------------------

    def insert_sync_run(self, sync_run: SyncRun) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cost_console.sync_runs (
                    sync_run_id, vendor_id, started_at, completed_at,
                    status, rows_ingested, error_summary
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    sync_run.sync_run_id,
                    sync_run.vendor_id,
                    sync_run.started_at,
                    sync_run.completed_at,
                    sync_run.status,
                    sync_run.rows_ingested,
                    sync_run.error_summary,
                ),
            )

    def update_sync_run(self, sync_run: SyncRun) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cost_console.sync_runs
                SET completed_at=%s, status=%s, rows_ingested=%s, error_summary=%s
                WHERE sync_run_id=%s
                """,
                (
                    sync_run.completed_at,
                    sync_run.status,
                    sync_run.rows_ingested,
                    sync_run.error_summary,
                    sync_run.sync_run_id,
                ),
            )

    # ------------------------------------------------------------------
    # Dashboard query
    # ------------------------------------------------------------------

    def get_summary_metrics(self) -> dict[str, Any]:
        with self._conn() as conn, conn.cursor() as cur:
            # Vendor spend today
            cur.execute(
                """
                SELECT vendor_id, COALESCE(SUM(cost_usd),0) as cost_usd
                FROM cost_console.cost_events
                WHERE incurred_at >= CURRENT_DATE
                GROUP BY vendor_id
                ORDER BY cost_usd DESC
                """
            )
            today_by_vendor = cur.fetchall()

            # Vendor spend MTD
            cur.execute(
                """
                SELECT vendor_id, COALESCE(SUM(cost_usd),0) as cost_usd
                FROM cost_console.cost_events
                WHERE incurred_at >= date_trunc('month', CURRENT_DATE)
                GROUP BY vendor_id
                ORDER BY cost_usd DESC
                """
            )
            mtd_by_vendor = cur.fetchall()

            today_total = sum(float(r["cost_usd"]) for r in today_by_vendor)
            mtd_total = sum(float(r["cost_usd"]) for r in mtd_by_vendor)

            # Sync health
            cur.execute(
                """
                SELECT vendor_id, status, completed_at
                FROM cost_console.sync_runs
                WHERE sync_run_id IN (
                    SELECT DISTINCT ON (vendor_id) sync_run_id
                    FROM cost_console.sync_runs
                    ORDER BY vendor_id, started_at DESC
                )
                """
            )
            last_syncs = cur.fetchall()

            # Recent failed syncs (last 24h)
            cur.execute(
                """
                SELECT COUNT(*) as n
                FROM cost_console.sync_runs
                WHERE status='failed'
                AND started_at > NOW() - INTERVAL '24 hours'
                """
            )
            failed_row = cur.fetchone()
            failed_today = int(failed_row["n"]) if failed_row else 0

            # Operational estimates today
            cur.execute(
                """
                SELECT
                    property_id,
                    workflow_name,
                    COALESCE(SUM(estimated_cost_usd),0) as est_cost
                FROM cost_console.operational_estimates
                WHERE occurred_at >= CURRENT_DATE
                GROUP BY property_id, workflow_name
                ORDER BY est_cost DESC
                LIMIT 20
                """
            )
            workflow_estimates = cur.fetchall()

            return {
                "today_spend": round(today_total, 2),
                "mtd_spend": round(mtd_total, 2),
                "today_by_vendor": [
                    {"vendor_id": r["vendor_id"], "cost_usd": round(float(r["cost_usd"]), 2)}
                    for r in today_by_vendor
                ],
                "mtd_by_vendor": [
                    {"vendor_id": r["vendor_id"], "cost_usd": round(float(r["cost_usd"]), 2)}
                    for r in mtd_by_vendor
                ],
                "last_syncs": [
                    {
                        "vendor_id": r["vendor_id"],
                        "status": r["status"],
                        "completed_at": r["completed_at"].isoformat() if r.get("completed_at") else None,
                    }
                    for r in last_syncs
                ],
                "failed_syncs_today": failed_today,
                "workflow_estimates_today": [
                    {
                        "property_id": r["property_id"],
                        "workflow_name": r["workflow_name"],
                        "estimated_cost_usd": round(float(r["est_cost"]), 4),
                    }
                    for r in workflow_estimates
                ],
            }


# ---------------------------------------------------------------------------
# Stable ID helper (deterministic UUID from content)
# ---------------------------------------------------------------------------

def stable_id(*parts: str) -> str:
    """Deterministic UUID-shaped ID from a set of identifying strings.
    Used to make upserts idempotent — same data always produces same ID."""
    h = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


# ---------------------------------------------------------------------------
# Bootstrap SQL — schema-qualified, idempotent
# ---------------------------------------------------------------------------

BOOTSTRAP_SQL = [
    # vendors
    """
    CREATE TABLE IF NOT EXISTS cost_console.vendors (
        vendor_id TEXT PRIMARY KEY,
        vendor_name TEXT NOT NULL,
        category TEXT,
        billing_model TEXT,
        is_active BOOLEAN DEFAULT TRUE
    )
    """,
    # vendor_accounts
    """
    CREATE TABLE IF NOT EXISTS cost_console.vendor_accounts (
        vendor_account_id TEXT PRIMARY KEY,
        vendor_id TEXT NOT NULL REFERENCES cost_console.vendors(vendor_id),
        account_name TEXT,
        account_reference TEXT,
        environment TEXT NOT NULL DEFAULT 'production',
        credentials_ref TEXT,
        is_active BOOLEAN DEFAULT TRUE
    )
    """,
    # usage_events
    """
    CREATE TABLE IF NOT EXISTS cost_console.usage_events (
        usage_event_id TEXT PRIMARY KEY,
        vendor_id TEXT NOT NULL REFERENCES cost_console.vendors(vendor_id),
        vendor_account_id TEXT,
        service_name TEXT,
        metric_name TEXT,
        metric_unit TEXT,
        quantity NUMERIC,
        event_start_at TIMESTAMPTZ,
        event_end_at TIMESTAMPTZ,
        source_reference TEXT,
        raw_payload_json JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_usage_vendor_time
       ON cost_console.usage_events(vendor_id, event_start_at)""",
    # cost_events
    """
    CREATE TABLE IF NOT EXISTS cost_console.cost_events (
        cost_event_id TEXT PRIMARY KEY,
        vendor_id TEXT NOT NULL REFERENCES cost_console.vendors(vendor_id),
        vendor_account_id TEXT,
        service_name TEXT,
        cost_category TEXT,
        cost_usd NUMERIC,
        incurred_at TIMESTAMPTZ,
        source_reference TEXT,
        raw_payload_json JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_cost_vendor_time
       ON cost_console.cost_events(vendor_id, incurred_at)""",
    # cost_attribution
    """
    CREATE TABLE IF NOT EXISTS cost_console.cost_attribution (
        attribution_id TEXT PRIMARY KEY,
        usage_event_id TEXT REFERENCES cost_console.usage_events(usage_event_id),
        cost_event_id TEXT REFERENCES cost_console.cost_events(cost_event_id),
        property_id TEXT,
        workflow_name TEXT,
        slot_name TEXT,
        job_id TEXT,
        environment TEXT NOT NULL DEFAULT 'production',
        attribution_method TEXT NOT NULL,
        attribution_confidence TEXT NOT NULL DEFAULT 'high',
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_attr_property
       ON cost_console.cost_attribution(property_id, workflow_name)""",
    """CREATE INDEX IF NOT EXISTS idx_attr_job
       ON cost_console.cost_attribution(job_id)""",
    # operational_estimates
    """
    CREATE TABLE IF NOT EXISTS cost_console.operational_estimates (
        estimate_id TEXT PRIMARY KEY,
        vendor_id TEXT NOT NULL,
        service_name TEXT,
        model TEXT,
        estimated_cost_usd NUMERIC NOT NULL,
        property_id TEXT,
        workflow_name TEXT,
        slot_name TEXT,
        job_id TEXT,
        environment TEXT NOT NULL DEFAULT 'production',
        generation_reason TEXT,
        occurred_at TIMESTAMPTZ NOT NULL,
        raw_payload_json JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_est_property_time
       ON cost_console.operational_estimates(property_id, occurred_at)""",
    # sync_runs
    """
    CREATE TABLE IF NOT EXISTS cost_console.sync_runs (
        sync_run_id TEXT PRIMARY KEY,
        vendor_id TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        status TEXT NOT NULL DEFAULT 'running',
        rows_ingested INT NOT NULL DEFAULT 0,
        error_summary TEXT
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_sync_vendor_time
       ON cost_console.sync_runs(vendor_id, started_at DESC)""",
    # --- Seed vendors ---
    """
    INSERT INTO cost_console.vendors (vendor_id, vendor_name, category, billing_model) VALUES
        ('anthropic',     'Anthropic',      'llm',      'usage'),
        ('openai',        'OpenAI',         'llm',      'usage'),
        ('railway',       'Railway',        'infra',    'usage'),
        ('cloudflare_r2', 'Cloudflare R2',  'storage',  'usage'),
        ('elevenlabs',    'ElevenLabs',     'audio',    'usage'),
        ('creatomate',    'Creatomate',     'video',    'usage'),
        ('claid',         'Claid',          'image',    'usage'),
        ('runway',        'Runway',         'video',    'usage'),
        ('supabase',      'Supabase',       'database', 'usage')
    ON CONFLICT (vendor_id) DO NOTHING
    """,
]


def bootstrap_schema(dsn: str) -> None:
    """Create all tables and seed vendor rows. Safe to run repeatedly."""
    repo = PostgresCostConsoleRepository(dsn)
    with repo._conn() as conn, conn.cursor() as cur:
        for stmt in BOOTSTRAP_SQL:
            cur.execute(stmt)
    print("Cost console schema bootstrapped.")


if __name__ == "__main__":
    import os
    dsn = os.environ.get("COST_CONSOLE_DSN")
    if not dsn:
        raise SystemExit("Set COST_CONSOLE_DSN first.")
    bootstrap_schema(dsn)
