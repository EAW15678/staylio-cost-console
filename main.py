"""
Staylio Cost Control Console — FastAPI Application

Fixes vs ChatGPT output:
- Static token auth on all endpoints (no open API in production)
- Attribution endpoints added
- Cloudflare R2 connector wired
- Railway failure returns actionable error (field verification instructions)
- Vendor seeding endpoint for setup
- Clean error handling with structured responses
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated, Any

import uvicorn
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, UploadFile

def _resolve_dates(
    start_date: str | None,
    end_date: str | None,
) -> tuple[str | None, str | None]:
    """Validate and return date strings. Both must be provided or neither."""
    if not start_date and not end_date:
        return None, None
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="Both start_date and end_date are required when using date range")
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must be in YYYY-MM-DD format")
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    return start_date, end_date
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from connectors.cloudflare_r2 import CloudflareR2Connector
from repository import PostgresCostConsoleRepository, bootstrap_schema

UTC = timezone.utc


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Staylio Cost Control Console",
    description="Internal FinOps API — not public",
    version="1.0.0",
    docs_url="/docs",         # disable in prod if desired
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("ALLOWED_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Auth — static bearer token (rotate via env var)
# ---------------------------------------------------------------------------

CONSOLE_API_TOKEN = os.environ.get("CONSOLE_API_TOKEN", "")


def require_auth(authorization: str = Header(default="")) -> None:
    if not CONSOLE_API_TOKEN:
        return  # Dev mode: no token set, pass through
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    if token != CONSOLE_API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")


Auth = Annotated[None, Depends(require_auth)]


# ---------------------------------------------------------------------------
# Dependency: repository
# ---------------------------------------------------------------------------

def get_repo() -> PostgresCostConsoleRepository:
    dsn = os.environ.get("COST_CONSOLE_DSN")
    if not dsn:
        raise HTTPException(status_code=503, detail="COST_CONSOLE_DSN not configured")
    return PostgresCostConsoleRepository(dsn)


# ---------------------------------------------------------------------------
# Dependency: connectors
# ---------------------------------------------------------------------------

def get_r2_connector(repo: PostgresCostConsoleRepository = Depends(get_repo)) -> CloudflareR2Connector:
    token = os.environ.get("CF_API_TOKEN")
    account_id = os.environ.get("CF_ACCOUNT_ID")
    if not token or not account_id:
        raise HTTPException(status_code=503, detail="CF_API_TOKEN or CF_ACCOUNT_ID not configured")
    return CloudflareR2Connector(repo=repo, api_token=token, account_id=account_id)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class OperationalEstimateRequest(BaseModel):
    vendor_id: str
    service_name: str | None = None
    model: str | None = None
    estimated_cost_usd: str       # string to avoid float precision issues
    property_id: str | None = None
    workflow_name: str | None = None
    slot_name: str | None = None
    job_id: str | None = None
    environment: str = "production"
    generation_reason: str | None = None
    occurred_at: datetime | None = None
    raw_payload_json: dict[str, Any] = {}


class R2SyncRequest(BaseModel):
    days_back: int = 1


class AnthropicSyncRequest(BaseModel):
    days_back: int = 7


class SummaryResponse(BaseModel):
    today_spend: float
    mtd_spend: float
    today_by_vendor: list[dict[str, Any]]
    mtd_by_vendor: list[dict[str, Any]]
    last_syncs: list[dict[str, Any]]
    failed_syncs_today: int
    workflow_estimates_today: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "staylio-cost-console", "ts": datetime.now(UTC).isoformat()}


@app.post("/admin/bootstrap")
def bootstrap(auth: Auth, repo: PostgresCostConsoleRepository = Depends(get_repo)) -> dict:
    """Create tables and seed vendor rows. Safe to run repeatedly."""
    dsn = os.environ.get("COST_CONSOLE_DSN", "")
    bootstrap_schema(dsn)
    return {"status": "ok", "message": "Schema bootstrapped"}


# --- OpenAI ---

@app.post("/sync/openai/activity")
async def sync_openai_activity(
    auth: Auth,
    file: UploadFile = File(...),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    """Import OpenAI Activity CSV from platform.openai.com/usage."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Must upload a .csv file")
    try:
        from connectors.openai import OpenAICostConnector
        connector = OpenAICostConnector(repo=repo)
        csv_bytes = await file.read()
        sync_run = connector.sync_activity_csv(csv_bytes, source_reference=f"upload/{file.filename}")
        return {"status": "success", "vendor": "openai", "type": "activity",
                "sync_run_id": sync_run.sync_run_id, "rows_ingested": sync_run.rows_ingested}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/sync/openai/cost")
async def sync_openai_cost(
    auth: Auth,
    file: UploadFile = File(...),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    """Import OpenAI Cost CSV from platform.openai.com/usage."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Must upload a .csv file")
    try:
        from connectors.openai import OpenAICostConnector
        connector = OpenAICostConnector(repo=repo)
        csv_bytes = await file.read()
        sync_run = connector.sync_cost_csv(csv_bytes, source_reference=f"upload/{file.filename}")
        return {"status": "success", "vendor": "openai", "type": "cost",
                "sync_run_id": sync_run.sync_run_id, "rows_ingested": sync_run.rows_ingested}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Cloudflare R2 ---

@app.post("/sync/cloudflare_r2/usage")
def sync_r2_usage(
    auth: Auth,
    payload: R2SyncRequest,
    connector: CloudflareR2Connector = Depends(get_r2_connector),
) -> dict:
    """Sync R2 usage from Cloudflare GraphQL analytics API."""
    try:
        sync_run = connector.sync_r2_usage(days_back=payload.days_back)
        return {
            "status": sync_run.status,
            "vendor": "cloudflare_r2",
            "sync_run_id": sync_run.sync_run_id,
            "rows_ingested": sync_run.rows_ingested,
            "error": sync_run.error_summary,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Anthropic ---

@app.post("/sync/anthropic/cost")
def sync_anthropic_cost(
    auth: Auth,
    payload: AnthropicSyncRequest = AnthropicSyncRequest(),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    """Sync cost data from Anthropic Admin API. Requires ANTHROPIC_ADMIN_KEY env var."""
    admin_key = os.environ.get("ANTHROPIC_ADMIN_KEY")
    if not admin_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_ADMIN_KEY not configured")
    try:
        from connectors.anthropic import AnthropicCostConnector
        connector = AnthropicCostConnector(repo=repo, admin_api_key=admin_key)
        sync_run = connector.sync_cost_report(days_back=payload.days_back)
        return {
            "status": sync_run.status,
            "vendor": "anthropic",
            "sync_run_id": sync_run.sync_run_id,
            "rows_ingested": sync_run.rows_ingested,
            "error": sync_run.error_summary,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Railway ---
# NOTE: The Railway connector requires GraphQL field verification before it will work.
# Steps:
#   1. Go to railway.com, open DevTools → Network while on the billing/usage page
#   2. Or query: POST https://backboard.railway.app/graphql/v2 with your token
#   3. Introspect: { __schema { queryType { fields { name } } } }
#   4. Find the correct field name for project usage, then update connectors/railway.py
# Until that's done, this endpoint returns a 503 with instructions.

@app.post("/sync/railway/usage")
def sync_railway_usage(auth: Auth) -> dict:
    token = os.environ.get("RAILWAY_API_TOKEN")
    if not token:
        raise HTTPException(status_code=503, detail="RAILWAY_API_TOKEN not configured")
    raise HTTPException(
        status_code=503,
        detail=(
            "Railway connector requires GraphQL schema verification before use. "
            "See connectors/railway.py for instructions. "
            "The field names in the current connector are provisional and will likely fail."
        ),
    )


# --- Operational estimates (pipeline-emitted) ---

@app.post("/events/estimate")
def create_estimate(
    auth: Auth,
    payload: OperationalEstimateRequest,
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    """
    Generic endpoint for pipeline agents to emit near-real-time cost estimates.
    Use the pipeline_emitter.py helpers instead of calling this directly.
    """
    from repository import CostAttribution, OperationalEstimate, stable_id

    occurred = payload.occurred_at or datetime.now(UTC)
    eid = stable_id(
        payload.vendor_id,
        payload.model or "",
        payload.property_id or "",
        payload.job_id or "",
        occurred.isoformat(),
    )

    est = OperationalEstimate(
        estimate_id=eid,
        vendor_id=payload.vendor_id,
        service_name=payload.service_name,
        model=payload.model,
        estimated_cost_usd=Decimal(payload.estimated_cost_usd),
        property_id=payload.property_id,
        workflow_name=payload.workflow_name,
        slot_name=payload.slot_name,
        job_id=payload.job_id,
        environment=payload.environment,
        generation_reason=payload.generation_reason,
        occurred_at=occurred,
        raw_payload_json=payload.raw_payload_json,
    )
    repo.insert_operational_estimate(est)

    if payload.property_id or payload.job_id:
        attr = CostAttribution(
            attribution_id=stable_id("attr", eid),
            usage_event_id=None,
            cost_event_id=None,
            property_id=payload.property_id,
            workflow_name=payload.workflow_name,
            slot_name=payload.slot_name,
            job_id=payload.job_id,
            environment=payload.environment,
            attribution_method="pipeline_emitted",
            attribution_confidence="high",
            notes=None,
        )
        repo.insert_cost_attribution(attr)

    return {"status": "ok", "estimate_id": eid, "estimated_cost_usd": payload.estimated_cost_usd}


# --- Dashboard ---

@app.get("/metrics/summary", response_model=SummaryResponse)
def get_summary(
    auth: Auth,
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> SummaryResponse:
    try:
        data = repo.get_summary_metrics()
        return SummaryResponse(**data)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/metrics/property/{property_id}")
def get_property_costs(
    property_id: str,
    auth: Auth,
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    """Cost breakdown for a single property."""
    import psycopg
    from psycopg.rows import dict_row

    dsn = os.environ.get("COST_CONSOLE_DSN", "")
    with psycopg.connect(dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_name, slot_name,
                   COALESCE(SUM(estimated_cost_usd),0) as cost,
                   COUNT(*) as events
            FROM cost_console.operational_estimates
            WHERE property_id = %s
            AND occurred_at >= date_trunc('month', CURRENT_DATE)
            GROUP BY workflow_name, slot_name
            ORDER BY cost DESC
            """,
            (property_id,),
        )
        rows = cur.fetchall()

    return {
        "property_id": property_id,
        "mtd_by_workflow": [
            {
                "workflow_name": r["workflow_name"],
                "slot_name": r["slot_name"],
                "cost_usd": round(float(r["cost"]), 4),
                "events": r["events"],
            }
            for r in rows
        ],
        "mtd_total": round(sum(float(r["cost"]) for r in rows), 4),
    }


# --- Phase 3: Reporting endpoints ---

@app.get("/metrics/vendors")
def get_vendor_breakdown(
    auth: Auth,
    period: str = Query("30d", regex="^(24h|7d|30d)$"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    sd, ed = _resolve_dates(start_date, end_date)
    try:
        rows = repo.get_vendor_spend(period, start_date=sd, end_date=ed)
        label = f"{sd} to {ed}" if sd else period
        return {
            "period": label,
            "vendors": [
                {
                    "vendor_id": r["vendor_id"],
                    "vendor_name": r["vendor_name"],
                    "category": r["category"],
                    "cost_usd": round(float(r["total_cost"]), 4),
                    "event_count": r["event_count"],
                }
                for r in rows
            ],
            "total": round(sum(float(r["total_cost"]) for r in rows), 4),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/metrics/category")
def get_category_breakdown(
    auth: Auth,
    period: str = Query("30d", regex="^(24h|7d|30d)$"),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    try:
        rows = repo.get_category_spend(period)
        return {
            "period": period,
            "categories": [
                {
                    "category": r["category"],
                    "cost_usd": round(float(r["total_cost"]), 4),
                    "event_count": r["event_count"],
                    "vendor_count": r["vendor_count"],
                }
                for r in rows
            ],
            "total": round(sum(float(r["total_cost"]) for r in rows), 4),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/metrics/workflows")
def get_workflow_breakdown(
    auth: Auth,
    period: str = Query("30d", regex="^(24h|7d|30d)$"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    sd, ed = _resolve_dates(start_date, end_date)
    try:
        rows = repo.get_workflow_spend(period, start_date=sd, end_date=ed)
        label = f"{sd} to {ed}" if sd else period
        return {
            "period": label,
            "workflows": [
                {
                    "workflow_name": r["workflow_name"],
                    "cost_usd": round(float(r["total_cost"]), 4),
                    "event_count": r["event_count"],
                    "property_count": r["property_count"],
                }
                for r in rows
            ],
            "total": round(sum(float(r["total_cost"]) for r in rows), 4),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/metrics/timeseries")
def get_timeseries(
    auth: Auth,
    period: str = Query("30d", regex="^(24h|7d|30d)$"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    repo: PostgresCostConsoleRepository = Depends(get_repo),
) -> dict:
    sd, ed = _resolve_dates(start_date, end_date)
    try:
        rows = repo.get_timeseries_spend(period, start_date=sd, end_date=ed)
        label = f"{sd} to {ed}" if sd else period
        return {
            "period": label,
            "series": [
                {
                    "date": r["date"].isoformat() if r["date"] else None,
                    "cost_usd": round(float(r["total_cost"]), 4),
                    "event_count": r["event_count"],
                }
                for r in rows
            ],
            "total": round(sum(float(r["total_cost"]) for r in rows), 4),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8001"))
    uvicorn.run("main:app", host=host, port=port, reload=True)
