"""
Staylio Cost Control Console — Pipeline Event Emitter

This is the missing link between the Staylio pipeline and the cost console.
Call these functions from Agents 1, 3, and 5 to emit real-time cost estimates
so you get property/workflow/job-level attribution without waiting for vendor syncs.

USAGE IN AGENTS:

    from cost_console.pipeline_emitter import emit_llm_cost, emit_media_cost, emit_storage_cost

    # In Agent 2 (content generation):
    emit_llm_cost(
        vendor="openai",
        model="gpt-4o",
        input_tokens=4200,
        output_tokens=800,
        property_id=property_id,
        workflow_name="page_build",
        job_id=job_id,
    )

    # In Agent 3 (after ElevenLabs call):
    emit_media_cost(
        vendor="elevenlabs",
        service="tts",
        units=1250,       # characters
        unit_name="characters",
        property_id=property_id,
        workflow_name="guest_review_audio_generation",
        slot_name="guest_review_audio_1",
        job_id=job_id,
    )

    # In Agent 3 (after R2 upload):
    emit_storage_cost(
        vendor="cloudflare_r2",
        operation="upload",
        bytes_transferred=file_size_bytes,
        property_id=property_id,
        workflow_name="hero_video_assembly",
        slot_name="hero_video",
        job_id=job_id,
    )
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from repository import (
    CostAttribution,
    CostConsoleRepository,
    OperationalEstimate,
    PostgresCostConsoleRepository,
    stable_id,
)

UTC = timezone.utc

# ---------------------------------------------------------------------------
# Pricing table — update when vendors change rates
# ---------------------------------------------------------------------------

# LLM: cost per 1M tokens (input, output)
LLM_PRICING: dict[str, tuple[Decimal, Decimal]] = {
    "gpt-4o":              (Decimal("2.50"),  Decimal("10.00")),
    "gpt-4o-mini":         (Decimal("0.15"),  Decimal("0.60")),
    "claude-sonnet-4-20250514": (Decimal("3.00"),  Decimal("15.00")),
    "claude-haiku-4-5-20251001": (Decimal("0.80"),  Decimal("4.00")),
}

# ElevenLabs: cost per 1000 characters
ELEVENLABS_PRICING: dict[str, Decimal] = {
    "default": Decimal("0.18"),  # ~$0.18 per 1000 chars (Creator plan estimate)
}

# Creatomate: cost per render (estimate — verify against your plan)
CREATOMATE_PRICING: dict[str, Decimal] = {
    "video_render":  Decimal("0.10"),  # per render, varies by duration
    "default":       Decimal("0.05"),
}

# Claid: cost per image (estimate — verify against your plan)
CLAID_PRICING: dict[str, Decimal] = {
    "enhance":     Decimal("0.012"),
    "upscale":     Decimal("0.015"),
    "default":     Decimal("0.010"),
}

# Runway: cost per second of video generated
RUNWAY_PRICING: dict[str, Decimal] = {
    "gen3_5s":     Decimal("0.25"),   # ~$0.05/s for 5s clip
    "default":     Decimal("0.05"),   # per second
}

# Google Cloud Vision: per 1000 requests
GCV_PRICING: dict[str, Decimal] = {
    "label_detection": Decimal("1.50"),
    "default":         Decimal("1.50"),
}

# Cloudflare R2
R2_CLASS_A_PER_MILLION = Decimal("4.50")
R2_CLASS_B_PER_MILLION = Decimal("0.36")
R2_STORAGE_PER_GB_MONTH = Decimal("0.015")


# ---------------------------------------------------------------------------
# Lazy repo initializer — avoids import-time DB connection
# ---------------------------------------------------------------------------

_repo: CostConsoleRepository | None = None


def _get_repo() -> CostConsoleRepository:
    global _repo
    if _repo is None:
        dsn = os.environ.get("COST_CONSOLE_DSN")
        if not dsn:
            raise RuntimeError(
                "COST_CONSOLE_DSN not set — cost events will not be emitted. "
                "Set this env var to enable cost tracking."
            )
        _repo = PostgresCostConsoleRepository(dsn)
    return _repo


def _safe_emit(est: OperationalEstimate, attr: CostAttribution | None = None) -> None:
    """Write to cost console but never crash the pipeline if it fails."""
    try:
        repo = _get_repo()
        repo.insert_operational_estimate(est)
        if attr:
            repo.insert_cost_attribution(attr)
    except Exception as exc:
        # Log but do not raise — cost tracking must never block the pipeline
        print(f"[cost_emitter] WARNING: failed to emit cost event: {exc}")


# ---------------------------------------------------------------------------
# Public emitters — call these from pipeline agents
# ---------------------------------------------------------------------------

def emit_llm_cost(
    *,
    vendor: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    property_id: str | None = None,
    workflow_name: str | None = None,
    slot_name: str | None = None,
    job_id: str | None = None,
    environment: str = "production",
    generation_reason: str | None = None,
) -> Decimal:
    """
    Emit a cost estimate after any LLM call.
    Call from Agent 1 (enrichment prompts), Agent 2 (content gen), Agent 5 (page build).
    """
    input_price, output_price = LLM_PRICING.get(model, (Decimal("3.00"), Decimal("15.00")))
    cost = (
        Decimal(input_tokens) / Decimal(1_000_000) * input_price
        + Decimal(output_tokens) / Decimal(1_000_000) * output_price
    ).quantize(Decimal("0.000001"))

    now = datetime.now(UTC)
    eid = stable_id(vendor, model, str(property_id or ""), str(job_id or ""), now.isoformat())

    est = OperationalEstimate(
        estimate_id=eid,
        vendor_id=vendor,
        service_name="chat_completions",
        model=model,
        estimated_cost_usd=cost,
        property_id=property_id,
        workflow_name=workflow_name,
        slot_name=slot_name,
        job_id=job_id,
        environment=environment,
        generation_reason=generation_reason,
        occurred_at=now,
        raw_payload_json={
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    )

    attr = CostAttribution(
        attribution_id=stable_id("attr", eid),
        usage_event_id=None,
        cost_event_id=None,
        property_id=property_id,
        workflow_name=workflow_name,
        slot_name=slot_name,
        job_id=job_id,
        environment=environment,
        attribution_method="pipeline_emitted",
        attribution_confidence="high",
        notes=f"{vendor}/{model} call",
    ) if (property_id or job_id) else None

    _safe_emit(est, attr)
    return cost


def emit_media_cost(
    *,
    vendor: str,
    service: str,
    units: int | float,
    unit_name: str,
    property_id: str | None = None,
    workflow_name: str | None = None,
    slot_name: str | None = None,
    job_id: str | None = None,
    environment: str = "production",
    generation_reason: str | None = None,
) -> Decimal:
    """
    Emit a cost estimate after ElevenLabs, Creatomate, Claid, or Runway calls.
    Call from Agent 3 after each external API call.

    Examples:
        emit_media_cost(vendor="elevenlabs", service="tts", units=1250,
                        unit_name="characters", ...)

        emit_media_cost(vendor="creatomate", service="video_render", units=1,
                        unit_name="renders", ...)

        emit_media_cost(vendor="claid", service="enhance", units=1,
                        unit_name="images", ...)

        emit_media_cost(vendor="runway", service="gen3_5s", units=5,
                        unit_name="seconds", ...)
    """
    # Look up unit cost from pricing tables
    if vendor == "elevenlabs":
        price_per_1k = ELEVENLABS_PRICING.get(service, ELEVENLABS_PRICING["default"])
        cost = (Decimal(str(units)) / Decimal(1000)) * price_per_1k
    elif vendor == "creatomate":
        price_per_unit = CREATOMATE_PRICING.get(service, CREATOMATE_PRICING["default"])
        cost = Decimal(str(units)) * price_per_unit
    elif vendor == "claid":
        price_per_unit = CLAID_PRICING.get(service, CLAID_PRICING["default"])
        cost = Decimal(str(units)) * price_per_unit
    elif vendor == "runway":
        price_per_unit = RUNWAY_PRICING.get(service, RUNWAY_PRICING["default"])
        cost = Decimal(str(units)) * price_per_unit
    elif vendor in ("google_vision", "gcv"):
        price_per_1k = GCV_PRICING.get(service, GCV_PRICING["default"])
        cost = (Decimal(str(units)) / Decimal(1000)) * price_per_1k
    else:
        cost = Decimal("0")

    cost = cost.quantize(Decimal("0.000001"))
    now = datetime.now(UTC)
    eid = stable_id(vendor, service, str(property_id or ""), str(job_id or ""), now.isoformat())

    est = OperationalEstimate(
        estimate_id=eid,
        vendor_id=vendor,
        service_name=service,
        model=service,
        estimated_cost_usd=cost,
        property_id=property_id,
        workflow_name=workflow_name,
        slot_name=slot_name,
        job_id=job_id,
        environment=environment,
        generation_reason=generation_reason,
        occurred_at=now,
        raw_payload_json={"service": service, "units": units, "unit_name": unit_name},
    )

    attr = CostAttribution(
        attribution_id=stable_id("attr", eid),
        usage_event_id=None,
        cost_event_id=None,
        property_id=property_id,
        workflow_name=workflow_name,
        slot_name=slot_name,
        job_id=job_id,
        environment=environment,
        attribution_method="pipeline_emitted",
        attribution_confidence="high",
        notes=f"{vendor}/{service} call — {units} {unit_name}",
    ) if (property_id or job_id) else None

    _safe_emit(est, attr)
    return cost


def emit_storage_cost(
    *,
    vendor: str,
    operation: str,                     # "upload" | "download" | "delete" | "list"
    bytes_transferred: int = 0,
    request_count: int = 1,
    property_id: str | None = None,
    workflow_name: str | None = None,
    slot_name: str | None = None,
    job_id: str | None = None,
    environment: str = "production",
    generation_reason: str | None = None,
) -> Decimal:
    """
    Emit a cost estimate for R2 storage operations.
    Call from Agent 3 after every upload to Cloudflare R2.
    """
    class_a_ops = {"upload", "list", "delete", "copy", "put"}
    is_class_a = operation.lower() in class_a_ops
    unit_cost = R2_CLASS_A_PER_MILLION if is_class_a else R2_CLASS_B_PER_MILLION
    op_cost = (Decimal(request_count) / Decimal(1_000_000)) * unit_cost

    storage_cost = Decimal(0)
    if bytes_transferred > 0:
        gb = Decimal(bytes_transferred) / Decimal(1_073_741_824)
        storage_cost = gb * R2_STORAGE_PER_GB_MONTH

    cost = (op_cost + storage_cost).quantize(Decimal("0.000001"))
    now = datetime.now(UTC)
    eid = stable_id(vendor, operation, str(property_id or ""), str(job_id or ""), now.isoformat())

    est = OperationalEstimate(
        estimate_id=eid,
        vendor_id=vendor,
        service_name=f"r2_{operation}",
        model=None,
        estimated_cost_usd=cost,
        property_id=property_id,
        workflow_name=workflow_name,
        slot_name=slot_name,
        job_id=job_id,
        environment=environment,
        generation_reason=generation_reason,
        occurred_at=now,
        raw_payload_json={
            "operation": operation,
            "bytes_transferred": bytes_transferred,
            "request_count": request_count,
        },
    )

    attr = CostAttribution(
        attribution_id=stable_id("attr", eid),
        usage_event_id=None,
        cost_event_id=None,
        property_id=property_id,
        workflow_name=workflow_name,
        slot_name=slot_name,
        job_id=job_id,
        environment=environment,
        attribution_method="pipeline_emitted",
        attribution_confidence="high",
        notes=f"R2 {operation}",
    ) if (property_id or job_id) else None

    _safe_emit(est, attr)
    return cost
