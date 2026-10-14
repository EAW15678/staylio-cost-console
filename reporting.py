"""
Staylio Cost Console — read-only reporting over the pipeline's cost ledger.

Source of truth: public.cost_events, written by the substrate pipeline for every
vendor charge (skills/contract.emit_cost). Nothing here writes. Every connection
is opened read-only with a statement timeout, every query is parameterised, and
every figure can be traced back to individual cost_events rows.

Cost per build: a build is a runs row with workflow='onboard' and
run_purpose='build'. Its cost is its own cost_events plus those of its step runs
(run_purpose 'skill_standalone') the pipeline links to it through runs.parent_run_id (pipeline commit 9107fc8,
Oct 11 2026). Builds that ran before that commit have no linked steps; they are
flagged `linked: false` so their partial totals are never mistaken for complete.

Days, "today" and "month to date" are Eastern time (America/New_York).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

TZ = "America/New_York"
PERIODS = {"24h": "24 hours", "7d": "7 days", "30d": "30 days", "90d": "90 days"}
PERIOD_CHOICES = "^(24h|7d|30d|90d|mtd|all)$"

#: One row per vendor charge, with the vendor name normalised through
#: cost_console.vendor_aliases (e.g. 'runwayml' -> 'runway').
LEDGER = """(
    SELECT c.id, COALESCE(a.canonical_vendor_id, lower(c.vendor)) AS vendor_id,
           c.service, c.units, c.unit_name, c.total_cost::numeric AS cost_usd,
           c.created_at AS incurred_at, c.property_id, c.run_id
    FROM public.cost_events c
    LEFT JOIN cost_console.vendor_aliases a ON lower(a.alias) = lower(c.vendor)
)"""


# One ordered mapping shared by SQL aggregation and the pure label helper.
# A reason rule matches only within its workflow; unknown workflows remain visible.
COMPONENT_RULES = (
    (("acquire_listing",), None, None, "Listing"),
    (("enhance", "observe"), None, None, "Photos"),
    (("conceive", "direct", "generate_motion", "finish_beats", "assemble"), None, None, "Film"),
    (("score_music",), None, None, "Film music"),
    (("narrate",), None, None, "Voiceover (film)"),
    (("narrate_guest_cards",), "exact", "guest_card_narration", "Guest-card voiceover"),
    (("narrate_guest_cards",), "prefix", "guest_card_music", "Guest-card music"),
    (("build_guide",), None, None, "Local guide"),
    (("write_copy",), None, None, "Copy"),
    (("landing_page_version",), None, None, "Page"),
    (("photo_correction",), None, None, "Enhancements"),
)


def component_label(workflow_name, generation_reason=None):
    for workflows, kind, reason, label in COMPONENT_RULES:
        if workflow_name not in workflows:
            continue
        if kind == "exact" and generation_reason != reason:
            continue
        if kind == "prefix" and not (generation_reason or "").startswith(reason):
            continue
        return label
    return workflow_name if workflow_name is not None else "(No workflow recorded)"


def component_sql(alias="c"):
    # Only internal identifiers and the constant mapping enter this SQL expression.
    def quote(value):
        return "'" + value.replace("'", "''") + "'"
    clauses = []
    for workflows, kind, reason, label in COMPONENT_RULES:
        condition = f"{alias}.workflow_name IN ({', '.join(quote(w) for w in workflows)})"
        if kind == "exact":
            condition += f" AND {alias}.generation_reason = {quote(reason)}"
        elif kind == "prefix":
            condition += f" AND starts_with({alias}.generation_reason, {quote(reason)})"
        clauses.append(f"WHEN {condition} THEN {quote(label)}")
    return "CASE " + " ".join(clauses) + f" ELSE COALESCE({alias}.workflow_name, '(No workflow recorded)') END"


COMPONENT_LEDGER = LEDGER.replace(
    "c.service,", f"c.workflow_name, c.generation_reason, {component_sql()} AS component, c.service,", 1)

# Aggregate charges and runs separately before joining inventory: multiple builds
# or components must never multiply a property's spend. Zero-spend inventory and
# unassigned ledger rows remain visible. Existing charged-property view is retained.
PROPERTY_COSTS_SQL = """
    WITH inventory AS (
        SELECT id AS property_id, name AS property_name, account_id FROM public.properties
        UNION ALL
        SELECT DISTINCT c.property_id, NULL::text, NULL::uuid FROM public.cost_events c
        WHERE NOT EXISTS (SELECT 1 FROM public.properties p WHERE p.id = c.property_id)
    ), charges AS (
        SELECT l.property_id, SUM(l.cost_usd) AS total_cost, COUNT(*) AS event_count,
               COUNT(DISTINCT l.vendor_id) AS vendor_count, MAX(l.incurred_at) AS last_activity
        FROM {ledger} l WHERE {where} GROUP BY l.property_id
    ), component_costs AS (
        SELECT l.property_id, l.component, COALESCE(SUM(l.cost_usd), 0) AS cost_usd, COUNT(*) AS events
        FROM {ledger} l WHERE {where} GROUP BY l.property_id, l.component
    ), components AS (
        SELECT property_id, jsonb_agg(jsonb_build_object(
            'component', component, 'cost_usd', cost_usd, 'events', events)
            ORDER BY cost_usd DESC, component) AS components
        FROM component_costs GROUP BY property_id
    ), builds AS (
        SELECT r.property_id, COUNT(*) AS build_count FROM public.runs r
        WHERE r.workflow = 'onboard' AND r.run_purpose = 'build' AND {build_where}
        GROUP BY r.property_id
    )
    SELECT i.property_id, i.property_name, i.account_id,
           COALESCE(a.name, '(No account recorded)') AS account_name,
           COALESCE(c.total_cost, 0) AS total_cost, COALESCE(c.event_count, 0) AS event_count,
           COALESCE(c.vendor_count, 0) AS vendor_count, c.last_activity,
           COALESCE(b.build_count, 0) AS build_count, COALESCE(k.components, '[]'::jsonb) AS components
    FROM inventory i LEFT JOIN public.accounts a ON a.id = i.account_id
    LEFT JOIN charges c ON c.property_id IS NOT DISTINCT FROM i.property_id
    LEFT JOIN builds b ON b.property_id IS NOT DISTINCT FROM i.property_id
    LEFT JOIN components k ON k.property_id IS NOT DISTINCT FROM i.property_id
    WHERE {inventory_filter}
    ORDER BY total_cost DESC, i.property_name NULLS LAST, i.property_id NULLS LAST
"""

ACCOUNT_COSTS_SQL = """
    WITH properties AS ({properties}), inventory AS (
        SELECT a.id AS account_id, a.name AS account_name FROM public.accounts a
        UNION ALL
        SELECT DISTINCT p.account_id, '(No account recorded)' FROM properties p
        WHERE NOT EXISTS (SELECT 1 FROM public.accounts a WHERE a.id = p.account_id)
    ), totals AS (
        SELECT account_id, SUM(total_cost) AS total_cost,
               COUNT(property_id) AS property_count, SUM(build_count) AS build_count,
               SUM(event_count) AS event_count
        FROM properties GROUP BY account_id
    ), component_costs AS (
        SELECT p.account_id, e.value->>'component' AS component,
               SUM((e.value->>'cost_usd')::numeric) AS cost_usd,
               SUM((e.value->>'events')::bigint) AS events
        FROM properties p CROSS JOIN LATERAL jsonb_array_elements(p.components) e(value)
        GROUP BY p.account_id, e.value->>'component'
    ), components AS (
        SELECT account_id, jsonb_agg(jsonb_build_object(
            'component', component, 'cost_usd', cost_usd, 'events', events)
            ORDER BY cost_usd DESC, component) AS components
        FROM component_costs GROUP BY account_id
    )
    SELECT i.account_id, i.account_name, COALESCE(t.total_cost, 0) AS total_cost,
           COALESCE(t.property_count, 0) AS property_count,
           COALESCE(t.build_count, 0) AS build_count, COALESCE(t.event_count, 0) AS event_count,
           COALESCE(k.components, '[]'::jsonb) AS components
    FROM inventory i LEFT JOIN totals t ON t.account_id IS NOT DISTINCT FROM i.account_id
    LEFT JOIN components k ON k.account_id IS NOT DISTINCT FROM i.account_id
    WHERE {account_filter} ORDER BY total_cost DESC, i.account_name, i.account_id NULLS LAST
"""

BUILD_COMPONENTS_SQL = """
    WITH b AS (
        SELECT r.run_id FROM public.runs r
        WHERE r.workflow = 'onboard' AND r.run_purpose = 'build' AND {where} {scope}
    ), steps AS (
        SELECT b.run_id AS build_id, b.run_id FROM b
        UNION ALL
        SELECT b.run_id, s.run_id FROM b JOIN public.runs s
          ON s.parent_run_id = b.run_id AND s.run_purpose = 'skill_standalone'
    )
    SELECT st.build_id, l.component, COALESCE(SUM(l.cost_usd), 0) AS cost_usd, COUNT(*) AS events
    FROM steps st JOIN {ledger} l ON l.run_id = st.run_id
    GROUP BY st.build_id, l.component ORDER BY st.build_id, cost_usd DESC, l.component
"""

TYPICAL_COMPONENTS_SQL = """
    WITH b AS (
        SELECT r.run_id FROM public.runs r
        WHERE r.workflow = 'onboard' AND r.run_purpose = 'build' AND r.status = 'complete' AND {where}
    ), steps AS (
        SELECT b.run_id AS build_id, b.run_id FROM b
        UNION ALL
        SELECT b.run_id, s.run_id FROM b JOIN public.runs s
          ON s.parent_run_id = b.run_id AND s.run_purpose = 'skill_standalone'
    ), costs AS (
        SELECT st.build_id, l.component, COALESCE(SUM(l.cost_usd), 0) AS cost_usd
        FROM steps st JOIN {ledger} l ON l.run_id = st.run_id GROUP BY st.build_id, l.component
    ), names AS (SELECT DISTINCT component FROM costs), per_build AS (
        SELECT b.run_id, names.component, COALESCE(costs.cost_usd, 0) AS cost_usd
        FROM b CROSS JOIN names LEFT JOIN costs ON costs.build_id = b.run_id AND costs.component = names.component
    )
    SELECT component, COUNT(*) AS build_count, AVG(cost_usd) AS average_cost,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY cost_usd) AS median_cost,
           COUNT(*) FILTER (WHERE cost_usd <> 0) AS builds_with_cost
    FROM per_build GROUP BY component ORDER BY average_cost DESC, component
"""


def property_costs_query(period="30d", start_date=None, end_date=None, account_id=None, property_id=None):
    where, params = window("l.incurred_at", period, start_date, end_date)
    build_where, build_params = window("r.started_at", period, start_date, end_date)
    filters, scope_params = [], []
    if account_id is not None:
        filters.append("i.account_id = %s::uuid")
        scope_params.append(account_id)
    if property_id is not None:
        filters.append("i.property_id = %s::uuid")
        scope_params.append(property_id)
    return PROPERTY_COSTS_SQL.format(ledger=COMPONENT_LEDGER, where=where, build_where=build_where,
                                    inventory_filter=" AND ".join(filters) or "TRUE"), params + params + build_params + scope_params


def account_costs_query(period="30d", start_date=None, end_date=None, account_id=None):
    sql, params = property_costs_query(period, start_date, end_date)
    return ACCOUNT_COSTS_SQL.format(properties=sql, account_filter="i.account_id = %s::uuid" if account_id else "TRUE"), params + ([account_id] if account_id else [])


def _components(rows):
    return [{**r, "cost_usd": _num(r["cost_usd"])} for r in rows]


def _property_row(r):
    return {**r, "property_id": str(r["property_id"]) if r["property_id"] else None,
            "property_name": r["property_name"] or ("Unknown property" if r["property_id"] else "No property recorded"),
            "account_id": str(r["account_id"]) if r["account_id"] else None,
            "total_cost": _num(r["total_cost"]), "components": _components(r["components"]),
            "last_activity": r["last_activity"].isoformat() if r["last_activity"] else None}


#: Plain-English names for why the money was spent (the root run's purpose).
PURPOSE_LABELS = {
    "build": "Property builds",
    "recovery": "Build recoveries",
    "publication": "Page publications",
    "correction": "Photo corrections",
    "skill_standalone": "Single steps not linked to a build",
    "legacy_unknown": "Before run purpose was recorded",
    None: "No run recorded",
}


def window(column: str, period: str = "30d", start_date: str | None = None,
           end_date: str | None = None) -> tuple[str, list[Any]]:
    """SQL condition + params limiting `column` to the requested period (Eastern time)."""
    if start_date and end_date:
        return (f"{column} >= (%s::date)::timestamp AT TIME ZONE '{TZ}' "
                f"AND {column} < ((%s::date + 1)::timestamp AT TIME ZONE '{TZ}')",
                [start_date, end_date])
    if period == "all":
        return "TRUE", []
    if period == "mtd":
        return (f"{column} >= date_trunc('month', now() AT TIME ZONE '{TZ}') AT TIME ZONE '{TZ}'", [])
    return f"{column} >= now() - %s::interval", [PERIODS.get(period, "30 days")]


def _num(v: Any, places: int = 4) -> float:
    return round(float(v or 0), places)


class ReportingRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @contextmanager
    def _cur(self) -> Generator[psycopg.Cursor, None, None]:
        with psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=8) as conn:
            conn.read_only = True
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                yield cur

    # --- totals ---------------------------------------------------------

    def vendor_spend(self, period="30d", start_date=None, end_date=None) -> list[dict]:
        where, params = window("l.incurred_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(f"""
                SELECT l.vendor_id, COALESCE(v.vendor_name, l.vendor_id) AS vendor_name,
                       COALESCE(v.category, 'uncategorized') AS category,
                       SUM(l.cost_usd) AS total_cost, COUNT(*) AS event_count
                FROM {LEDGER} l LEFT JOIN cost_console.vendors v ON v.vendor_id = l.vendor_id
                WHERE {where}
                GROUP BY l.vendor_id, v.vendor_name, v.category
                ORDER BY total_cost DESC""", params)
            return [{**r, "total_cost": _num(r["total_cost"])} for r in cur.fetchall()]

    def category_spend(self, period="30d", start_date=None, end_date=None) -> list[dict]:
        where, params = window("l.incurred_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(f"""
                SELECT COALESCE(v.category, 'uncategorized') AS category, SUM(l.cost_usd) AS total_cost,
                       COUNT(*) AS event_count, COUNT(DISTINCT l.vendor_id) AS vendor_count
                FROM {LEDGER} l LEFT JOIN cost_console.vendors v ON v.vendor_id = l.vendor_id
                WHERE {where}
                GROUP BY 1 ORDER BY total_cost DESC""", params)
            return [{**r, "total_cost": _num(r["total_cost"])} for r in cur.fetchall()]

    def purpose_spend(self, period="30d", start_date=None, end_date=None) -> list[dict]:
        """Spend by why it happened: the purpose of the build (or other run) the charge belongs to."""
        where, params = window("l.incurred_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(f"""
                SELECT root.run_purpose AS purpose, SUM(l.cost_usd) AS total_cost, COUNT(*) AS event_count,
                       COUNT(DISTINCT l.property_id) AS property_count
                FROM {LEDGER} l
                LEFT JOIN public.runs r ON r.run_id = l.run_id
                LEFT JOIN public.runs root ON root.run_id = COALESCE(r.parent_run_id, r.run_id)
                WHERE {where}
                GROUP BY root.run_purpose ORDER BY total_cost DESC""", params)
            return [{"purpose": r["purpose"], "workflow_name": PURPOSE_LABELS.get(r["purpose"], r["purpose"]),
                     "total_cost": _num(r["total_cost"]), "event_count": r["event_count"],
                     "property_count": r["property_count"]} for r in cur.fetchall()]

    def timeseries(self, period="30d", start_date=None, end_date=None) -> list[dict]:
        where, params = window("l.incurred_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(f"""
                SELECT (l.incurred_at AT TIME ZONE '{TZ}')::date AS date, SUM(l.cost_usd) AS total_cost,
                       COUNT(*) AS event_count
                FROM {LEDGER} l WHERE {where}
                GROUP BY 1 ORDER BY 1""", params)
            return [{"date": r["date"].isoformat(), "total_cost": _num(r["total_cost"]),
                     "event_count": r["event_count"]} for r in cur.fetchall()]

    # --- builds -----------------------------------------------------------

    _BUILDS_SQL = """
        WITH b AS (
            SELECT r.run_id, r.property_id, r.status, r.started_at, r.completed_at
            FROM public.runs r
            WHERE r.workflow = 'onboard' AND r.run_purpose = 'build' AND {where} {prop}
            ORDER BY r.started_at DESC LIMIT %s
        ), steps AS (
            SELECT b.run_id AS build_id, b.run_id FROM b
            UNION ALL
            SELECT b.run_id, s.run_id FROM b JOIN public.runs s ON s.parent_run_id = b.run_id AND s.run_purpose = 'skill_standalone'
        ), per_vendor AS (
            SELECT st.build_id, l.vendor_id, SUM(l.cost_usd) AS cost_usd, CASE WHEN COUNT(DISTINCT (l.service, l.unit_name)) = 1 THEN SUM(l.units) END AS units,
                   CASE WHEN COUNT(DISTINCT (l.service, l.unit_name)) = 1 THEN MAX(l.unit_name) END AS unit_name, COUNT(*) AS events
            FROM steps st JOIN {ledger} l ON l.run_id = st.run_id
            GROUP BY st.build_id, l.vendor_id
        )
        SELECT b.run_id, b.property_id, p.name AS property_name, b.status, b.started_at, b.completed_at,
               (SELECT COUNT(*) FROM public.runs k WHERE k.parent_run_id = b.run_id AND k.run_purpose = 'skill_standalone') AS linked_steps,
               COALESCE(SUM(pv.cost_usd), 0) AS total_cost,
               COALESCE(jsonb_agg(jsonb_build_object(
                   'vendor_id', pv.vendor_id, 'vendor_name', COALESCE(v.vendor_name, pv.vendor_id),
                   'cost_usd', ROUND(pv.cost_usd, 4), 'units', pv.units, 'unit_name', pv.unit_name,
                   'events', pv.events) ORDER BY pv.cost_usd DESC)
                 FILTER (WHERE pv.vendor_id IS NOT NULL), '[]'::jsonb) AS vendors
        FROM b
        LEFT JOIN public.properties p ON p.id = b.property_id
        LEFT JOIN per_vendor pv ON pv.build_id = b.run_id
        LEFT JOIN cost_console.vendors v ON v.vendor_id = pv.vendor_id
        GROUP BY b.run_id, b.property_id, p.name, b.status, b.started_at, b.completed_at
        ORDER BY b.started_at DESC"""

    def builds(self, period="30d", start_date=None, end_date=None, property_id=None, limit=200, account_id=None) -> list[dict]:
        where, params = window("r.started_at", period, start_date, end_date)
        prop = ""
        if property_id:
            prop, params = "AND r.property_id = %s", params + [property_id]
        if account_id is not None:
            prop += " AND r.property_id IN (SELECT id FROM public.properties WHERE account_id = %s::uuid)"
            params.append(account_id)
        with self._cur() as cur:
            cur.execute(self._BUILDS_SQL.format(where=where, prop=prop, ledger=LEDGER), params + [limit])
            rows = cur.fetchall()
        components = self.build_components(period, start_date, end_date, build_ids=[str(r["run_id"]) for r in rows])
        return [{"run_id": str(r["run_id"]), "property_id": str(r["property_id"]) if r["property_id"] else None,
                     "property_name": r["property_name"], "status": r["status"],
                     "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                     "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                     "linked_steps": r["linked_steps"], "linked": r["linked_steps"] > 0,
                     "total_cost": _num(r["total_cost"]), "vendors": r["vendors"],
                     "components": components.get(str(r["run_id"]), [])} for r in rows]

    def build_averages(self, period="90d", start_date=None, end_date=None) -> dict:
        """Average vendor cost per completed build, over builds whose steps are linked (complete totals only)."""
        where, params = window("r.started_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(f"""
                WITH b AS (
                    SELECT r.run_id FROM public.runs r
                    WHERE r.workflow = 'onboard' AND r.run_purpose = 'build' AND r.status = 'complete'
                      AND EXISTS (SELECT 1 FROM public.runs k WHERE k.parent_run_id = r.run_id AND k.run_purpose = 'skill_standalone') AND {where}
                ), steps AS (
                    SELECT b.run_id AS build_id, b.run_id FROM b
                    UNION ALL SELECT b.run_id, s.run_id FROM b JOIN public.runs s ON s.parent_run_id = b.run_id AND s.run_purpose = 'skill_standalone'
                ), per AS (
                    SELECT st.build_id, l.vendor_id, SUM(l.cost_usd) AS c,
                           CASE WHEN COUNT(DISTINCT (l.service, l.unit_name)) = 1 THEN SUM(l.units) END AS u,
                           CASE WHEN COUNT(DISTINCT (l.service, l.unit_name)) = 1 THEN MAX(l.unit_name) END AS un,
                           CASE WHEN COUNT(DISTINCT (l.service, l.unit_name)) = 1 THEN MAX(l.service) END AS svc
                    FROM steps st JOIN {LEDGER} l ON l.run_id = st.run_id GROUP BY 1, 2
                ), n AS (SELECT COUNT(*) AS builds FROM b)
                SELECT per.vendor_id, COALESCE(v.vendor_name, per.vendor_id) AS vendor_name, n.builds,
                       COUNT(*) AS builds_using, SUM(per.c) / NULLIF(n.builds, 0) AS cost_per_build,
                       CASE WHEN COUNT(DISTINCT (per.svc, per.un)) = 1 AND COUNT(per.u) = COUNT(*) THEN AVG(per.u) END AS units_when_used,
                       CASE WHEN COUNT(DISTINCT (per.svc, per.un)) = 1 AND COUNT(per.u) = COUNT(*) THEN MAX(per.un) END AS unit_name
                FROM per CROSS JOIN n LEFT JOIN cost_console.vendors v ON v.vendor_id = per.vendor_id
                GROUP BY per.vendor_id, v.vendor_name, n.builds ORDER BY cost_per_build DESC""", params)
            rows = cur.fetchall()
            builds = rows[0]["builds"] if rows else 0
            if not rows:
                cur.execute(f"""SELECT COUNT(*) AS n FROM public.runs r WHERE r.workflow = 'onboard'
                    AND r.run_purpose = 'build' AND r.status = 'complete'
                    AND EXISTS (SELECT 1 FROM public.runs k WHERE k.parent_run_id = r.run_id AND k.run_purpose = 'skill_standalone') AND {where}""", params)
                builds = cur.fetchone()["n"]
            vendors = [{"vendor_id": r["vendor_id"], "vendor_name": r["vendor_name"],
                        "builds_using": r["builds_using"], "cost_per_build": _num(r["cost_per_build"]),
                        "units_when_used": _num(r["units_when_used"], 1) if r["units_when_used"] is not None else None, "unit_name": r["unit_name"]} for r in rows]
            return {"completed_linked_builds": builds, "cost_per_build": _num(sum(x["cost_per_build"] for x in vendors)),
                    "vendors": vendors, **self.typical_components(period, start_date, end_date)}

    # --- properties -------------------------------------------------------

    def properties(self, period="30d", start_date=None, end_date=None) -> list[dict]:
        where, params = window("l.incurred_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(f"""
                SELECT l.property_id, p.name AS property_name, SUM(l.cost_usd) AS total_cost, COUNT(*) AS event_count,
                       COUNT(DISTINCT l.vendor_id) AS vendor_count, MAX(l.incurred_at) AS last_activity,
                       (SELECT COUNT(*) FROM public.runs r WHERE r.property_id = l.property_id
                          AND r.workflow = 'onboard' AND r.run_purpose = 'build') AS builds_all_time
                FROM {LEDGER} l LEFT JOIN public.properties p ON p.id = l.property_id
                WHERE {where}
                GROUP BY l.property_id, p.name ORDER BY total_cost DESC""", params)
            rows = cur.fetchall()
        details = {p["property_id"]: p for p in self.property_costs(period, start_date, end_date)}
        return [{"property_id": str(r["property_id"]) if r["property_id"] else None,
                     "property_name": r["property_name"] or ("No property recorded" if not r["property_id"] else "Unknown property"),
                     "total_cost": _num(r["total_cost"]), "event_count": r["event_count"],
                     "vendor_count": r["vendor_count"], "builds_all_time": r["builds_all_time"],
                     "last_activity": r["last_activity"].isoformat() if r["last_activity"] else None,
                     "account_name": details.get(str(r["property_id"]) if r["property_id"] else None, {}).get("account_name"),
                     "components": details.get(str(r["property_id"]) if r["property_id"] else None, {}).get("components", [])}
                    for r in rows]

    def property_detail(self, property_id: str, period="all", start_date=None, end_date=None) -> dict:
        where, params = window("l.incurred_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute("SELECT name FROM public.properties WHERE id = %s", [property_id])
            row = cur.fetchone()
            cur.execute(f"""
                SELECT l.vendor_id, COALESCE(v.vendor_name, l.vendor_id) AS vendor_name, SUM(l.cost_usd) AS total_cost,
                       COUNT(*) AS event_count
                FROM {LEDGER} l LEFT JOIN cost_console.vendors v ON v.vendor_id = l.vendor_id
                WHERE l.property_id = %s AND {where}
                GROUP BY l.vendor_id, v.vendor_name ORDER BY total_cost DESC""", [property_id] + params)
            vendors = [{**r, "total_cost": _num(r["total_cost"])} for r in cur.fetchall()]
        details = self.property_costs(period, start_date, end_date, property_id=property_id)
        detail = details[0] if details else {}
        return {"property_id": property_id, "property_name": row["name"] if row else None,
                "account_id": detail.get("account_id"), "account_name": detail.get("account_name"),
                "build_count": detail.get("build_count", 0), "components": detail.get("components", []),
                "total_cost": _num(sum(v["total_cost"] for v in vendors)), "by_vendor": vendors,
                "builds": self.builds(period, start_date, end_date, property_id=property_id, limit=None)}

    # --- account and component additions ----------------------------------

    def property_costs(self, period="30d", start_date=None, end_date=None, account_id=None, property_id=None):
        sql, params = property_costs_query(period, start_date, end_date, account_id, property_id)
        with self._cur() as cur:
            cur.execute(sql, params)
            return [_property_row(r) for r in cur.fetchall()]

    def accounts(self, period="30d", start_date=None, end_date=None, account_id=None):
        sql, params = account_costs_query(period, start_date, end_date, account_id)
        with self._cur() as cur:
            cur.execute(sql, params)
            return [{**r, "account_id": str(r["account_id"]) if r["account_id"] else None,
                     "total_cost": _num(r["total_cost"]), "components": _components(r["components"])}
                    for r in cur.fetchall()]

    def account_detail(self, account_id, period="30d", start_date=None, end_date=None):
        accounts = self.accounts(period, start_date, end_date, account_id)
        if not accounts:
            return None
        properties = self.property_costs(period, start_date, end_date, account_id=account_id)
        builds = self.builds(period, start_date, end_date, limit=None, account_id=account_id)
        for p in properties:
            p["builds"] = [b for b in builds if b["property_id"] == p["property_id"]]
        return {**accounts[0], "properties": properties}

    def build_components(self, period="30d", start_date=None, end_date=None, build_ids=None):
        if build_ids == []:
            return {}
        where, params = window("r.started_at", period, start_date, end_date)
        scope = "AND r.run_id = ANY(%s::uuid[])" if build_ids is not None else ""
        with self._cur() as cur:
            cur.execute(BUILD_COMPONENTS_SQL.format(where=where, scope=scope, ledger=COMPONENT_LEDGER),
                        params + ([build_ids] if build_ids is not None else []))
            result = {}
            for r in cur.fetchall():
                result.setdefault(str(r["build_id"]), []).append(
                    {"component": r["component"], "cost_usd": _num(r["cost_usd"]), "events": r["events"]})
            return result

    def typical_components(self, period="90d", start_date=None, end_date=None):
        where, params = window("r.started_at", period, start_date, end_date)
        with self._cur() as cur:
            cur.execute(TYPICAL_COMPONENTS_SQL.format(where=where, ledger=COMPONENT_LEDGER), params)
            components = [{**r, "average_cost": _num(r["average_cost"]), "median_cost": _num(r["median_cost"])}
                          for r in cur.fetchall()]
            cur.execute(f"""SELECT COUNT(*) AS n FROM public.runs r WHERE r.workflow = 'onboard'
                AND r.run_purpose = 'build' AND r.status = 'complete' AND {where}""", params)
            count = cur.fetchone()["n"]
        return {"component_build_count": count, "components": components}


    def monthly_property_costs(self):
        with self._cur() as cur:
            cur.execute(MONTHLY_PROPERTY_COSTS_SQL)
            return monthly_property_result(cur.fetchall())

    # --- summary ----------------------------------------------------------

    def summary(self) -> dict:
        with self._cur() as cur:
            cur.execute(f"""
                SELECT l.vendor_id, COALESCE(v.vendor_name, l.vendor_id) AS vendor_name,
                       SUM(l.cost_usd) FILTER (WHERE l.incurred_at >= date_trunc('day', now() AT TIME ZONE '{TZ}') AT TIME ZONE '{TZ}') AS today,
                       SUM(l.cost_usd) AS mtd
                FROM {LEDGER} l LEFT JOIN cost_console.vendors v ON v.vendor_id = l.vendor_id
                WHERE l.incurred_at >= date_trunc('month', now() AT TIME ZONE '{TZ}') AT TIME ZONE '{TZ}'
                GROUP BY l.vendor_id, v.vendor_name ORDER BY mtd DESC""")
            rows = cur.fetchall()
            cur.execute("""
                SELECT DISTINCT ON (vendor_id) vendor_id, status, completed_at
                FROM cost_console.sync_runs ORDER BY vendor_id, started_at DESC""")
            syncs = cur.fetchall()
            cur.execute("""SELECT COUNT(*) AS n FROM cost_console.sync_runs
                           WHERE status = 'failed' AND started_at > now() - interval '24 hours'""")
            failed = cur.fetchone()["n"]
        return {
            "today_spend": _num(sum(float(r["today"] or 0) for r in rows), 2),
            "mtd_spend": _num(sum(float(r["mtd"] or 0) for r in rows), 2),
            "today_by_vendor": [{"vendor_id": r["vendor_id"], "vendor_name": r["vendor_name"], "cost_usd": _num(r["today"])}
                                for r in rows if r["today"]],
            "mtd_by_vendor": [{"vendor_id": r["vendor_id"], "vendor_name": r["vendor_name"], "cost_usd": _num(r["mtd"])}
                              for r in rows],
            "last_syncs": [{"vendor_id": s["vendor_id"], "status": s["status"],
                            "completed_at": s["completed_at"].isoformat() if s["completed_at"] else None} for s in syncs],
            "failed_syncs_today": failed,
            "recent_builds": self.builds("30d", limit=5),
        }


# Consecutive 30-day windows, independent of the dashboard calendar-period filter.
MONTHLY_PROPERTY_COSTS_SQL = """
WITH RECURSIVE first_build AS (
    SELECT property_id, MIN(started_at) AS first_build_at
    FROM public.runs
    WHERE workflow = 'onboard' AND run_purpose = 'build' AND started_at <= now()
    GROUP BY property_id
), run_tree AS (
    SELECT run_id, workflow AS root_workflow FROM public.runs WHERE parent_run_id IS NULL
    UNION ALL
    SELECT child.run_id, tree.root_workflow FROM public.runs child
    JOIN run_tree tree ON child.parent_run_id = tree.run_id
), charges AS (
    SELECT c.property_id, c.created_at, COALESCE(c.total_cost, 0)::numeric AS cost,
           CASE
             WHEN concat_ws(' ', c.vendor, c.workflow_name, r.workflow, tree.root_workflow)
                  ~* '(paid[_ -]?ads?|ad[_ -]?spend|google[_ -]?ads|meta[_ -]?ads|facebook[_ -]?ads|tiktok[_ -]?ads|microsoft[_ -]?ads|bing[_ -]?ads)' THEN 'paid_ads'
             WHEN concat_ws(' ', c.workflow_name, r.workflow, tree.root_workflow)
                  ~* '(social|video[_ -]?distribut|distribut.*video)' THEN 'social_videos'
             WHEN r.workflow IN ('onboard', 'monthly_cycle', 'recover')
               OR tree.root_workflow IN ('onboard', 'monthly_cycle', 'recover')
               OR c.workflow_name IN ('onboard', 'monthly_cycle', 'recover') THEN 'build'
             ELSE 'other'
           END AS category
    FROM public.cost_events c
    LEFT JOIN public.runs r ON r.run_id = c.run_id
    LEFT JOIN run_tree tree ON tree.run_id = r.run_id
)
SELECT p.id AS property_id, p.name AS property_name, a.name AS account_name,
       f.first_build_at, m.month_number,
       f.first_build_at + (m.month_number - 1) * interval '720 hours' AS starts_at,
       f.first_build_at + m.month_number * interval '720 hours' AS ends_at,
       COALESCE(SUM(c.cost) FILTER (WHERE c.category = 'build'), 0) AS build,
       COALESCE(SUM(c.cost) FILTER (WHERE c.category = 'social_videos'), 0) AS social_videos,
       COALESCE(SUM(c.cost) FILTER (WHERE c.category = 'paid_ads'), 0) AS paid_ads,
       COALESCE(SUM(c.cost), 0) AS month_total
FROM public.properties p
LEFT JOIN public.accounts a ON a.id = p.account_id
LEFT JOIN first_build f ON f.property_id = p.id
LEFT JOIN LATERAL generate_series(1,
    FLOOR(EXTRACT(EPOCH FROM (now() - f.first_build_at)) / 2592000)::integer + 1
) m(month_number) ON true
LEFT JOIN charges c ON c.property_id = p.id
    AND c.created_at >= f.first_build_at + (m.month_number - 1) * interval '720 hours'
    AND c.created_at < f.first_build_at + m.month_number * interval '720 hours'
    AND c.created_at <= now()
GROUP BY p.id, p.name, a.name, f.first_build_at, m.month_number
ORDER BY p.name, p.id, m.month_number
"""

def monthly_property_result(rows):
    properties, totals = {}, {}
    fields = ("build", "social_videos", "paid_ads", "month_total")
    for row in rows:
        key = str(row["property_id"])
        prop = properties.setdefault(key, {
            "property_id": key, "property_name": row["property_name"],
            "account_name": row["account_name"],
            "first_build_at": row["first_build_at"].isoformat() if row["first_build_at"] else None,
            "months": [],
        })
        month = row["month_number"]
        if month is None:
            continue
        values = {field: float(row[field]) for field in fields}
        prop["months"].append({
            "month_number": month, **values,
            "starts_at": row["starts_at"].isoformat(), "ends_at": row["ends_at"].isoformat(),
        })
        summary = totals.setdefault(month, {"month_number": month, "property_count": 0,
                                           **{field: 0.0 for field in fields}})
        summary["property_count"] += 1
        for field in fields:
            summary[field] += values[field]
    averages = [{**r, **{field: round(r[field] / r["property_count"], 6) for field in fields}}
                for _, r in sorted(totals.items())]
    return {"properties": list(properties.values()), "averages": averages}
