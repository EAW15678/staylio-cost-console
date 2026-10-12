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
            SELECT st.build_id, l.vendor_id, SUM(l.cost_usd) AS cost_usd, SUM(l.units) AS units,
                   MAX(l.unit_name) AS unit_name, COUNT(*) AS events
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

    def builds(self, period="30d", start_date=None, end_date=None, property_id=None, limit=200) -> list[dict]:
        where, params = window("r.started_at", period, start_date, end_date)
        prop = ""
        if property_id:
            prop, params = "AND r.property_id = %s", params + [property_id]
        with self._cur() as cur:
            cur.execute(self._BUILDS_SQL.format(where=where, prop=prop, ledger=LEDGER), params + [limit])
            return [{"run_id": str(r["run_id"]), "property_id": str(r["property_id"]) if r["property_id"] else None,
                     "property_name": r["property_name"], "status": r["status"],
                     "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                     "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                     "linked_steps": r["linked_steps"], "linked": r["linked_steps"] > 0,
                     "total_cost": _num(r["total_cost"]), "vendors": r["vendors"]} for r in cur.fetchall()]

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
                    SELECT st.build_id, l.vendor_id, SUM(l.cost_usd) AS c, SUM(l.units) AS u, MAX(l.unit_name) AS un
                    FROM steps st JOIN {LEDGER} l ON l.run_id = st.run_id GROUP BY 1, 2
                ), n AS (SELECT COUNT(*) AS builds FROM b)
                SELECT per.vendor_id, COALESCE(v.vendor_name, per.vendor_id) AS vendor_name, n.builds,
                       COUNT(*) AS builds_using, SUM(per.c) / NULLIF(n.builds, 0) AS cost_per_build,
                       AVG(per.u) AS units_when_used, MAX(per.un) AS unit_name
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
                        "units_when_used": _num(r["units_when_used"], 1), "unit_name": r["unit_name"]} for r in rows]
            return {"completed_linked_builds": builds, "cost_per_build": _num(sum(x["cost_per_build"] for x in vendors)),
                    "vendors": vendors}

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
            return [{"property_id": str(r["property_id"]) if r["property_id"] else None,
                     "property_name": r["property_name"] or ("No property recorded" if not r["property_id"] else "Unknown property"),
                     "total_cost": _num(r["total_cost"]), "event_count": r["event_count"],
                     "vendor_count": r["vendor_count"], "builds_all_time": r["builds_all_time"],
                     "last_activity": r["last_activity"].isoformat() if r["last_activity"] else None}
                    for r in cur.fetchall()]

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
        return {"property_id": property_id, "property_name": row["name"] if row else None,
                "total_cost": _num(sum(v["total_cost"] for v in vendors)), "by_vendor": vendors,
                "builds": self.builds(period, start_date, end_date, property_id=property_id)}

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
