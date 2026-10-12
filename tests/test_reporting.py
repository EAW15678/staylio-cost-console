"""Reporting reads the pipeline ledger read-only and never leaks connection details."""
import os
os.environ.pop("CONSOLE_API_TOKEN", None)
from fastapi.testclient import TestClient
import main
import reporting
from reporting import window, PURPOSE_LABELS


def test_window_periods_are_parameterised_and_eastern():
    sql, params = window("x", "7d")
    assert sql == "x >= now() - %s::interval" and params == ["7 days"]
    sql, params = window("x", start_date="2026-10-01", end_date="2026-10-11")
    assert "America/New_York" in sql and params == ["2026-10-01", "2026-10-11"]
    assert window("x", "all") == ("TRUE", [])
    assert "date_trunc('month'" in window("x", "mtd")[0]


def test_every_run_purpose_has_a_plain_label():
    for p in ("build", "recovery", "publication", "correction", "skill_standalone", "legacy_unknown", None):
        assert PURPOSE_LABELS[p]


def test_build_cost_counts_only_linked_step_runs():
    # Recovery/publication children are reported under their own purpose, not inside a build's cost.
    assert reporting.ReportingRepository._BUILDS_SQL.count("s.run_purpose = 'skill_standalone'") == 1
    assert "k.run_purpose = 'skill_standalone'" in reporting.ReportingRepository._BUILDS_SQL


class Boom:
    def __getattr__(self, name):
        def fail(*a, **k):
            raise RuntimeError('connection to server at "1.2.3.4" failed for user "postgres"')
        return fail


class Fake:
    def builds(self, *a, **k):
        return [{"run_id": "b1", "total_cost": 2.99, "linked": True, "vendors": []}]
    def vendor_spend(self, *a, **k):
        return [{"vendor_id": "anthropic", "vendor_name": "Anthropic", "category": "ai_generation", "total_cost": 1.5, "event_count": 3}]


def client(repo):
    main.app.dependency_overrides[main.get_reporting] = lambda: repo
    return TestClient(main.app)


def test_errors_are_plain_and_never_expose_connection_details():
    r = client(Boom()).get("/metrics/vendors")
    assert r.status_code == 500
    assert "postgres" not in r.text and "1.2.3.4" not in r.text
    assert r.json()["detail"].startswith("Cost data unavailable")


def test_builds_and_vendors_endpoints_shape():
    c = client(Fake())
    assert c.get("/metrics/builds?period=all").json()["builds"][0]["total_cost"] == 2.99
    body = c.get("/metrics/vendors?period=mtd").json()
    assert body["total"] == 1.5 and body["vendors"][0]["cost_usd"] == 1.5


def test_bad_inputs_are_rejected():
    c = client(Fake())
    assert c.get("/metrics/vendors?period=forever").status_code == 422
    assert c.get("/metrics/property/not-a-uuid").status_code == 400
