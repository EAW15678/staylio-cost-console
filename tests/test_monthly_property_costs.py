
"""Monthly property windows and averages use recorded costs, without vendor calls."""
from datetime import datetime, timezone
from decimal import Decimal
import reporting
from tests.test_cost_views import Repository
from tests.test_reporting import client

def row(prop, month, cost=0, social=0, ads=0):
    stamp = datetime(2026, 10, 1, tzinfo=timezone.utc)
    return dict(property_id=prop, property_name=prop, account_name="Owner",
                first_build_at=stamp if month else None, month_number=month,
                starts_at=stamp, ends_at=stamp, build=Decimal(str(cost)),
                social_videos=Decimal(str(social)), paid_ads=Decimal(str(ads)),
                month_total=Decimal(str(cost + social + ads)))

def test_averages_zero_fill_reached_months_but_never_include_unreached_months():
    data = reporting.monthly_property_result([
        row("old", 1, 10, 2, 1), row("old", 2, 0),
        row("new", 1, 20), row("unbuilt", None)])
    assert data["averages"] == [
        dict(month_number=1, property_count=2, build=15, social_videos=1, paid_ads=.5, month_total=16.5),
        dict(month_number=2, property_count=1, build=0, social_videos=0, paid_ads=0, month_total=0)]
    assert data["properties"][2]["months"] == []
    assert data["properties"][2]["first_build_at"] is None
    assert data["properties"][0]["months"][0]["month_total"] == 13

def test_empty_inventory():
    assert reporting.monthly_property_result([]) == {"properties": [], "averages": []}

def test_monthly_repository_and_api_are_read_only_and_additive():
    repo = Repository([[row("house", 1, 12)]])
    data = repo.monthly_property_costs()
    sql, params = repo.cursor.queries[0]
    assert params == []
    assert sql.lstrip().startswith("WITH RECURSIVE")
    assert "MIN(started_at)" in sql and "run_purpose = 'build'" in sql
    assert "2592000" in sql and "interval '720 hours'" in sql
    assert "c.created_at >=" in sql and "c.created_at < f.first_build_at" in sql
    assert "LEFT JOIN run_tree" in sql
    class Fake:
        def monthly_property_costs(self): return data
    r = client(Fake()).get("/metrics/properties/monthly")
    assert r.status_code == 200 and r.json()["averages"][0]["build"] == 12
