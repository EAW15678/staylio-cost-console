"""Additive cost views: labels, inventory, safe totals, API contracts and lineage."""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
import main
import reporting
from tests.test_reporting import client


@pytest.mark.parametrize('workflow,reason,label', [
    ('acquire_listing', None, 'Listing'), ('enhance', None, 'Photos'),
    ('observe', None, 'Photos'), ('conceive', None, 'Film'),
    ('direct', None, 'Film'), ('generate_motion', None, 'Film'),
    ('finish_beats', None, 'Film'), ('assemble', None, 'Film'),
    ('score_music', None, 'Film music'), ('narrate', None, 'Voiceover (film)'),
    ('narrate_guest_cards', 'guest_card_narration', 'Guest-card voiceover'),
    ('narrate_guest_cards', 'guest_card_music', 'Guest-card music'),
    ('narrate_guest_cards', 'guest_card_music_silent', 'Guest-card music'),
    ('narrate_guest_cards', 'other', 'narrate_guest_cards'),
    ('narrate_guest_cards', None, 'narrate_guest_cards'),
    ('other', 'guest_card_music', 'other'),
    ('build_guide', None, 'Local guide'), ('write_copy', None, 'Copy'),
    ('landing_page_version', None, 'Page'), ('photo_correction', None, 'Enhancements'),
    ('legacy_unknown', None, 'legacy_unknown'), ('', None, ''),
    (None, None, '(No workflow recorded)'),
])
def test_component_mapping_never_drops_unknown_workflows(workflow, reason, label):
    assert reporting.component_label(workflow, reason) == label


class Cursor:
    def __init__(self, results):
        self.results = iter(results)
        self.queries = []
    def execute(self, sql, params=None):
        self.queries.append((sql, params or []))
        self.current = next(self.results)
        assert sql.lstrip().startswith(('SELECT', 'WITH'))
    def fetchall(self):
        return self.current
    def fetchone(self):
        return self.current[0]


class Repository(reporting.ReportingRepository):
    def __init__(self, results):
        self.cursor = Cursor(results)
    @contextmanager
    def _cur(self):
        yield self.cursor


ACCOUNT = '00000000-0000-0000-0000-000000000001'
PROPERTY = '00000000-0000-0000-0000-000000000002'
BUILD = '00000000-0000-0000-0000-000000000003'


def property_row(total='12.3456', components=None):
    return dict(property_id=UUID(PROPERTY), property_name='House', account_id=UUID(ACCOUNT),
                account_name='Owner', total_cost=Decimal(total), event_count=2, vendor_count=1,
                last_activity=datetime(2026, 10, 13, tzinfo=timezone.utc), build_count=2,
                components=components if components is not None else [dict(component='Film', cost_usd=Decimal(total), events=2)])


def test_property_totals_preserve_zero_spend_inventory_and_component_amounts():
    paid = property_row()
    empty = {**property_row('0', []), 'property_id': None, 'property_name': None, 'account_id': None,
             'last_activity': None, 'event_count': 0, 'vendor_count': 0, 'build_count': 0}
    repo = Repository([[paid, empty]])
    result = repo.property_costs('all')
    assert sum(p['total_cost'] for p in result) == 12.3456
    assert result[0]['components'][0]['cost_usd'] == result[0]['total_cost']
    assert result[0]['build_count'] == 2 and result[0]['account_name'] == 'Owner'
    assert result[1]['total_cost'] == 0 and result[1]['components'] == []
    assert result[1]['property_id'] is None and result[1]['property_name'] == 'No property recorded'
    sql, params = repo.cursor.queries[0]
    assert params == []
    assert 'NOT EXISTS (SELECT 1 FROM public.properties' in sql
    assert 'LEFT JOIN charges' in sql and 'IS NOT DISTINCT FROM' in sql
    # Charge/component aggregates finish before joining builds; no cost x build fanout.
    assert 'FROM' in sql and 'GROUP BY l.property_id' in sql
    assert 'LEFT JOIN builds b' in sql and 'COALESCE(b.build_count, 0)' in sql


def test_account_totals_are_not_multiplied_by_components_or_builds():
    rows = [dict(account_id=UUID(ACCOUNT), account_name='Owner', total_cost=Decimal('15'),
                 property_count=2, build_count=3, event_count=4,
                 components=[dict(component='Film', cost_usd=10, events=2), dict(component='Copy', cost_usd=5, events=2)]),
            dict(account_id=None, account_name='(No account recorded)', total_cost=Decimal('2'),
                 property_count=0, build_count=0, event_count=1,
                 components=[dict(component='legacy', cost_usd=2, events=1)])]
    repo = Repository([rows])
    result = repo.accounts('all')
    assert sum(a['total_cost'] for a in result) == 17
    for a in result:
        assert sum(c['cost_usd'] for c in a['components']) == a['total_cost']
    sql, _ = repo.cursor.queries[0]
    assert 'SELECT account_id, SUM(total_cost) AS total_cost' in sql
    assert 'COUNT(property_id) AS property_count, SUM(build_count)' in sql
    assert 'LEFT JOIN totals' in sql and 'LEFT JOIN components' in sql
    assert 'SELECT a.id AS account_id, a.name AS account_name FROM public.accounts' in sql


def test_filters_are_bound_for_both_cost_and_build_windows():
    sql, params = reporting.property_costs_query('all', '2026-10-01', '2026-10-13', ACCOUNT, PROPERTY)
    assert params == ['2026-10-01', '2026-10-13'] * 3 + [ACCOUNT, PROPERTY]
    assert sql.count('%s') == len(params)
    assert ACCOUNT not in sql and PROPERTY not in sql
    assert sql.count('America/New_York') == 6
    sql, params = reporting.account_costs_query('7d', account_id=ACCOUNT)
    assert params == ['7 days'] * 3 + [ACCOUNT]
    assert sql.count('%s') == len(params)


def test_builds_keep_vendors_and_add_only_linked_component_costs():
    row = dict(run_id=UUID(BUILD), property_id=UUID(PROPERTY), property_name='House', status='complete',
               started_at=datetime(2026, 10, 13, tzinfo=timezone.utc), completed_at=None,
               linked_steps=1, total_cost=Decimal('3'),
               vendors=[dict(vendor_id='elevenlabs', cost_usd=3, units=None, unit_name=None)])
    repo = Repository([[row], [dict(build_id=UUID(BUILD), component='Film music', cost_usd=Decimal('3'), events=1)]])
    result = repo.builds('all')
    assert result[0]['vendors'] == row['vendors']
    assert result[0]['components'] == [dict(component='Film music', cost_usd=3.0, events=1)]
    assert repo.cursor.queries[1][1] == [[BUILD]]
    assert "s.run_purpose = 'skill_standalone'" in repo.cursor.queries[1][0]
    assert 'ANY(%s::uuid[])' in repo.cursor.queries[1][0]


def test_mixed_units_are_omitted_in_builds_and_averages():
    assert 'COUNT(DISTINCT (l.service, l.unit_name)) = 1 THEN SUM(l.units)' in reporting.ReportingRepository._BUILDS_SQL
    row = dict(vendor_id='elevenlabs', vendor_name='ElevenLabs', builds=2, builds_using=2,
               cost_per_build=Decimal('3'), units_when_used=None, unit_name=None)
    repo = Repository([[row], [], [dict(n=2)]])
    data = repo.build_averages('all')
    assert data['vendors'][0]['units_when_used'] is None
    assert data['vendors'][0]['unit_name'] is None
    assert 'COUNT(per.u) = COUNT(*)' in repo.cursor.queries[0][0]


def test_typical_build_zero_fills_components_across_all_completed_builds():
    repo = Repository([[dict(component='Film', build_count=3, average_cost=Decimal('4'), median_cost=0, builds_with_cost=1)], [dict(n=3)]])
    result = repo.typical_components('all')
    assert result['component_build_count'] == 3
    assert result['components'][0]['median_cost'] == 0
    sql = repo.cursor.queries[0][0]
    assert 'CROSS JOIN names LEFT JOIN costs' in sql
    assert 'COALESCE(costs.cost_usd, 0)' in sql
    assert "r.status = 'complete'" in sql and 'EXISTS' not in sql
    assert 'percentile_cont(0.5)' in sql


def test_account_drilldown_attaches_each_build_to_its_own_property():
    class Detail(reporting.ReportingRepository):
        def accounts(self, *args):
            return [dict(account_id=ACCOUNT, total_cost=7)]
        def property_costs(self, *args, **kwargs):
            return [dict(property_id=PROPERTY), dict(property_id='other')]
        def builds(self, *args, **kwargs):
            assert kwargs == dict(limit=None, account_id=ACCOUNT)
            return [dict(run_id=BUILD, property_id=PROPERTY, components=[], vendors=[])]
    result = Detail('unused').account_detail(ACCOUNT, 'all')
    assert len(result['properties'][0]['builds']) == 1
    assert result['properties'][1]['builds'] == []


class FakeViews:
    def accounts(self, *args):
        return [dict(account_id=ACCOUNT, account_name='Owner', total_cost=7, property_count=2, build_count=3, components=[])]
    def account_detail(self, account_id, *args):
        return {**self.accounts()[0], 'properties': [dict(property_id=PROPERTY, builds=[], components=[])]} if account_id == ACCOUNT else None
    def property_costs(self, *args):
        return [dict(property_id=PROPERTY, account_name='Owner', total_cost=7, build_count=3, components=[])]


def test_account_and_property_endpoints_totals_and_validation():
    c = client(FakeViews())
    assert c.get('/metrics/accounts?period=all').json()['total'] == 7
    assert c.get('/metrics/property-costs?period=all').json()['properties'][0]['account_name'] == 'Owner'
    assert c.get(f'/metrics/account/{ACCOUNT}?period=mtd').json()['properties'][0]['builds'] == []
    assert c.get('/metrics/account/bad').status_code == 400
    assert c.get('/metrics/account/00000000-0000-0000-0000-000000000099').status_code == 404
    assert c.get('/metrics/accounts?start_date=2026-10-01').status_code == 400
    assert c.get('/metrics/property-costs?period=invalid').status_code == 422


def test_every_previous_endpoint_and_view_is_retained():
    expected = {'/health', '/admin/bootstrap', '/sync/openai/activity', '/sync/openai/cost',
                '/sync/cloudflare_r2/usage', '/sync/anthropic/cost', '/sync/railway/usage', '/events/estimate',
                '/metrics/summary', '/metrics/vendors', '/metrics/category', '/metrics/workflows',
                '/metrics/timeseries', '/metrics/builds', '/metrics/builds/averages', '/metrics/properties',
                '/metrics/property/{property_id}'}
    assert expected <= {r.path for r in main.app.routes}
    ui = Path('dashboard/src/App.jsx').read_text()
    for title in ['Daily spend (Eastern)', 'Spend by purpose', 'Cost of each property build',
                  'Cost by property', 'Spend by vendor this period', 'Account costs', 'Property costs']:
        assert title in ui
    assert '>Recorded total<' in ui and '>Complete total<' not in ui
    assert 'function VendorList' in ui and 'function SpendChart' in ui


@pytest.mark.parametrize('name', ['accounts', 'properties', 'builds', 'components', 'typical'])
def test_sql_regression_fixtures_use_values_and_selects_only(name):
    from tests.sql_fixtures import queries
    import re
    sql = queries()[name]
    assert sql.lstrip().startswith('WITH')
    statements = re.sub(r"'(?:''|[^'])*'", "''", sql)
    assert not re.search(r'\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|COPY|CALL)\b', statements, re.I)
    assert 'public.' not in sql and 'cost_console.' not in sql
    assert 'fixture_cost_events' in sql
