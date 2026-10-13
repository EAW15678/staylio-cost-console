"""SELECT-only PostgreSQL fixtures for validating the production query builders.

No CREATE/INSERT or persistent fixture data: wrap a generated SELECT with
fixture_query() and execute it using an authorized read-only database connection.
"""
import reporting

FIXTURES = """
fixture_accounts AS (
 SELECT ('00000000-0000-0000-0000-' || lpad(id::text,12,'0'))::uuid AS id, name
 FROM (VALUES (1,'Owner A'),(2,'Owner B'),(3,'Zero owner')) x(id,name)
), fixture_properties AS (
 SELECT ('10000000-0000-0000-0000-' || lpad(id::text,12,'0'))::uuid AS id, name,
        ('00000000-0000-0000-0000-' || lpad(account::text,12,'0'))::uuid AS account_id
 FROM (VALUES (1,'Paid house',1),(2,'Other house',2),(3,'Zero house',1),(4,'Zero account house',3)) x(id,name,account)
), fixture_runs AS (
 SELECT ('20000000-0000-0000-0000-' || lpad(id::text,12,'0'))::uuid AS run_id,
        ('10000000-0000-0000-0000-' || lpad(property::text,12,'0'))::uuid AS property_id,
        'onboard'::text AS workflow, purpose AS run_purpose, status,
        CASE WHEN parent IS NOT NULL THEN ('20000000-0000-0000-0000-' || lpad(parent::text,12,'0'))::uuid END AS parent_run_id,
        '2026-10-13T00:00:00Z'::timestamptz AS started_at, '2026-10-13T01:00:00Z'::timestamptz AS completed_at
 FROM (VALUES (1,1,'build','complete',NULL::int),(2,1,'build','complete',NULL),
              (3,2,'build','complete',NULL),(4,1,'skill_standalone','complete',1),
              (5,1,'recovery','complete',1),(6,2,'skill_standalone','complete',NULL)) x(id,property,purpose,status,parent)
), fixture_cost_events AS (
 SELECT id, CASE WHEN property IS NOT NULL THEN ('10000000-0000-0000-0000-' || lpad(property::text,12,'0'))::uuid END AS property_id,
        CASE WHEN run IS NOT NULL THEN ('20000000-0000-0000-0000-' || lpad(run::text,12,'0'))::uuid END AS run_id,
        vendor, service, units, unit_name, total_cost, workflow_name, generation_reason,
        '2026-10-13T00:30:00Z'::timestamptz AS created_at
 FROM (VALUES
 (1,1,1,'elevenlabs','music_guest_bed',60::real,'seconds',1::numeric,'narrate_guest_cards','guest_card_music_silent'),
 (2,1,1,'elevenlabs','tts_guest',200,'characters',2,'narrate_guest_cards','guest_card_narration'),
 (3,1,4,'fal','camera',5,'seconds',8,'generate_motion','camera_clip'),
 (4,1,5,'anthropic','copy',100,'tokens',100,'write_copy','landing_page_copy'),
 (5,1,2,'elevenlabs','tts',300,'characters',3,'narrate','narration_tts'),
 (6,2,6,'anthropic','legacy',1,'calls',7,'unmapped_workflow','other'),
 (7,NULL,NULL,'other','unassigned',1,'calls',2,NULL,NULL)
 ) x(id,property,run,vendor,service,units,unit_name,total_cost,workflow_name,generation_reason)
), fixture_vendor_aliases AS (SELECT 'unused'::text AS alias, 'unused'::text AS canonical_vendor_id WHERE FALSE),
fixture_vendors AS (SELECT id AS vendor_id, id AS vendor_name FROM (VALUES ('elevenlabs'),('fal'),('anthropic'),('other')) x(id))
"""


def fixture_query(query):
    for relation in ('accounts', 'properties', 'runs', 'cost_events'):
        query = query.replace('public.' + relation, 'fixture_' + relation)
    for relation in ('vendor_aliases', 'vendors'):
        query = query.replace('cost_console.' + relation, 'fixture_' + relation)
    assert '%s' not in query
    return f'WITH {FIXTURES}, result AS ({query}) SELECT * FROM result'


def queries():
    return {
        'accounts': fixture_query(reporting.account_costs_query('all')[0]),
        'properties': fixture_query(reporting.property_costs_query('all')[0]),
        'builds': fixture_query(reporting.ReportingRepository._BUILDS_SQL.format(
            where='TRUE', prop='', ledger=reporting.LEDGER).replace('LIMIT %s', 'LIMIT NULL')),
        'components': fixture_query(reporting.BUILD_COMPONENTS_SQL.format(
            where='TRUE', scope='', ledger=reporting.COMPONENT_LEDGER)),
        'typical': fixture_query(reporting.TYPICAL_COMPONENTS_SQL.format(where='TRUE', ledger=reporting.COMPONENT_LEDGER)),
    }


def validate(name, rows):
    """Expected figures derive from fixture charges, independently of query SQL."""
    if name == 'accounts':
        by_name = {r['account_name']: r for r in rows}
        assert len(rows) == 4
        assert float(by_name['Owner A']['total_cost']) == 114
        assert int(by_name['Owner A']['property_count']) == 2
        assert int(by_name['Owner A']['build_count']) == 2
        assert float(by_name['Owner B']['total_cost']) == 7
        assert float(by_name['Zero owner']['total_cost']) == 0
        assert float(by_name['(No account recorded)']['total_cost']) == 2
        assert sum(float(r['total_cost']) for r in rows) == 123
    elif name == 'properties':
        assert len(rows) == 5
        assert sum(float(r['total_cost']) for r in rows) == 123
        assert sum(int(r['build_count']) for r in rows) == 3
        assert sum(int(r['event_count']) for r in rows) == 7
        assert sum(float(c['cost_usd']) for r in rows for c in r['components']) == 123
        labels = {c['component'] for r in rows for c in r['components']}
        assert 'unmapped_workflow' in labels and '(No workflow recorded)' in labels
    elif name == 'builds':
        by_id = {r['run_id'][-1]: r for r in rows}
        assert len(rows) == 3
        assert [float(by_id[k]['total_cost']) for k in ('1','2','3')] == [11,3,0]
        el = next(v for v in by_id['1']['vendors'] if v['vendor_id'] == 'elevenlabs')
        assert el['units'] is None and el['unit_name'] is None
        el = by_id['2']['vendors'][0]
        assert el['units'] == 300 and el['unit_name'] == 'characters'
    elif name == 'components':
        assert sum(float(r['cost_usd']) for r in rows) == 14
        assert {r['component'] for r in rows} == {'Film','Guest-card music','Guest-card voiceover','Voiceover (film)'}
    elif name == 'typical':
        by_name = {r['component']: r for r in rows}
        for r in rows:
            assert int(r['build_count']) == 3
            assert float(r['median_cost']) == 0
        assert abs(float(by_name['Film']['average_cost']) - 8/3) < 1e-10
        assert abs(float(by_name['Guest-card music']['average_cost']) - 1/3) < 1e-10
