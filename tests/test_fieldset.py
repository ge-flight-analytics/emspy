import os
import pytest
import pandas as pd

import emspy
from emspy.query import Fieldset
from emspy.query.fieldset import _parse_fieldset_fields
from emspy.query.fltquery import _extract_field_entity_type
from mock_connection import MockConnection
from mock_ems import MockEMS
from mock_query import MockFltQuery


# Fixtures -------------------------------------------------------------------

@pytest.fixture
def mocks(monkeypatch):
    monkeypatch.setattr(emspy.query.query, 'EMS', MockEMS)
    monkeypatch.setattr(emspy, 'Connection', MockConnection)


@pytest.fixture
def fieldset(mocks):
    ems_id = 1
    connection = MockConnection(user='', pwd='')
    return Fieldset(connection, ems_id)


@pytest.fixture
def fltquery(mocks, tmp_path):
    # MockFltQuery wires Fieldset into FltQuery via _init_assets and uses the
    # bundled mock_metadata.db for the unused fieldtree.
    test_path = os.path.dirname(os.path.realpath(__file__))
    db_path = os.path.join(test_path, 'mock_metadata.db')
    return MockFltQuery(MockConnection(user='', pwd=''), 'ems-mock',
                         data_file=db_path)


@pytest.fixture
def fltquery_with_flights_db(fltquery):
    # FltQuery pointed at the FDW Flights database so the entity-type guard
    # in select_fieldset can compare against a real database ID.
    fltquery.set_database('FDW Flights')
    return fltquery


# get_groups -----------------------------------------------------------------

def test_get_groups_returns_dataframe(fieldset):
    df = fieldset.get_groups()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['id', 'name', 'description']
    assert 'mock-group-id' in df['id'].tolist()
    assert 'Mock Group' in df['name'].tolist()


def test_get_groups_uses_cache_on_second_call(fieldset, monkeypatch):
    fieldset.get_groups()  # priming call
    call_count = {'n': 0}
    real_request = fieldset._conn.request

    def counting_request(*args, **kwargs):
        call_count['n'] += 1
        return real_request(*args, **kwargs)

    monkeypatch.setattr(fieldset._conn, 'request', counting_request)
    fieldset.get_groups()
    assert call_count['n'] == 0


def test_get_groups_refresh_bypasses_cache(fieldset, monkeypatch):
    fieldset.get_groups()  # priming call
    call_count = {'n': 0}
    real_request = fieldset._conn.request

    def counting_request(*args, **kwargs):
        call_count['n'] += 1
        return real_request(*args, **kwargs)

    monkeypatch.setattr(fieldset._conn, 'request', counting_request)
    fieldset.get_groups(refresh=True)
    assert call_count['n'] == 1


# get_group ------------------------------------------------------------------

def test_get_group_splits_subgroups_and_fieldsets(fieldset):
    df = fieldset.get_group('mock-group-id')
    assert list(df.columns) == ['type', 'id', 'name', 'description']
    groups = df[df['type'] == 'group']
    fieldsets = df[df['type'] == 'fieldset']
    assert len(groups) == 1
    assert groups.iloc[0]['name'] == 'Sub Group'
    assert len(fieldsets) == 2
    assert set(fieldsets['name'].tolist()) == {'Mock Fieldset', 'Alt Shape Fieldset'}


def test_get_group_empty(fieldset):
    df = fieldset.get_group('empty-group-id')
    assert len(df) == 0
    assert list(df.columns) == ['type', 'id', 'name', 'description']


def test_get_group_validates_group_id(fieldset):
    with pytest.raises(ValueError):
        fieldset.get_group('')
    with pytest.raises(ValueError):
        fieldset.get_group(None)


# get_fieldset ---------------------------------------------------------------

def test_get_fieldset_schemaitems_shape(fieldset):
    fs = fieldset.get_fieldset('mock-group-id', 'Mock Fieldset')
    assert fs['name'] == 'Mock Fieldset'
    assert fs['group_id'] == 'mock-group-id'
    assert fs['ems_id'] == 1
    df = fs['fields']
    assert list(df.columns) == ['id', 'name', 'type']
    assert df.iloc[0]['id'] == 'field-id-1'
    assert df.iloc[0]['name'] == 'Takeoff Airport Code'
    assert df.iloc[0]['type'] == 'string'
    assert len(df) == 3


def test_get_fieldset_alt_fields_shape(fieldset):
    # Older response shape using 'fields' + id/name/dataType
    fs = fieldset.get_fieldset('mock-group-id', 'Alt Shape Fieldset')
    df = fs['fields']
    assert df.iloc[0]['id'] == 'alt-field-1'
    assert df.iloc[0]['name'] == 'Alt One'
    assert df.iloc[0]['type'] == 'number'
    assert len(df) == 2


def test_get_fieldset_cache(fieldset, monkeypatch):
    fieldset.get_fieldset('mock-group-id', 'Mock Fieldset')
    call_count = {'n': 0}
    real_request = fieldset._conn.request

    def counting_request(*args, **kwargs):
        call_count['n'] += 1
        return real_request(*args, **kwargs)

    monkeypatch.setattr(fieldset._conn, 'request', counting_request)
    fieldset.get_fieldset('mock-group-id', 'Mock Fieldset')
    assert call_count['n'] == 0


# find -----------------------------------------------------------------------

def test_find_unambiguous_match(fieldset):
    hits = fieldset.find('Mock Fieldset')
    assert len(hits) == 1
    assert hits[0]['group_id'] == 'mock-group-id'
    assert hits[0]['name'] == 'Mock Fieldset'
    assert hits[0]['path'] == ['Mock Group']


def test_find_is_case_insensitive(fieldset):
    hits = fieldset.find('mock fieldset')
    assert len(hits) == 1
    assert hits[0]['group_id'] == 'mock-group-id'
    # Canonical casing is preserved in the hit so downstream get_fieldset()
    # calls hit the API with the case the server expects.
    assert hits[0]['name'] == 'Mock Fieldset'
    assert hits[0]['path'] == ['Mock Group']


def test_find_walks_into_subgroups(fieldset):
    hits = fieldset.find('Deep Fieldset')
    assert len(hits) == 1
    assert hits[0]['group_id'] == 'sub-group-id'
    assert hits[0]['path'] == ['Mock Group', 'Sub Group']


def test_find_returns_multiple_for_ambiguous_name(fieldset):
    hits = fieldset.find('Dup Fieldset')
    assert len(hits) == 2
    group_ids = sorted(h['group_id'] for h in hits)
    assert group_ids == ['dup-group-a', 'dup-group-b']


def test_find_skips_permission_gated_subgroups(fieldset, capsys):
    # 'Forbidden Group' raises HTTPError on get_group; the walk should
    # swallow it, print a friendly note, and continue.
    hits = fieldset.find('Mock Fieldset')
    assert len(hits) == 1
    captured = capsys.readouterr()
    assert 'Skipping fieldset group' in captured.out
    assert 'Forbidden Group' in captured.out
    assert 'HTTP 403' in captured.out
    assert 'Continuing search' in captured.out


def test_find_returns_empty_when_not_found(fieldset):
    assert fieldset.find('Nonexistent Fieldset') == []


# clear_cache ----------------------------------------------------------------

def test_clear_cache_empties_all_three(fieldset):
    fieldset.get_groups()
    fieldset.get_group('mock-group-id')
    fieldset.get_fieldset('mock-group-id', 'Mock Fieldset')
    assert fieldset._root_cache is not None
    assert 'mock-group-id' in fieldset._group_cache
    assert ('mock-group-id', 'Mock Fieldset') in fieldset._fieldset_cache

    fieldset.clear_cache()
    assert fieldset._root_cache is None
    assert fieldset._group_cache == {}
    assert fieldset._fieldset_cache == {}


# Parser-level robustness ----------------------------------------------------

def test_parse_fields_handles_bare_strings():
    df = _parse_fieldset_fields(['just-an-id', 'another-id'])
    assert list(df.columns) == ['id', 'name', 'type']
    assert df.iloc[0]['id'] == 'just-an-id'
    assert df.iloc[0]['name'] is None


def test_parse_fields_handles_snake_case():
    df = _parse_fieldset_fields([{'moniker': 'x', 'description': 'X',
                                   'data_type': 'number'}])
    assert df.iloc[0]['type'] == 'number'


def test_parse_fields_empty():
    df = _parse_fieldset_fields([])
    assert list(df.columns) == ['id', 'name', 'type']
    assert len(df) == 0


# FltQuery.select_fieldset integration --------------------------------------

def test_select_fieldset_with_explicit_group(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    queryset = fltquery._FltQuery__queryset
    columns = fltquery._FltQuery__columns

    assert len(queryset['select']) == 3
    assert queryset['select'][0] == {
        'fieldId': 'field-id-1',
        'aggregate': 'none',
    }
    assert len(columns) == 3
    assert columns[0]['id'] == 'field-id-1'
    assert columns[0]['name'] == 'Takeoff Airport Code'
    assert columns[0]['type'] == 'string'


def test_select_fieldset_by_name_walks_tree(fltquery):
    fltquery.select_fieldset('Mock Fieldset')
    queryset = fltquery._FltQuery__queryset
    assert len(queryset['select']) == 3
    assert {e['fieldId'] for e in queryset['select']} == {
        'field-id-1', 'field-id-2', 'field-id-3'
    }


def test_select_fieldset_by_name_is_case_insensitive(fltquery):
    fltquery.select_fieldset('mock fieldset')
    queryset = fltquery._FltQuery__queryset
    assert len(queryset['select']) == 3
    assert {e['fieldId'] for e in queryset['select']} == {
        'field-id-1', 'field-id-2', 'field-id-3'
    }


def test_select_fieldset_ambiguous_name_raises(fltquery):
    with pytest.raises(ValueError) as exc:
        fltquery.select_fieldset('Dup Fieldset')
    msg = str(exc.value)
    assert 'ambiguous' in msg
    assert 'Dup Group A' in msg
    assert 'Dup Group B' in msg


def test_select_fieldset_not_found_raises(fltquery):
    with pytest.raises(ValueError) as exc:
        fltquery.select_fieldset('Nonexistent Fieldset')
    assert 'not found' in str(exc.value)


def test_select_fieldset_with_prefetched_dict(fltquery):
    # Pre-fetch via Fieldset, then pass the dict to select_fieldset.
    fs = Fieldset(fltquery._conn, fltquery._ems_id).get_fieldset(
        'mock-group-id', 'Alt Shape Fieldset'
    )
    fltquery.select_fieldset(fs)
    queryset = fltquery._FltQuery__queryset
    assert len(queryset['select']) == 2
    assert queryset['select'][0]['fieldId'] == 'alt-field-1'


def test_select_fieldset_stacks_with_regular_select(fltquery):
    # Verifies fieldset entries coexist with select() entries in one queryset.
    # Stub a fake select() entry directly rather than going through fieldtree
    # discovery, which is unrelated to what we're testing.
    fltquery._FltQuery__queryset['select'].append({
        'fieldId': 'pre-existing-field',
        'aggregate': 'none',
    })
    fltquery._FltQuery__columns.append({
        'id': 'pre-existing-field', 'name': 'Pre-existing', 'type': 'string'
    })
    pre_len = len(fltquery._FltQuery__queryset['select'])
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    post_len = len(fltquery._FltQuery__queryset['select'])
    assert post_len == pre_len + 3
    # Both kinds of entries are flat {fieldId, aggregate} dicts.
    assert fltquery._FltQuery__queryset['select'][0]['fieldId'] == 'pre-existing-field'


def test_select_fieldset_rejects_bad_type(fltquery):
    with pytest.raises(TypeError):
        fltquery.select_fieldset(12345)


def test_select_fieldset_empty_fieldset_is_noop(fltquery):
    fltquery.select_fieldset('Empty Fieldset', group='mock-group-id')
    assert len(fltquery._FltQuery__queryset['select']) == 0


# Entity-type extraction -----------------------------------------------------

def test_extract_field_entity_type_recognises_shaped_id():
    fid = ("[-hub-][field][[[ems-core][entity-type][foqa-flights]]"
           "[[ems-core][base-field][flight.exact-date]]]")
    assert _extract_field_entity_type(fid) == "[ems-core][entity-type][foqa-flights]"


def test_extract_field_entity_type_handles_dimension_token():
    fid = ("[-hub-][field][[[ems-core][entity-type]"
           "[dimension:096709fb2fb743b1bb3b6bbbf7160c8b]]"
           "[[ems-core][base-field][example]]]")
    assert _extract_field_entity_type(fid) == (
        "[ems-core][entity-type][dimension:096709fb2fb743b1bb3b6bbbf7160c8b]"
    )


def test_extract_field_entity_type_returns_none_for_bare_id():
    assert _extract_field_entity_type("field-id-1") is None


def test_extract_field_entity_type_returns_none_for_non_string():
    assert _extract_field_entity_type(None) is None
    assert _extract_field_entity_type(12345) is None


# Database verification guard -----------------------------------------------

def test_select_fieldset_skips_verification_when_no_database_set(fltquery):
    # The 'fltquery' fixture never calls set_database. The guard must skip
    # silently rather than blowing up when there's nothing to compare to.
    fltquery.select_fieldset('Flights Fieldset', group='real-ids-group-id')
    assert len(fltquery._FltQuery__queryset['select']) == 2


def test_select_fieldset_passes_verification_when_database_matches(
        fltquery_with_flights_db):
    fltquery_with_flights_db.select_fieldset(
        'Flights Fieldset', group='real-ids-group-id'
    )
    assert len(fltquery_with_flights_db._FltQuery__queryset['select']) == 2


def test_select_fieldset_rejects_mismatched_database(fltquery_with_flights_db):
    with pytest.raises(ValueError) as exc:
        fltquery_with_flights_db.select_fieldset(
            'Aircraft Fieldset', group='real-ids-group-id'
        )
    msg = str(exc.value)
    assert "different database" in msg
    assert "foqa-flights" in msg
    assert "aircraft" in msg
    # Nothing was added.
    assert len(fltquery_with_flights_db._FltQuery__queryset['select']) == 0


def test_select_fieldset_rejects_mixed_databases(fltquery_with_flights_db):
    with pytest.raises(ValueError) as exc:
        fltquery_with_flights_db.select_fieldset(
            'Mixed DB Fieldset', group='real-ids-group-id'
        )
    msg = str(exc.value)
    assert "multiple databases" in msg
    assert "foqa-flights" in msg
    assert "aircraft" in msg
    assert len(fltquery_with_flights_db._FltQuery__queryset['select']) == 0


def test_select_fieldset_skips_verification_for_unshaped_ids(
        fltquery_with_flights_db):
    # 'Mock Fieldset' uses bare 'field-id-N' IDs with no extractable entity
    # type. The guard must skip silently and let the select proceed.
    fltquery_with_flights_db.select_fieldset(
        'Mock Fieldset', group='mock-group-id'
    )
    assert len(fltquery_with_flights_db._FltQuery__queryset['select']) == 3


# deselect against fieldset-added fields ------------------------------------

def test_deselect_fieldset_field_by_name(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    assert len(fltquery._FltQuery__queryset['select']) == 3

    fltquery.deselect('Landing Airport Code')

    qs = fltquery._FltQuery__queryset
    cols = fltquery._FltQuery__columns
    assert len(qs['select']) == 2
    assert 'field-id-2' not in {e['fieldId'] for e in qs['select']}
    assert 'field-id-2' not in {c['id'] for c in cols}
    assert {c['id'] for c in cols} == {'field-id-1', 'field-id-3'}


def test_deselect_fieldset_field_by_name_is_case_insensitive(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    fltquery.deselect('landing airport')
    qs = fltquery._FltQuery__queryset
    assert len(qs['select']) == 2
    assert 'field-id-2' not in {e['fieldId'] for e in qs['select']}


def test_deselect_fieldset_field_by_exact_id(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    fltquery.deselect('field-id-2')
    qs = fltquery._FltQuery__queryset
    cols = fltquery._FltQuery__columns
    assert len(qs['select']) == 2
    assert {c['id'] for c in cols} == {'field-id-1', 'field-id-3'}


def test_deselect_unknown_field_raises_with_selection_listed(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    with pytest.raises(ValueError) as exc:
        fltquery.deselect('does-not-exist')
    msg = str(exc.value)
    assert 'no matching field' in msg
    # The error should name what's actually selected so the user can correct
    # the call without printing the queryset themselves.
    assert 'Takeoff Airport Code' in msg
    assert 'field-id-1' in msg
    # Nothing was removed.
    assert len(fltquery._FltQuery__queryset['select']) == 3


def test_deselect_removes_only_matching_field(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    fltquery.deselect('field-id-1')
    fltquery.deselect('field-id-3')
    qs = fltquery._FltQuery__queryset
    cols = fltquery._FltQuery__columns
    assert len(qs['select']) == 1
    assert qs['select'][0]['fieldId'] == 'field-id-2'
    assert len(cols) == 1
    assert cols[0]['id'] == 'field-id-2'


def test_deselect_ambiguous_substring_removes_shortest_by_default(fltquery):
    # 'Mock Fieldset' has 'Takeoff Airport Code' and 'Landing Airport Code';
    # the substring 'airport' matches both. The default (legacy) behaviour
    # is to remove only the shortest-named match (tie -> first encountered).
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    fltquery.deselect('airport')

    qs = fltquery._FltQuery__queryset
    # Two airport fields and one date field were selected; one airport is
    # removed by the default single-match behaviour.
    assert len(qs['select']) == 2
    remaining_ids = {e['fieldId'] for e in qs['select']}
    # field-id-3 (Flight Date) must still be present; exactly one of
    # field-id-1 / field-id-2 must remain.
    assert 'field-id-3' in remaining_ids
    assert len(remaining_ids & {'field-id-1', 'field-id-2'}) == 1


def test_deselect_all_matches_removes_every_substring_hit(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    fltquery.deselect('airport', all_matches=True)

    qs = fltquery._FltQuery__queryset
    remaining_ids = {e['fieldId'] for e in qs['select']}
    # Both airport fields removed, only the date field remains.
    assert remaining_ids == {'field-id-3'}


def test_deselect_rejects_unknown_kwarg(fltquery):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    with pytest.raises(TypeError):
        fltquery.deselect('field-id-1', bogus=True)


def test_deselect_prints_removed_field(fltquery, capsys):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    capsys.readouterr()  # drain the select_fieldset print

    fltquery.deselect('field-id-2')
    out = capsys.readouterr().out
    assert 'Deselected' in out
    assert 'field-id-2' in out
    assert 'Landing Airport Code' in out


def test_deselect_prints_multi_removal_summary(fltquery, capsys):
    fltquery.select_fieldset('Mock Fieldset', group='mock-group-id')
    capsys.readouterr()

    fltquery.deselect('airport', all_matches=True)
    out = capsys.readouterr().out
    assert 'Deselected 2 fields' in out
    assert 'Takeoff Airport Code' in out
    assert 'Landing Airport Code' in out
