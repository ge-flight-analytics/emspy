import os
import pytest
from mock_connection import MockConnection
from mock_query import MockFltQuery

test_path = os.path.dirname(os.path.realpath(__file__))

FLIGHT_RECORD_ID = (
    '[-hub-][field]'
    '[[[ems-core][entity-type][foqa-flights]]'
    '[[ems-core][base-field][flight.uid]]]'
)

ENGINE_SERIES_ID = (
    '[-hub-][field]'
    '[[[ems-core][entity-type][foqa-flights]]'
    '[[airframe-engine-field-set][base-field][engine-series-2]]]'
)


def make_query():
    """Create a MockFltQuery with fieldtree populated."""
    connection = MockConnection(user='', pwd='')
    query = MockFltQuery(connection, 'ems24-app',
                         data_file=os.path.join(test_path, 'mock_metadata.db'))
    query.set_database('FDW Flights')
    query.update_fieldtree(
        'Flight Information',
        exclude_tree=['Processing', 'Date Times', 'FlightPulse']
    )
    query.update_fieldtree(
        'Aircraft Information',
        exclude_tree=['Airframe Information', 'Engine Information',
                      'Fleet Information']
    )
    return query


def test_select_id_single():
    """select_id adds a single field id to the queryset."""
    query = make_query()
    query.select_id(FLIGHT_RECORD_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 1
    assert qs['select'][0]['fieldId'] == FLIGHT_RECORD_ID
    assert qs['select'][0]['aggregate'] == 'none'


def test_select_id_multiple():
    """select_id accepts multiple field ids at once."""
    query = make_query()
    query.select_id(FLIGHT_RECORD_ID, ENGINE_SERIES_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 2
    assert qs['select'][0]['fieldId'] == FLIGHT_RECORD_ID
    assert qs['select'][1]['fieldId'] == ENGINE_SERIES_ID


def test_select_id_aggregate():
    """select_id supports the aggregate keyword."""
    query = make_query()
    query.select_id(FLIGHT_RECORD_ID, aggregate='avg')
    qs = query.in_dict()
    assert qs['select'][0]['aggregate'] == 'avg'


def test_select_id_invalid_aggregate():
    """select_id exits on invalid aggregate."""
    query = make_query()
    with pytest.raises(SystemExit):
        query.select_id(FLIGHT_RECORD_ID, aggregate='invalid')


def test_select_id_cached():
    """Field ids already in the fieldtree are resolved from cache, not API."""
    query = make_query()
    # FLIGHT_RECORD_ID is in fieldtree after update_fieldtree
    query.select_id(FLIGHT_RECORD_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 1
    assert qs['select'][0]['fieldId'] == FLIGHT_RECORD_ID


def test_select_id_api_lookup():
    """Field ids not in fieldtree are resolved via the API and cached."""
    connection = MockConnection(user='', pwd='')
    query = MockFltQuery(connection, 'ems24-app',
                         data_file=os.path.join(test_path, 'mock_metadata.db'))
    query.set_database('FDW Flights')
    # Don't call update_fieldtree so fieldtree is empty;
    # resolve_id should fall back to the API
    query.select_id(FLIGHT_RECORD_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 1
    assert qs['select'][0]['fieldId'] == FLIGHT_RECORD_ID


def test_select_id_combines_with_select():
    """select_id and select can be used together."""
    query = make_query()
    query.select('Flight Record')
    query.select_id(ENGINE_SERIES_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 2
    assert qs['select'][0]['fieldId'] == FLIGHT_RECORD_ID
    assert qs['select'][1]['fieldId'] == ENGINE_SERIES_ID


def test_deselect_after_select_id_cached():
    """deselect removes a field that was added via select_id (cached path)."""
    query = make_query()
    query.select_id(FLIGHT_RECORD_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 1

    query.deselect('Flight Record')
    qs = query.in_dict()
    assert len(qs['select']) == 0


def test_deselect_after_select_id_api():
    """deselect removes a field added via select_id's API fallback path.

    The fieldtree is left empty - deselect must match against the current
    selection directly (by name in this case), without consulting the
    fieldtree.
    """
    connection = MockConnection(user='', pwd='')
    query = MockFltQuery(connection, 'ems24-app',
                         data_file=os.path.join(test_path, 'mock_metadata.db'))
    query.set_database('FDW Flights')
    # Empty fieldtree forces resolve_id to use the API fallback,
    # which produces a dict with parent_id=None
    query.select_id(FLIGHT_RECORD_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 1

    query.deselect('Flight Record')
    qs = query.in_dict()
    assert len(qs['select']) == 0


def test_deselect_by_exact_field_id():
    """deselect accepts the exact field id, no name lookup needed."""
    connection = MockConnection(user='', pwd='')
    query = MockFltQuery(connection, 'ems24-app',
                         data_file=os.path.join(test_path, 'mock_metadata.db'))
    query.set_database('FDW Flights')
    query.select_id(FLIGHT_RECORD_ID)
    query.deselect(FLIGHT_RECORD_ID)
    qs = query.in_dict()
    assert len(qs['select']) == 0
