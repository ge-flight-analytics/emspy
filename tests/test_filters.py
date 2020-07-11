import pytest
from mock_connection import MockConnection
from mock_query import MockFltQuery, MockFilterQuery


def get_filter(query):
    return query._FltQuery__queryset['filter']['args'][0]['value']


def test_equal():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' == '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'equal'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }

def test_notEqual():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' != '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'notEqual'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }

def test_greaterThan():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' > '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'greaterThan'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }

def test_greaterThanOrEqual():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' >= '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'greaterThanOrEqual'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }

def test_lessThan():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' < '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'lessThan'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }

def test_lessThanOrEqual():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' <= '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'lessThanOrEqual'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }

def test_notBetweenExclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' > 'Flight Record' > '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'notBetweenExclusive'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }

def test_betweenExclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' < 'Flight Record' < '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'betweenExclusive'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }

def test_betweenInclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' <= 'Flight Record' <= '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'betweenInclusive'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }

def test_notBetweenInclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' >= 'Flight Record' >= '17000'")
    filter = get_filter(query)
    assert filter['operator'] == 'notBetweenInclusive'
    assert filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.uid]]]'

    }
    assert filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }

def test_in():
    query = MockFilterQuery('Flight Record')
    with pytest.raises(ValueError):
        query.filter("'Flight Record' in ['17000', '17001', '17002']")

def test_notIn():
    query = MockFilterQuery('Flight Record')
    with pytest.raises(ValueError):
        query.filter("'Flight Record' not in ['17000', '17001', '17002']")
