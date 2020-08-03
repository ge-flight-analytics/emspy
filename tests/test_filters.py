import pytest
from mock_connection import MockConnection
from mock_query import MockFltQuery, MockFilterQuery


def get_filter(query):
    return query._FltQuery__queryset['filter']['args'][0]['value']


def test_number_equal():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' == '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'equal'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_notEqual():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' != '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_greaterThan():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' > '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'greaterThan'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_greaterThanOrEqual():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' >= '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'greaterThanOrEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_lessThan():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' < '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'lessThan'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_lessThanOrEqual():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' <= '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'lessThanOrEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_notBetweenExclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' > 'Flight Record' > '17000'")
    queryset_filter = get_filter(query)
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['operator'] == 'notBetweenExclusive'
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_betweenExclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' < 'Flight Record' < '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'betweenExclusive'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_betweenInclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' <= 'Flight Record' <= '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'betweenInclusive'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_notBetweenInclusive():
    query = MockFilterQuery('Flight Record')
    query.filter("'15000' >= 'Flight Record' >= '17000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notBetweenInclusive'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '15000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '17000'
    }


def test_number_in():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' in ['17000', '17001', '17002']")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'in'
    assert len(queryset_filter['args']) == 4
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '17001'
    }
    assert queryset_filter['args'][3] == {
        'type': 'constant',
        'value': '17002'
    }


def test_number_notIn():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' not in ['17000', '17001', '17002']")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notIn'
    assert len(queryset_filter['args']) == 4
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '17000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '17001'
    }
    assert queryset_filter['args'][3] == {
        'type': 'constant',
        'value': '17002'
    }


def test_number_isNull():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' is null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }


def test_number_isNotNull():
    query = MockFilterQuery('Flight Record')
    query.filter("'Flight Record' is not null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNotNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.uid]]]'
    }


def test_dateTime_dateTimeBefore():
    query = MockFilterQuery('Flight Date (Exact)')
    query.filter("'Flight Date (Exact)' < '2020-01-01 00:00:00+00:00'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'dateTimeBefore'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exact-date]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '2020-01-01 00:00:00+00:00'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': 'Utc'
    }


def test_dateTime_dateTimeOnAfter():
    query = MockFilterQuery('Flight Date (Exact)')
    query.filter("'Flight Date (Exact)' >= '2020-01-01 00:00:00+00:00'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'dateTimeOnAfter'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exact-date]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '2020-01-01 00:00:00+00:00'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': 'Utc'
    }


def test_dateTime_dateTimeRange():
    # Not implemented, but it exists as an option in the API
    query = MockFilterQuery('Flight Date (Exact)')
    with pytest.raises(ValueError):
        query.filter(
            "'2019-01-01 00:00:00+00:00' <= 'Flight Date (Exact)' <= '2020-01-01 00:00:00+00:00'"
        )


def test_dateTime_isNull():
    # Not implemented, but it exists as an option in the API
    query = MockFilterQuery('Flight Date (Exact)')
    query.filter("'Flight Date (Exact)' is null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exact-date]]]'
    }


def test_dateTime_isNotNull():
    # Not implemented, but it exists as an option in the API
    query = MockFilterQuery('Flight Date (Exact)')
    query.filter("'Flight Date (Exact)' is not null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNotNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exact-date]]]'
    }


def test_dateTime_unsupported():
    query = MockFilterQuery('Flight Date (Exact)')
    with pytest.raises(ValueError):
        query.filter("'Flight Date (Exact)' == '2020-01-01 00:00:00+00:00'")
    with pytest.raises(ValueError):
        query.filter("'Flight Date (Exact)' != '2020-01-01 00:00:00+00:00'")
    with pytest.raises(ValueError):
        query.filter("'Flight Date (Exact)' > '2020-01-01 00:00:00+00:00'")
    with pytest.raises(ValueError):
        query.filter("'Flight Date (Exact)' <= '2020-01-01 00:00:00+00:00'")
    with pytest.raises(ValueError):
        query.filter(
            "'2019-01-01 00:00:00+00:00' < 'Flight Date (Exact)' < '2020-01-01 00:00:00+00:00'"
        )


def test_boolean_isTrue():
    query = MockFilterQuery('Takeoff Valid')
    query.filter("'Takeoff Valid' == True")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isTrue'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exist-takeoff]]]'
    }


def test_boolean_isNotTrue():
    query = MockFilterQuery('Takeoff Valid')
    query.filter("'Takeoff Valid' != True")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isFalse'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exist-takeoff]]]'
    }



def test_boolean_isFalse():
    query = MockFilterQuery('Takeoff Valid')
    query.filter("'Takeoff Valid' == False")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isFalse'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exist-takeoff]]]'
    }


def test_boolean_isNotFalse():
    query = MockFilterQuery('Takeoff Valid')
    query.filter("'Takeoff Valid' != False")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isTrue'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exist-takeoff]]]'
    }


def test_boolean_isNull():
    query = MockFilterQuery('Takeoff Valid')
    query.filter("'Takeoff Valid' is null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exist-takeoff]]]'
    }


def test_boolean_isNotNull():
    query = MockFilterQuery('Takeoff Valid')
    query.filter("'Takeoff Valid' is not null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNotNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.exist-takeoff]]]'
    }


def test_boolean_unsupported():
    query = MockFilterQuery('Takeoff Valid')
    with pytest.raises(ValueError):
        query.filter("'Takeoff Valid' == '0'")
    with pytest.raises(ValueError):
        query.filter("'Takeoff Valid' == '1'")
    with pytest.raises(ValueError):
        query.filter("'Takeoff Valid' > True")
    with pytest.raises(ValueError):
        query.filter("'Takeoff Valid' < True")
    with pytest.raises(ValueError):
        query.filter("'Takeoff Valid' >= True")
    with pytest.raises(ValueError):
        query.filter("'Takeoff Valid' <= True")
    with pytest.raises(ValueError):
        query.filter("True < 'Takeoff Valid' < False")


def test_string_equal():
    query = MockFilterQuery('Flight Number String')
    query.filter("'Flight Number String' == '000000000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'equal'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.flight-num-str]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '000000000'
    }


def test_string_notEqual():
    query = MockFilterQuery('Flight Number String')
    query.filter("'Flight Number String' != '000000000'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.flight-num-str]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '000000000'
    }


def test_string_in():
    query = MockFilterQuery('Flight Number String')
    query.filter("'Flight Number String' in ['000000000', '00000000_', '0000000__']")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'in'
    assert len(queryset_filter['args']) == 4
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.flight-num-str]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '000000000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '00000000_'
    }
    assert queryset_filter['args'][3] == {
        'type': 'constant',
        'value': '0000000__'
    }


def test_string_notIn():
    query = MockFilterQuery('Flight Number String')
    query.filter("'Flight Number String' not in ['000000000', '00000000_', '0000000__']")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notIn'
    assert len(queryset_filter['args']) == 4
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.flight-num-str]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': '000000000'
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': '00000000_'
    }
    assert queryset_filter['args'][3] == {
        'type': 'constant',
        'value': '0000000__'
    }


def test_string_isNull():
    query = MockFilterQuery('Flight Number String')
    query.filter("'Flight Number String' is null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.flight-num-str]]]'
    }


def test_string_isNotNull():
    query = MockFilterQuery('Flight Number String')
    query.filter("'Flight Number String' is not null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNotNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.flight-num-str]]]'
    }


def test_string_unsupported():
    query = MockFilterQuery('Flight Number String')
    with pytest.raises(ValueError):
        query.filter("'Flight Number String' > '000000000'")
    with pytest.raises(ValueError):
        query.filter("'Flight Number String' < '000000000'")
    with pytest.raises(ValueError):
        query.filter("'Flight Number String' >= '000000000'")
    with pytest.raises(ValueError):
        query.filter("'Flight Number String' <= '000000000'")
    with pytest.raises(ValueError):
        query.filter("'000000000' < 'Flight Number String' < '000000000'")
    with pytest.raises(ValueError):
        query.filter("'000000000' <= 'Flight Number String' <= '000000000'")
    with pytest.raises(ValueError):
        query.filter("'000000000' > 'Flight Number String' > '000000000'")
    with pytest.raises(ValueError):
        query.filter("'000000000' >= 'Flight Number String' >= '000000000'")


def test_discrete_equal():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' == 'High'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'equal'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 2
    }


def test_discrete_notEqual():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' != 'Low'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 1
    }


def test_discrete_greaterThan():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' > 'Low'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'greaterThan'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 1
    }


def test_discrete_greaterThanOrEqual():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' >= 'High'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'greaterThanOrEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 2
    }


def test_discrete_lessThan():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' < 'High'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'lessThan'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 2
    }


def test_discrete_lessThanOrEqual():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' <= 'Low'")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'lessThanOrEqual'
    assert len(queryset_filter['args']) == 2
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 1
    }


def test_discrete_in():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' in ['High', 'Low']")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'in'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 2
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': 1
    }


def test_discrete_notIn():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' not in ['Unknown', 'Low']")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'notIn'
    assert len(queryset_filter['args']) == 3
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }
    assert queryset_filter['args'][1] == {
        'type': 'constant',
        'value': 0
    }
    assert queryset_filter['args'][2] == {
        'type': 'constant',
        'value': 1
    }


def test_discrete_isNull():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' is null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }


def test_discrete_isNotNull():
    query = MockFilterQuery('Flight Date Confidence')
    query.filter("'Flight Date Confidence' is not null")
    queryset_filter = get_filter(query)
    assert queryset_filter['operator'] == 'isNotNull'
    assert len(queryset_filter['args']) == 1
    assert queryset_filter['args'][0] == {
        'type': 'field',
        'value': '[-hub-][field][[[ems-core][entity-type][foqa-flights]]'
                 '[[ems-core][base-field][flight.date-confidence]]]'
    }


def test_discrete_unsupported():
    query = MockFilterQuery('Flight Date Confidence')
    with pytest.raises(ValueError):
        query.filter("'Unknown' < 'Flight Date Confidence' < 'High'")
