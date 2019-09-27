from emspy.query import TSeriesQuery
from unittest.mock import patch
from mock_connection import MockConnection
from mock_ems import MockEMS

from urllib.error import HTTPError

import sys
sys.path.insert(0, '..')



'''
QUERYSET TESTS
'''

@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_ids_in_queryset_using_select_ids_no_lookup():
    sys = 'ems24-app'
    c = MockConnection(user='', pwd='')

    tsq = TSeriesQuery(c, 'ems24-app')
    analytic_ids = ['asdfasdf', 'asdfasdf']
    names = ['one', 'two']

    tsq.select_ids(analytic_ids, names)
    queryset = tsq.get_queryset()

    selected_analytic_id_list = [entry['analyticId'] for entry in queryset['select']]
    for analytic_id in analytic_ids:
        assert analytic_id in selected_analytic_id_list

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_id_in_queryset_using_select_ids_with_lookup():
    sys = 'ems24-app'
    analytic_ids = ['fake-bar-alt-id-that-exists=', 'fake-pressure-alt-id-that-exists=']
    names = ['Baro-Corrected Altitude (ft)', 'Pressure Altitude (ft)']
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    tsq.select_ids(analytic_ids, names, lookup=True)
    queryset = tsq.get_queryset()

    selected_analytic_id_list = [entry['analyticId'] for entry in queryset['select']]
    for analytic_id in analytic_ids:
        assert analytic_id in selected_analytic_id_list

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


'''
LOOKUP TESTS
'''


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_id_with_lookup_using_select_ids_adds_units():
    sys = 'ems24-app'
    ems_id = 24
    analytic_id = 'fake-bar-alt-id-that-exists='
    name = 'Baro-Corrected Altitude (ft)'
    units_assert = 'ft'
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    tsq.select_ids(analytic_id, name, lookup=True)

    df = tsq._TSeriesQuery__analytic._param_table
    units = df.loc[(df['ems_id'] == ems_id) & (df['id'] == analytic_id), 'units'].values[0]
    assert units == units_assert

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_id_with_lookup_using_select_ids_adds_description():
    sys = 'ems24-app'
    ems_id = 24
    analytic_id = 'fake-bar-alt-id-that-exists='
    name = 'Baro-Corrected Altitude (ft)'
    description_assert = 'The altitude above mean sea level (MSL) obtained by applying corrections to the pressure ' \
                         'altitude using the altimeter setting.  This parameter may have discrete jumps during these ' \
                         'corrections.'
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    tsq.select_ids(analytic_id, name, lookup=True)

    df = tsq._TSeriesQuery__analytic._param_table
    description = df.loc[(df['ems_id'] == ems_id) & (df['id'] == analytic_id), 'description'].values[0]
    assert description == description_assert

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_id_with_lookup_using_select_ids_adds_name():
    sys = 'ems24-app'
    ems_id = 24
    analytic_id = 'fake-bar-alt-id-that-exists='
    name_assert = 'Baro-Corrected Altitude (ft)'
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    tsq.select_ids(analytic_id, lookup=True)

    df = tsq._TSeriesQuery__analytic._param_table
    name = df.loc[(df['ems_id'] == ems_id) & (df['id'] == analytic_id), 'name'].values[0]
    assert name == name_assert

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


'''
INCORRECT ID TESTS
'''


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_id_doesnt_exist_with_lookup_using_select_ids():
    sys = 'ems24-app'
    analytic_id = 'fake-pressure-alt-id-that-DOES-NOT-exist='
    name = 'pressure altitude?'
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    try:
        tsq.select_ids(analytic_id, name, lookup=True)
        assert False
    except HTTPError as e:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


'''
PARAMETER EDGE CASE TESTS
'''

@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_analytic_id_returns_error_using_select_ids_with_no_names_and_no_lookup():
    sys = 'ems24-app'
    analytic_id = 'fake-bar-alt-id-that-exists='
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    try:
        tsq.select_ids(analytic_id)
        assert False
    except ValueError as e:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_passing_different_length_lists_to_select_ids():
    # Set up with two analytic_ids but only one name.
    sys = 'ems24-app'
    analytic_ids = ['fake-bar-alt-id-that-exists=', 'fake-pressure-alt-id-that-exists=']
    names = ['Baro-Corrected Altitude (ft)']
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    try:
        tsq.select_ids(analytic_ids, names, lookup=True)
        assert False  # Should not be able to select ids with different length lists
    except ValueError as e:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_passing_mix_of_strings_and_lists_to_select_ids():
    # Set up with two analytic_ids but only one name.
    sys = 'ems24-app'
    analytic_ids = 'fake-bar-alt-id-that-exists='
    names = ['Baro-Corrected Altitude (ft)', 'Baro-Corrected Altitude 2 (ft)']
    c = MockConnection(user='', pwd='')
    tsq = TSeriesQuery(c, 'ems24-app')

    try:
        tsq.select_ids(analytic_ids, names, lookup=True)
        assert False  # Should not be able to select ids with different length lists
    except TypeError as e:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()
