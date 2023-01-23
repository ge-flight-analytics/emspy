import emspy, os, sys, pytest
from emspy.query import LocalData
from mock_connection import MockConnection
from mock_ems import MockEMS
from urllib.error import HTTPError


def get_test_db_path():
    tests_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(tests_dir, 'emsMetaData.db')


@pytest.fixture
def mocks(monkeypatch):
    monkeypatch.setattr(emspy.query.query, 'EMS', MockEMS)
    monkeypatch.setattr(emspy, 'Connection', MockConnection)


@pytest.fixture
def tsq(mocks):
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    data_file = get_test_db_path()
    return emspy.query.TSeriesQuery(connection, ems_system, data_file)


@pytest.fixture
def tsq_no_db(mocks):
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    return emspy.query.TSeriesQuery(connection, ems_system, data_file=None)


# QUERYSET TESTS
def test_analytic_ids_in_queryset_using_select_ids_no_lookup(tsq):
    analytic_ids = ['asdfasdf', 'asdfasdf']
    names = ['one', 'two']

    tsq.select_ids(analytic_ids, names)
    queryset = tsq.get_queryset()

    selected_analytic_id_list = [entry['analyticId'] for entry in queryset['select']]
    for analytic_id in analytic_ids:
        assert analytic_id in selected_analytic_id_list

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


def test_analytic_id_in_queryset_using_select_ids_with_lookup(tsq):
    analytic_ids = ['fake-bar-alt-id-that-exists=', 'fake-pressure-alt-id-that-exists=']
    names = ['Baro-Corrected Altitude (ft)', 'Pressure Altitude (ft)']

    tsq.select_ids(analytic_ids, names, lookup=True)
    queryset = tsq.get_queryset()

    selected_analytic_id_list = [entry['analyticId'] for entry in queryset['select']]
    for analytic_id in analytic_ids:
        assert analytic_id in selected_analytic_id_list

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


# LOOKUP TESTS
def test_analytic_id_with_lookup_using_select_ids_adds_units(tsq):
    ems_id = 24
    analytic_id = 'fake-bar-alt-id-that-exists='
    name = 'Baro-Corrected Altitude (ft)'
    units_assert = 'ft'

    tsq.select_ids(analytic_id, name, lookup=True)

    df = tsq._TSeriesQuery__analytic._param_table
    units = df.loc[(df['ems_id'] == ems_id) & (df['id'] == analytic_id), 'units'].values[0]
    assert units == units_assert

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


def test_analytic_id_with_lookup_using_select_ids_adds_description(tsq):
    ems_id = 24
    analytic_id = 'fake-bar-alt-id-that-exists='
    name = 'Baro-Corrected Altitude (ft)'
    description_assert = 'Altitude above mean sea level (MSL)'

    tsq.select_ids(analytic_id, name, lookup=True)

    df = tsq._TSeriesQuery__analytic._param_table
    description = df.loc[(df['ems_id'] == ems_id)
                         & (df['id'] == analytic_id), 'description'].values[0]
    assert description == description_assert

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


def test_analytic_id_with_lookup_using_select_ids_adds_name(tsq):
    ems_id = 24
    analytic_id = 'fake-bar-alt-id-that-exists='
    name_assert = 'Baro-Corrected Altitude (ft)'

    tsq.select_ids(analytic_id, lookup=True)

    df = tsq._TSeriesQuery__analytic._param_table
    name = df.loc[(df['ems_id'] == ems_id) & (df['id'] == analytic_id), 'name'].values[0]
    assert name == name_assert

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


def test_select_from_pset_has_correct_ids(tsq):
    expected_analytic_ids = ['fake-pres-alt-id-that-exists=','fake-rad-alt-id-that-exists=','fake-gear_height-id-that-exists=']
    expected_analytic_names = ['Pressure Altitude (ft)','Radio Altitude (1, left, Capt or Only) (ft)','Best Estimate of Main Gear Height AGL (ft)']
    tsq.select_from_pset('\path\set')
    selects = tsq._TSeriesQuery__queryset['select']
    columns = tsq._TSeriesQuery__columns
    for id in selects:
        assert id['analyticId'] in expected_analytic_ids
    for column in columns:
        assert column['name'] in expected_analytic_names

    # destroy
    tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


# INCORRECT ID TESTS
def test_analytic_id_doesnt_exist_with_lookup_using_select_ids(tsq):
    analytic_id = 'fake-pressure-alt-id-that-DOES-NOT-exist='
    name = 'pressure altitude?'

    try:
        tsq.select_ids(analytic_id, name, lookup=True)
        assert False
    except HTTPError:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


# PARAMETER EDGE CASE TESTS
def test_analytic_id_returns_error_using_select_ids_with_no_names_and_no_lookup(tsq):
    analytic_id = 'fake-bar-alt-id-that-exists='

    try:
        tsq.select_ids(analytic_id)
        assert False
    except ValueError:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


def test_passing_diffet_length_lists_to_select_ids(tsq):
    # Set up with two analytic_ids but only one name.
    analytic_ids = ['fake-bar-alt-id-that-exists=', 'fake-pressure-alt-id-that-exists=']
    names = ['Baro-Corrected Altitude (ft)']

    try:
        tsq.select_ids(analytic_ids, names, lookup=True)
        assert False  # Should not be able to select ids with different length lists
    except ValueError:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


def test_passing_mix_of_strings_and_lists_to_select_ids(tsq):
    # Set up with two analytic_ids but only one name.
    analytic_ids = 'fake-bar-alt-id-that-exists='
    names = ['Baro-Corrected Altitude (ft)', 'Baro-Corrected Altitude 2 (ft)']

    try:
        tsq.select_ids(analytic_ids, names, lookup=True)
        assert False  # Should not be able to select ids with different length lists
    except TypeError:
        assert True
    finally:
        # destroy
        tsq._TSeriesQuery__analytic._metadata.delete_all_tables()


# DB FILE TESTS
def test_db_file_not_created(tsq_no_db):
    # Delete the default db file location if it exists.
    if os.path.exists(LocalData.default_data_file):
        os.remove(LocalData.default_data_file)

    # Run the select_ids test with the database file set to None
    test_analytic_ids_in_queryset_using_select_ids_no_lookup(tsq_no_db)

    # Make sure the default data file was not created.
    assert os.path.exists(LocalData.default_data_file) is False
