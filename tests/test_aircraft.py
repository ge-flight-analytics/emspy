import pytest
from emspy.query import Aircraft
from mock_connection import MockConnection
from mock_ems import MockEMS
import pandas as pd


@pytest.fixture(scope='session')
def query():
    c = MockConnection(user='', pwd='')
    ems = MockEMS(c)
    AircraftQuery = Aircraft(c, ems.get_id())
    return AircraftQuery


def test_list_all(query):
    assert isinstance(query.list_all(), pd.DataFrame)


def test_data_colnames(query):
    expected_colnames = {
        'id',
        'fleetIds',
        'isActive',
        'isApproved',
        'name'
    }
    assert set(query.data_colnames()) == expected_colnames


def test_search_contain(query):
    search = query.search('name', val='00000', searchtype='contain')
    assert isinstance(search, pd.DataFrame)
    assert all(search['name'].str.contains('00000'))


def test_search_match(query):
    search = query.search('name', val='UNKNOWN', searchtype='match')
    assert isinstance(search, pd.DataFrame)
    assert search.shape[0] == 1
    assert search['name'].iloc[0] == 'UNKNOWN'


def test_get_id(query):
    aircraft_id = query.get_id('000001')
    assert aircraft_id == 1


def test_get_name(query):
    aircraft_name = query.get_name(2)
    assert aircraft_name == '000002'


def test_search_by_fleetid(query):
    aircraft_ids = query.search_by_fleetid(2)
    assert isinstance(aircraft_ids, pd.DataFrame)
    assert aircraft_ids.shape[0] == 3
    assert all(aircraft_ids['id'].values == [1, 2, 5])
