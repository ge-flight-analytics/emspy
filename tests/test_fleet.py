import pytest
from emspy.query import Fleet
from mock_connection import MockConnection
from mock_ems import MockEMS
import pandas as pd


@pytest.fixture(scope='session')
def query():
    c = MockConnection(user='', pwd='')
    ems = MockEMS(c)
    FleetQuery = Fleet(c, ems.get_id())
    return FleetQuery


def test_data_colnames(query):
    expected_colnames = {
        'id',
        'name'
    }
    assert set(query.data_colnames()) == expected_colnames


def test_list_all(query):
    assert isinstance(query.list_all(), pd.DataFrame)


def test_search_contain(query):
    search = query.search('name', val='Fleet', searchtype='contain')
    assert isinstance(search, pd.DataFrame)
    assert all(search['name'].str.contains('Fleet'))


def test_search_match(query):
    search = query.search('name', val='UNKNOWN', searchtype='match')
    assert isinstance(search, pd.DataFrame)
    assert search.shape[0] == 1
    assert search['name'].iloc[0] == 'UNKNOWN'


def test_get_id(query):
    fleet_id = query.get_id('Mock Fleet')
    assert fleet_id == 1


def test_get_name(query):
    fleet_name = query.get_name(1)
    assert fleet_name == 'Mock Fleet'