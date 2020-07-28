import pytest
from emspy.query import FlightPhase
from mock_connection import MockConnection
from mock_ems import MockEMS
import pandas as pd


@pytest.fixture(scope='session')
def query():
    c = MockConnection(user='', pwd='')
    FlightPhaseQuery = FlightPhase(c)
    return FlightPhaseQuery


def test_data_colnames(query):
    expected_colnames = {
        'id',
        'name'
    }
    assert set(query.data_colnames()) == expected_colnames


def test_list_all(query):
    assert isinstance(query.list_all(), pd.DataFrame)


def test_search_contain(query):
    search = query.search('name', val='Taxi', searchtype='contain')
    assert isinstance(search, pd.DataFrame)
    assert all(search['name'].str.contains('Taxi'))


def test_search_match(query):
    search = query.search('name', val='unknown state', searchtype='match')
    assert isinstance(search, pd.DataFrame)
    assert search.shape[0] == 1
    assert search['name'].iloc[0] == 'unknown state'


def test_get_id(query):
    flightphase_id = query.get_id('Taxi Out')
    assert flightphase_id == 1


def test_get_name(query):
    flightphase_name = query.get_name(2)
    assert flightphase_name == 'C) Takeoff'
