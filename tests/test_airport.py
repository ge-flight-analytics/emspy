import pytest
from emspy.query import Airport
from mock_connection import MockConnection
from mock_ems import MockEMS
import pandas as pd

@pytest.fixture(scope='session')
def query():
    c = MockConnection(user='', pwd='')
    ems = MockEMS(c)
    AirportQuery = Airport(c, ems.get_id())
    return AirportQuery


def test_list_all(query):
    assert isinstance(query.list_all(), pd.DataFrame)


def test_data_colnames(query):
    expected_colnames = {
        'city',
        'codeFaa',
        'codeIata',
        'codeIcao',
        'codePreferred',
        'country',
        'elevation',
        'id',
        'latitude',
        'longitude',
        'name'
    }
    assert set(query.data_colnames()) == expected_colnames


def test_search_contain(query):
    search = query.search('name', val='Mock Airport', searchtype='contain')
    assert isinstance(search, pd.DataFrame)
    assert all(search['name'].str.contains('Mock Airport'))


def test_search_match(query):
    search = query.search('name', val='Mock Airport 1', searchtype='match')
    assert isinstance(search, pd.DataFrame)
    assert search.shape[0] == 1
    assert search['name'].iloc[0] == 'Mock Airport 1'


def test_get_id_default(query):
    airport_id = query.get_id('XXXX')
    assert airport_id == 1
    airport_id = query.get_id('ZZZZ', code='default')
    assert airport_id == 3


def test_get_id_faa(query):
    airport_id = query.get_id('YYY', code='faa')
    assert airport_id == 2


def test_get_id_icao(query):
    airport_id = query.get_id('XXXX', code='icao')
    assert airport_id == 1


def test_get_id_city(query):
    airport_id = query.get_id('Mock City 1', code='city')
    assert airport_id == [1, 2]


def test_get_id_name(query):
    airport_id = query.get_id('Mock Airport 3', code='name')
    assert airport_id == 3


def test_get_name_default(query):
    airport_default = query.get_name(2)
    assert airport_default == 'YYYY'
    airport_default = query.get_name(3, code='default')
    assert airport_default == 'ZZZZ'


def test_get_name_faa(query):
    airport_faa = query.get_name(1, code='faa')
    assert airport_faa == 'XXX'


def test_get_name_icao(query):
    airport_icao = query.get_name(2, code='icao')
    assert airport_icao == 'YYYY'


def test_get_name_city(query):
    airport_city = query.get_name(1, code='city')
    assert airport_city == 'Mock City 1'


def test_get_name_name(query):
    airport_name = query.get_name(3, code='name')
    assert airport_name == 'Mock Airport 3'
