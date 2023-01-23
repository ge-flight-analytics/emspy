import emspy, os, sys, pytest
from emspy.query import LocalData
from mock_connection import MockConnection
from mock_ems import MockEMS
from urllib.error import HTTPError


@pytest.fixture
def mocks(monkeypatch):
    monkeypatch.setattr(emspy.query.query, 'EMS', MockEMS)
    monkeypatch.setattr(emspy, 'Connection', MockConnection)


@pytest.fixture
def analyticset(mocks):
    ems_id = 1
    connection = MockConnection(user='', pwd='')
    return emspy.query.AnalyticSet(connection, ems_id)

# PATH TESTS
def test_get_analytic_set_with_path_but_no_set(analyticset):
    with pytest.raises(ValueError):
        analyticset.get_analytic_set('path\to\set\\')

def test_get_analytic_set_for_root_with_root_path(analyticset):
    analyticset.get_analytic_set('root\set')
    assert analyticset._group_id == 'root'
    assert analyticset._analytic_set_id == 'set'

def test_get_analytic_set_for_root_with_blank_path(analyticset):
    analyticset.get_analytic_set("\set")
    assert analyticset._group_id == 'root'
    assert analyticset._analytic_set_id == 'set'

def test_get_analytic_set_for_root_with_parameter_sets_path(analyticset):
    analyticset.get_analytic_set('parameter sets\set')
    assert analyticset._group_id == 'root'
    assert analyticset._analytic_set_id == 'set'

def test_get_analytic_set_using_backslashes(analyticset):
    analyticset.get_analytic_set('\path\set')
    assert analyticset._group_id == 'path'
    assert analyticset._analytic_set_id == 'set'

def test_get_analytic_set_using_backslashes_with_special_characters(analyticset):
    analyticset.get_analytic_set('\folder\aet')
    assert analyticset._group_id == 'folder'
    assert analyticset._analytic_set_id == 'aet'

def test_get_analytic_set_using_backslashes(analyticset):
    analyticset.get_analytic_set('/folder/set')
    assert analyticset._group_id == 'folder'
    assert analyticset._analytic_set_id == 'set'

def test_get_analytic_set_with_longer_path(analyticset):
    analyticset.get_analytic_set('/folder1\folder2/folder3\set')
    assert analyticset._group_id == 'folder1:folder2:folder3'
    assert analyticset._analytic_set_id == 'set'

def test_get_group_content_with_no_path(analyticset):
    analyticset.get_group_content('root')
    assert analyticset._analytic_set_path == 'root'

def test_get_group_content_with_path(analyticset):
    analyticset.get_group_content('\folder 1\folder 2\\')
    assert analyticset._analytic_set_path == r'\folder 1\folder 2'+'\\'


# QUERY TESTS

def test_get_analytic_set_retieved_has_right_name(analyticset):
    analyticset.get_analytic_set('\path\mock set')
    assert analyticset._analytic_set_name == 'mock set'

def test_get_analytic_set_set_does_not_exist(analyticset):
    analyticset.get_analytic_set('\path\A PARAMETER SET THAT DOES NOT EXIST')
    assert analyticset._analytic_set_name == None
    assert analyticset._analytic_set_description == None

def test_get_analytic_set_retieved_has_right_data(analyticset):
    df = analyticset.get_analytic_set('\path\mock set')
    assert len(list(df)) == len(analyticset._AnalyticSet__analytic_set_columns) # Checking the right number of columns
    assert len(df) == 3 # Expect 3 rows from the mock

def test_get_group_content_retieved_has_right_data(analyticset):
    group_dict = analyticset.get_group_content('\path\\')
    assert list(group_dict) == ['groups', 'sets']
    assert len(group_dict['groups']) == 2 # Expect 2 groups from the mock
    assert len(group_dict['sets']) == 2 # Expect 2 sets from the mock