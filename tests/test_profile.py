import sys
sys.path.insert(0, '..')
from emspy.query import Profile
from emspy import Connection
from unittest.mock import patch
from mock_connection import MockConnection
from mock_query import MockQuery
from mock_ems import MockEMS

sys = 'ems24-app'

user_name = ''
pwd = ''
proxies = {
    'http': '',
    'https': ''
}

c = MockConnection(user=user_name, pwd=pwd, proxies=proxies)


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_excess_profile_matches_by_profile_name():
    try:
        # There are multiple profiles with the exact name "Block-Cost Model Planned Fuel Components" in the mocked
        # data set.
        p = Profile(c, sys, profile_name='Duplicate Profile')
        assert 0  # should never get here.
    except LookupError:
        assert 1


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_no_profile_matches_by_profile_name():
    try:
        # Obviously a profile that would never exist.
        p = Profile(c, sys, profile_name='this profile would never exist 1 2 3 4')  # no profile should match.
        assert 0  # should never get here.
    except LookupError:
        assert 1


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_no_profile_matches_by_profile_number():
    try:
        # Obviously a profile that would never exist.
        p = Profile(c, sys, profile_number=1000000000000000000000000000)  # no profile should match.
        assert 0  # should never get here.
    except LookupError:
        assert 1


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_one_profile_match_by_profile_name():
    p = Profile(c, sys, profile_name='Single Real Profile')  # no profile should match.
    assert (p._profile_name == 'Single Real Profile')


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_one_profile_match_by_profile_number():
    p = Profile(c, sys, profile_number=108)  # no profile should match.
    assert (p._profile_name == 'Single Real Profile 2')


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_profile_attributes_by_profile_number():
    p = Profile(c, sys, profile_number=88)  # one profile should match.
    assert (p._profile_name == 'Duplicate Profile')
    assert (p._guid == '5b8dc8cb-c8cb-c8cb-c8cb-c8cb39e6c8cb')
    assert (p._current_version == 9)
    assert (p._library == True)
    assert (p._local_id == 88)


@patch('emspy.query.ems.EMS.get_id', new=MockEMS.get_id)
@patch('emspy.Connection.request', new=MockConnection.request)
def test_profile_attributes_by_profile_name():
    p = Profile(c, sys, profile_name='Single Profile 3')  # one profile should match.
    assert (p._profile_name == 'Single Profile 3')
    assert (p._guid == 'f163eeee-63ee-63ee-63ee-1b363eed63ee')
    assert (p._current_version == 8)
    assert (p._library == True)
    assert (p._local_id == 56)
