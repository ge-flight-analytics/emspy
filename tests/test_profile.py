import sys
import pytest

from emspy.query import Profile
from mock_connection import MockConnection
from mock_ems import MockEMS

if sys.version_info[0] == 2:
    from mock import patch
else:
    from unittest.mock import patch

sys.path.insert(0, '..')


# @patch() decorator replaces an object with a mock object for the test it decorates
# For more information on how to use @patch, see
# https://stackoverflow.com/questions/32461465/python-patch-not-working
@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_excess_profile_matches_by_profile_name():
    with pytest.raises(LookupError):
        ems_system = 'ems24-app'
        connection = MockConnection(user='', pwd='')
        # There are multiple profiles with the name "Duplicate Profile" in the mocked data set.
        profile = Profile(
            connection,
            ems_system,
            profile_name='Duplicate Profile'
        )


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_no_profile_matches_by_profile_name():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    with pytest.raises(LookupError):
        # Obviously a profile that would never exist.
        profile = Profile(
            connection,
            ems_system,
            profile_name='this profile would never exist 1 2 3 4'
        )  # no profile should match.


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_no_profile_matches_by_profile_number():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    with pytest.raises(LookupError):
        # Obviously a profile that would never exist.
        profile = Profile(
            connection,
            ems_system,
            profile_number=1000000000000000000000000000
        )  # no profile should match.


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_one_profile_match_by_profile_name():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    profile = Profile(
        connection,
        ems_system,
        profile_name='Single Real Profile'
    )  # one profile should match.
    assert profile._profile_name == 'Single Real Profile'


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_one_profile_match_by_profile_number():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    profile = Profile(
        connection,
        ems_system,
        profile_number=108
    )  # one profile should match.
    assert profile._profile_name == 'Single Real Profile 2'


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_profile_attributes_by_profile_number():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    profile = Profile(
        connection,
        ems_system,
        profile_number=88
    )  # one profile should match.
    assert profile._profile_name == 'Duplicate Profile'
    assert profile._guid == '5b8dc8cb-c8cb-c8cb-c8cb-c8cb39e6c8cb'
    assert profile._current_version == 9
    assert profile._library == True
    assert profile._local_id == 88


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_profile_attributes_by_profile_name():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    # one profile should match
    profile = Profile(
        connection,
        ems_system,
        profile_name='Single Profile 3'
    )
    assert profile._profile_name == 'Single Profile 3'
    assert profile._guid == 'f163eeee-63ee-63ee-63ee-1b363eed63ee'
    assert profile._current_version == 8
    assert profile._library == True
    assert profile._local_id == 56


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_bad_profile_name_search():
    with pytest.raises(LookupError):
        ems_system = 'ems24-app'
        connection = MockConnection(user='', pwd='')
        profile = Profile(
            connection,
            ems_system,
            profile_name='A PROFILE THAT SHOULD NEVER EXIST'
        )   # no profiles should match.


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_name_search_returns_shortest():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    profile = Profile(
        connection,
        ems_system,
        profile_name='Single Real Profile',
        searchtype='contain'
    )
    assert profile._profile_name == 'Single Real Profile'
