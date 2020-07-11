import os
from builtins import object
from emspy.query import FltQuery
from mock_connection import MockConnection


test_path = os.path.dirname(os.path.realpath(__file__))


class MockQuery(object):
    def __init__(self, conn, ems_name):
        self._conn 		= conn
        self._ems_name  = ems_name
        self.__ems 		= None
        self.__fleet 	= None
        self.__aircraft = None
        self.__airport 	= None
        self.__flightphase = None
        self._ems_id 	= self.__ems.get_id(ems_name)

    def get_ems_id(self):
        return 3


class MockFltQuery(FltQuery):
    def __init__(self, conn, ems_name, data_file):
        self._conn 		= conn
        self._ems_name  = ems_name
        self.__ems 		= None
        self.__fleet 	= None
        self.__aircraft = None
        self.__airport 	= None
        self.__flightphase = None
        self._ems_id 	= 1
        self._init_assets(data_file)
        self.reset()


def MockFilterQuery(field, dbname=os.path.join(test_path, 'mock_metadata.db')):
    sys = 'ems24-app'
    c = MockConnection(user='', pwd='')
    query = MockFltQuery(c, sys, data_file=dbname)
    query.set_database('FDW Flights')
    query.update_fieldtree(
        'Aircraft Information',
        exclude_tree=[
            'Airframe Information',
            'Engine Information',
            'Fleet Information'
        ]
    )
    query.update_fieldtree(
        'Flight Information',
        exclude_tree=[
            'Processing',
            'Date Times',
            'FlightPulse'
        ]
    )
    query.select(field)
    return query