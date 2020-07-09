from builtins import object
from emspy.query import FltQuery

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