from builtins import object
from emspy.query import *


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
