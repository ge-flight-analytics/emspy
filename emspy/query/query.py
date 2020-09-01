from builtins import object
from emspy.query import *


class Query(object):
	def __init__(self, conn, ems_name):
		"""
		Query initialization

		Parameters
		----------
		conn: empsy.connection.Connection
			connection object
		ems_name: str
			EMS system name
		"""
		self._conn = conn
		self._ems_name = ems_name
		self.__ems = None
		self.__fleet = None
		self.__aircraft = None
		self.__airport = None
		self.__flightphase = None
		self.__init_ems()
		self._ems_id = self.__ems.get_id(ems_name)
		# self._init_assets()
		# self.reset()

	def __init_ems(self):
		self.__ems = EMS(self._conn)

	def get_ems_id(self):
		"""
		Returns EMS id

		Returns
		-------
		int
			EMS system id
		"""
		return self._ems_id

	def _init_assets(self):
		# self.__fleet = Fleet(self._conn, self._ems_id)
		# self.__aircraft = Aircraft(self._conn, self._ems_id)
		# self.__airport = Airport(self._conn, self._ems_id)
		# self.__flightphase = FlightPhase(self._conn, self._ems_id)		
		# self.__flight = Flight(self._conn, self._ems_id)
		raise NotImplementedError()
