from emspy.query import *


class Query:

	def __init__(self, conn, ems_name):

		self._conn 		= conn
		self._ems_name  = ems_name
		self.__ems 		= None
		self.__fleet 	= None
		self.__aircraft = None
		self.__airport 	= None
		self.__flightphase = None
		self.__init_ems()
		self._ems_id 	= self.__ems.get_id(ems_name)
		# self._init_assets()
		# self.reset()


	def __init_ems(self):

		self.__ems = EMS(self._conn)


	def _init_assets(self):

		# self.__fleet = Fleet(self._conn, self._ems_id)
		# self.__aircraft = Aircraft(self._conn, self._ems_id)
		# self.__airport = Airport(self._conn, self._ems_id)
		# self.__flightphase = FlightPhase(self._conn, self._ems_id)		
		# self.__flight = Flight(self._conn, self._ems_id)
		raise NotImplementedError()