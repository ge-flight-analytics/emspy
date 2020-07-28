from __future__ import absolute_import
from .asset import Asset


class FlightPhase(Asset):
	"""
	Retrieve the data of defined flight phases.
	"""

	def __init__(self, conn, ems_id=7):
		"""
		Flight phase object initialization

		Parameters
		----------
		conn: emspy.connection.Connection
			connection object
		ems_id: int
			EMS system id
		"""
		Asset.__init__(self, conn, "FlightPhase")
		self._ems_id = ems_id
		self.update_list()

	def update_list(self):
		"""
		Update flight phase list

		Returns
		-------
		None
		"""
		Asset.update_list(self, uri_keys=('flt_phase','list'), uri_args=self._ems_id)
		self._rename_datacol('description', 'name')

	def get_id(self, name=None):
		"""
		Get flight phase id from name

		Parameters
		----------
		name: str
			flight phase name

		Returns
		-------
		int
			flight phase id
		"""
		flightphase_id = self.search('name', name)['id'].tolist()
		return flightphase_id if len(flightphase_id) > 1 else flightphase_id[0]

	def get_name(self, id_val=None):
		"""
		Get flight phase name from id

		Parameters
		----------
		id_val: int
			flight phase id

		Returns
		-------
		str
			flight phase name
		"""
		flightphase_name = self.search('id', id_val, searchtype="match")['name'].tolist()
		return flightphase_name if len(flightphase_name) > 1 else flightphase_name[0]
