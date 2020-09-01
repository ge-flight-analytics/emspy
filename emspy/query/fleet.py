from __future__ import absolute_import
from .asset import Asset


class Fleet(Asset):
	"""
	Manages fleet-wise querying
	"""

	def __init__(self, conn, ems_id):
		"""
		Fleet asset object initialization

		Parameters
		----------
		conn: emspy.connection.Connection
			connection object
		ems_id: int
			EMS system id
		"""
		Asset.__init__(self, conn, "Fleet")
		self._ems_id = ems_id
		self.update_list()

	def update_list(self):
		"""
		Update fleet list

		Returns
		-------
		None
		"""
		Asset.update_list(self, uri_keys=('fleet', 'list'), uri_args=self._ems_id)
		self._rename_datacol('description', 'name')

	def get_id(self, name=None):
		"""
		Get fleet id from name

		Parameters
		----------
		name: str
			fleet name

		Returns
		-------
		fleet_id: int
			corresponding fleet name
		"""
		fleet_id = self.search('name', name)['id'].tolist()
		return fleet_id if len(fleet_id) > 1 else fleet_id[0]

	def get_name(self, id_val=None):
		"""
		Get fleet name from id

		Parameters
		----------
		id_val: int
			fleet id

		Returns
		-------
		fleet_name: str
			corresponding fleet name
		"""
		fleet_name = self.search('id', id_val, searchtype="match")['name'].tolist()
		return fleet_name if len(fleet_name) > 1 else fleet_name[0]
