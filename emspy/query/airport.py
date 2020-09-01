from __future__ import absolute_import
from .asset import Asset


class Airport(Asset):
	""" Manages airport info """

	_col_tr = {
		'default': 'codePreferred',
		'faa': 'codeFaa',
		'icao': 'codeIcao',
		'city': 'city',
		'name': 'name'
	}

	def __init__(self, conn, ems_id):
		"""
		Airport asset object initialization

		Parameters
		----------
		conn: emspy.connection.Connection
			connection object
		ems_id: int
			EMS system id
		"""
		Asset.__init__(self, conn, "Airport")
		self._ems_id = ems_id
		self.update_list()

	def update_list(self):
		"""
		Update airport list

		Returns
		-------
		None
		"""
		Asset.update_list(self, uri_keys=('airport', 'list'), uri_args=self._ems_id, colsort=False)

	def get_id(self, name=None, code='default'):
		"""
		Get airport id from ICAO code, FAA code, city or name

		Parameters
		----------
		name: str
			ICAO code, FAA code, city or name
		code: str
			input selector:
				- default: preferred code
				- faa: FAA code
				- icao: ICAO code
				- city: city
				- name: airport name

		Returns
		-------
		airport_id: int
			airport id
		"""
		airport_id = self.search(Airport._col_tr[code], name)['id'].tolist()
		return airport_id if len(airport_id) > 1 else airport_id[0]

	def get_name(self, id_val=None, code='default'):
		"""
		Get airport name from id

		Parameters
		----------
		id_val: int
			airport id
		code: str
			output selector:
				- default: preferred code
				- faa: FAA code
				- icao: ICAO code
				- city: city
				- name: airport name

		Returns
		-------
		airport_name: str
			airport output as per selector
		"""
		airport_name = self.search('id', id_val, searchtype="match")[Airport._col_tr[code]].tolist()
		return airport_name if len(airport_name) > 1 else airport_name[0]
