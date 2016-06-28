from asset import Asset


class Airport(Asset):
	'''Manages airport info'''
	_col_tr = {
		'default': 'codePreferred',
		'faa': 'codeFaa',
		'icao': 'codeIcao',
		'city': 'city',
		'name': 'name'
	}

	def __init__(self, conn, ems_id):

		Asset.__init__(self, conn, "Airport")
		self._ems_id = ems_id
		self.update_list()


	def update_list(self):

		Asset.update_list(self, uri_keys=('airport','list'), uri_args=self._ems_id, colsort=False)


	def get_id(self, name = None, code='default'):

		a = self.search(Airport._col_tr[code], name)['id'].tolist()
		return a if len(a) > 1 else a[0]


	def get_name(self, id_val = None, code='default'):

		a = self.search('id', id_val, searchtype="match")[Airport._col_tr[code]].tolist()
		return a if len(a) > 1 else a[0]