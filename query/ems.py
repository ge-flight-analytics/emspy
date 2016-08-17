from asset import Asset


class EMS(Asset):
	'''Retrieves EMS system-specific info'''

	def __init__(self, conn):
		Asset.__init__(self, conn, "EMS")
		self.update_list()


	def update_list(self):

		Asset.update_list(self, uri_keys=('ems_sys', 'list'))



	def get_id(self, name = None):

		name = name.upper()
		a    = self.search('name', name, searchtype="match")['id'].tolist()
		return a if len(a) > 1 else a[0]


	def get_name(self, id_val = None):

		a = self.search('id', id_val, searchtype="match")['name'].tolist()
		return a if len(a) > 1 else a[0]


