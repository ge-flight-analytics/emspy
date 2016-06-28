import pandas as pd


class Asset:
	'''
	Super class of all "asset" classes and the EMS class. It is a template class so 
	should not be instantiated directly.
	'''

	def __init__(self, conn, asset_type):

		self._conn = conn
		self._asset_type = asset_type
		self._assets = None


	def update_list(self, uri_keys=None, uri_args=None, body=None, colsort=True):

		resp_h, dict_data = self._conn.request(uri_keys=uri_keys, uri_args=uri_args, body=body)
		a = pd.DataFrame.from_dict(dict_data)
		if colsort:
			a = a[sorted(a.columns, key=len)]
		self._assets = a


	def list_all(self):

		return self._assets


	def search(self, col, val = None, searchtype="contain"):

		if val is None:
			return self.list_all()
		
		a = self._assets

		if type(val)==str:			
			if searchtype=="contain":
				return a[a[col].str.contains(val, na=False)]
			if searchtype=="match":
				return a[a[col].str.match(val)]
		
		return a[a[col]==val]


	def get_id(self, name = None, keyword = None):

		raise NotImplementedError()


	def get_name(self, id_val = None):

		raise NotImplementedError()
		

	def _rename_datacol(self, old, new):

		self._assets = self._assets.rename(columns = {old:new})


	def data_colnames(self):

		return self._assets.columns


def size(an_asset):
	
	if issubclass(an_asset, Asset):
		raise TypeError("Input should be an object inherited from Asset class.")

	if an_asset.list_all() is None: 
		return 0
	else:
		return an_asset.list_all().shape[0]



