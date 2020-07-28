from __future__ import print_function
from builtins import range
from builtins import object
import pandas as pd
import numpy as np
import sys
from emspy.query import LocalData


class Analytic(object):
	"""
	Analytic class
	"""
	def __init__(self, conn, ems_id, data_file=LocalData.default_data_file):
		"""
		Analytic class initialization

		Parameters
		----------
		conn: emspy.connection.Connection
			connection object
		ems_id: int
			EMS system id
		data_file: str
			path to local DB file
		"""
		self._conn = conn
		self._ems_id = ems_id
		self._metadata = None
		self._load_paramtable(data_file)

	def _load_paramtable(self, file_name=LocalData.default_data_file):
		# Load parameter table
		if self._metadata is None:
			self._metadata = LocalData(file_name)
		elif not self._metadata.is_db_path_correct(file_name):
			self._metadata.close()
			self._metadata = LocalData(file_name)

		self._param_table = self._metadata.get_data("params", "ems_id = %d" % self._ems_id)

	def _save_paramtable(self):
		# Save parameter table
		if len(self._param_table) > 0:
			self._metadata.delete_data("params", "ems_id = %d" % self._ems_id)
			self._metadata.append_data("params", self._param_table)

	def get_param_details(self, analytic_id):
		"""
		Method to get parameter details from the API

		Parameters
		----------
		analytic_id: str
			unique analytic id

		Returns
		-------
		content: list
			response from API
		"""
		_, content = self._conn.request(
			rtype="POST",
			uri_keys=('analytic', 'search'),
			uri_args=self._ems_id,
			jsondata={'id': analytic_id}
		)
		return content

	def search_param(self, keyword, in_df=False):
		"""
		Method to search a parameter in EMS and return the following fields:
			- id: EMS id
			- name: parameter name
			- description: parameter description
			- units: parameter units
			- ems_id: ems system id

		Parameters
		----------
		keyword: str
			parameter to search
		in_df: bool
			selector for output type, if True returns a dataframe,
			if False returns a list
		Returns
		-------
		pd.DataFrame or list
			parameter information (id, name, description, units, ems_id)
		"""
		print('Searching for params with keyword "%s" from EMS ...' % keyword, end=' ')
		# EMS API Call
		_, content = self._conn.request(
			uri_keys=('analytic', 'search'),
			uri_args=self._ems_id,
			body={'text': keyword}
		)
		if len(content) == 0:
			sys.exit("No parameter found with search keyword %s." % keyword)
		elif len(content) == 1:
			res = content
		else:
			word_len = [len(x['name']) for x in content]
			idx = np.argsort(word_len).tolist()
			res = [content[i] for i in idx]
		print("done.")

		for i in range(len(res)):
			res[i]['ems_id'] = self._ems_id

		if in_df:
			return pd.DataFrame(res)
		
		return res

	def get_param(self, keyword, unique=True):
		"""
		Get parameter information

		Parameters
		----------
		keyword: str
			parameter to fetch
		unique: bool
			indicates whether to return the parameter with the shortest name (if True)

		Returns
		-------
		dict
			fetched parameter information (uri_root, ems_id, id, name, description, units)
		"""
		# if the param table is empty, just return an empty param dict.
		if self._param_table.empty:
			return dict(ems_id="", id="", name="", description="", units="")
		# If the param table is not empty, do search by keyword
		bool_idx = self._param_table['name'].str.contains(keyword, case=False, regex=False)
		df = self._param_table[bool_idx]
		# If the search result is empty, return empty param dict
		if df.empty:
			return dict(ems_id="", id="", name="", description="", units="")
		# If not empty, return the one with shortest name
		if df.shape[0] > 1:
			idx = df['name'].map(lambda x: len(x)).sort_values().index
			df = df.loc[idx, :]
		# When unique = True
		if unique:
			return df.iloc[0, :].to_dict()
		# When unique = False
		return df.to_dict('records')
