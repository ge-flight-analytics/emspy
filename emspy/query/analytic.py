from __future__ import print_function
from builtins import range
from builtins import object
import pandas as pd
import numpy as np
import os, sys, re
import emspy
from emspy.query import LocalData

class Analytic(object):

	def __init__(self, conn, ems_id, data_file = None):
		self._conn        = conn
		self._ems_id      = ems_id
		self._metadata    = None
		self._load_paramtable(data_file)


	def _load_paramtable(self, file_name = None):
		if self._metadata is None:
			self._metadata = LocalData(file_name)
		else:
			if (file_name is None) and (self._metadata.file_loc() != os.path.abspath(file_name)):
				self._metadata.close()
				self._metadata = LocalData(file_name)

		self._param_table = self._metadata.get_data("params", "ems_id = %d" % self._ems_id)

	
	def _save_paramtable(self):
		if len(self._param_table) > 0:
			self._metadata.delete_data("params", "ems_id = %d" % self._ems_id)
			self._metadata.append_data("params", self._param_table)


	def search_param(self, keyword, in_df = False):
		print('Searching for params with keyword "%s" from EMS ...' % keyword, end=' ')
		# EMS API Call
		resp_h, content = self._conn.request( uri_keys=('analytic', 'search'),
											  uri_args=self._ems_id,
											  body={'text': keyword})
		if len(content) == 0:
			sys.exit("No parameter found with search keyword %s." % keyword)
		elif len(content) == 1:
			res = content
		else:
			word_len     = [len(x['name']) for x in content]
			idx          = np.argsort(word_len).tolist()
			res          = [content[i] for i in idx]
		print("done.")

		for i in range(len(res)):
			res[i]['ems_id'] = self._ems_id

		if in_df:
			return pd.DataFrame(res)
		
		return res
		

	def get_param(self, keyword, unique = True):
		# if the param table is empty, just return an empty param dict.
		if self._param_table.empty:
			return dict(ems_id="", id="", name="", description="", units="")
		# If the param table is not empty, do search by keyword
		bool_idx = self._param_table['name'].str.contains(keyword, case = False, regex=False)
		df = self._param_table[bool_idx]
		# If the search result is empty, return empty param dict
		if df.empty:
			return dict(ems_id="", id="", name="", description="", units="")
		# If not empty, return the one with shortest name
		if df.shape[0] > 1:
			idx = df['name'].map(lambda x: len(x)).sort_values().index
			df  = df.loc[idx, :]
		# When unique = True
		if unique:
			return df.iloc[0,:].to_dict()
        # When unique = False
		return df.to_dict('records')




