import pandas as pd
import numpy as np
import os, sys, re
import emspy

class Analytic:
	file_name_fmt = os.path.join(emspy.__path__[0], "data", "param_table_ems_id_%d.cpk")

	def __init__(self, conn, ems_id, new_data = True):
		self._conn        = conn
		self._ems_id      = ems_id
		if self.__paramfile_exists() and not new_data:
			self._load_paramtable()
		else:
			self._param_table = pd.DataFrame() 


	def search_param(self, keyword, in_dataframe = False):
		print 'Searching params with keyword "%s" from EMS ...' % keyword,
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
		print "done."
		if in_dataframe:
			return pd.DataFrame(res)
		return res
		

	def get_param(self, keyword, unique = True):
		# if the param table is empty, just return an empty param dict.
		if self._param_table.empty:
			return dict(id="", name="", description="", units="")
		# If the param table is not empty, do search by keyword
		bool_idx = self._param_table['name'].str.contains(keyword, case = False, regex=False)
		df = self._param_table[bool_idx]
		# If the search result is empty, return empty param dict
		if df.empty:
			return dict(id="", name="", description="", units="")
		# If not empty, return the one with shortest name
		if df.shape[0] > 1:
			idx = df['name'].map(lambda x: len(x)).sort_values().index
			df  = df.loc[idx, :]
		# When unique = True
		if unique:
			return df.iloc[0,:].to_dict()
        # When unique = False
		return df.to_dict('records')


	def _save_paramtable(self, file_name = None):
		import cPickle as p
		if file_name is None:
			file_name = Analytic.file_name_fmt % self._ems_id
		p.dump(self._param_table, open(file_name, "wb"))


	def _load_paramtable(self, file_name = None):
		import cPickle as p
		if file_name is None:
			file_name = Analytic.file_name_fmt % self._ems_id
		self._param_table = p.load(open(file_name, "rb"))


	def __paramfile_exists(self):
		return os.path.exists(Analytic.file_name_fmt % self._ems_id)



