from builtins import object
import emspy

import pandas as pd
import numpy as np
import os, sys, re, sqlite3


class LocalData(object):
	table_info = {
		"fieldtree": ["ems_id", "db_id", "id", "nodetype", "type", "name", "parent_id" ],
		"dbtree"   : ["ems_id", "id", "nodetype", "name", "parent_id"],
	    "kvmaps"   : ["ems_id", "id", "key", "value"],
	    "params"   : ["ems_id", "id", "name", "description", "units"]
	    }


	def __init__(self, dbfile = None):

		if dbfile is None:
			dbfile = os.path.join(emspy.__path__[0], "data","emsMetaData.db")
		dbfile = os.path.abspath(dbfile)
		self.__dbfile = dbfile
		self.__connect()

	
	def __del__(self):

		self.close()


	def __connect(self):

		self._conn = sqlite3.connect(self.__dbfile)


	def __check_colnames(self, table_name, df):

		colnames = np.array(LocalData.table_info[table_name])
		chk_cols = np.array([c in df.columns for c in colnames])
		missing  = colnames[~chk_cols]
		if any(~chk_cols):
			sys.exit("Data misses the following columns that are required: %s" % missing)


	def close(self):

		self._conn.close()


	def append_data(self, table_name, df):
		
		self.__check_colnames(table_name, df)
		df.to_sql(table_name, self._conn, index=False, if_exists="append")
	


	def get_data(self, table_name, condition = None):

		if self.table_exists(table_name):
			q  = "SELECT * FROM %s" % table_name
			if condition is not None:
				q = q + " WHERE %s" % condition
			q  = q + ";"
			df = pd.read_sql_query(q, self._conn)

			# Strange columns appear. Get only the actual columns
			return df[LocalData.table_info[table_name]]		
		return pd.DataFrame(columns = LocalData.table_info[table_name])
		


	def delete_data(self, table_name, condition = None):

		if self.table_exists(table_name):
			if condition is None:
				self._conn.execute("DROP TABLE %s" % table_name)
			else:
				self._conn.execute("DELETE FROM %s WHERE %s;" % (table_name, condition))
			self._conn.commit()


	def delete_all_tables(self):

		for table_name in list(LocalData.table_info.keys()):
			if table_exists(table_name):
				self._conn.execute("DROP TABLE %s" % table_name)
		self._conn.commit()


	def table_exists(self, table_name):

		cursor = self._conn.cursor()
		cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		tables = [t[0] for t in cursor.fetchall()]
		return table_name in tables


	def file_loc(self):

		return self.__dbfile
