import os
import sqlite3
import sys
from builtins import object

import numpy as np
import pandas as pd

import emspy


class LocalData(object):
	"""
	Class to save metadata in a local DB file
	"""
	default_data_file = os.path.join(emspy.__path__[0], "data", "emsMetaData.db")
	table_info = {
		"fieldtree": ["uri_root", "ems_id", "db_id", "id", "nodetype", "type", "name", "parent_id"],
		"dbtree": ["uri_root", "ems_id", "id", "nodetype", "name", "parent_id"],
		"kvmaps": ["uri_root", "ems_id", "id", "key", "value"],
		"params": ["uri_root", "ems_id", "id", "name", "description", "units"]
	}

	def __init__(self, dbfile=default_data_file):
		"""
		LocalData object initialization

		Parameters
		----------
		dbfile: str
			path to database file
		"""
		self.__dbfile = None
		self._conn = None

		# This is done to support a mode that does not use a data file at all
		# (when dbfile is set to None explicitly)
		if dbfile:
			self.__dbfile = os.path.abspath(dbfile)
			self.__connect()

	def __del__(self):
		self.close()

	def __connect(self):
		if self.__dbfile:
			self._conn = sqlite3.connect(self.__dbfile)

	@staticmethod
	def __check_colnames(table_name, df):
		colnames = np.array(LocalData.table_info[table_name])
		chk_cols = np.array([c in df.columns for c in colnames])
		missing = colnames[~chk_cols]
		if any(~chk_cols):
			sys.exit("Data misses the following columns that are required: %s" % missing)

	def close(self):
		"""
		Closes SQLite connection

		Returns
		-------
		None
		"""
		if self._conn is not None:
			self._conn.close()

	def append_data(self, table_name, df):
		"""
		Adds data to a table

		Parameters
		----------
		table_name: str
			table to append to
		df: pandas.DataFrame
			data to append

		Returns
		-------
		None
		"""
		self.__check_colnames(table_name, df)
		if self.__dbfile is not None:
			df.to_sql(table_name, self._conn, index=False, if_exists="append")

	def get_data(self, table_name, condition=None):
		"""
		Selects data from a table

		Parameters
		----------
		table_name: str
			table to query
		condition: str
			filtering condition (WHERE clause)

		Returns
		-------
		pd.DataFrame
			selected data from the table
		"""
		if (self.__dbfile is not None) and self.table_exists(table_name):
			query = "SELECT * FROM %s" % table_name
			if condition is not None:
				query = query + " WHERE %s" % condition
			query = query + ";"
			df = pd.read_sql_query(query, self._conn)

			# Strange columns appear. Get only the actual columns
			return df[[col for col in LocalData.table_info[table_name] if col in df]]
		return pd.DataFrame(columns=LocalData.table_info[table_name])

	def delete_data(self, table_name, condition=None):
		"""
		Drops a table or deletes from a table

		Parameters
		----------
		table_name: str
			table to drop or to remove records from
		condition: str
			removal condition (WHERE clause), if set to None (default) the table is dropped instead

		Returns
		-------
		None
		"""
		if (self.__dbfile is not None) and self.table_exists(table_name):
			if condition is None:
				self._conn.execute("DROP TABLE %s" % table_name)
			else:
				self._conn.execute("DELETE FROM %s WHERE %s;" % (table_name, condition))
			self._conn.commit()

	def delete_all_tables(self):
		"""
		Deletes all tables from the database

		Returns
		-------
		None
		"""
		if self.__dbfile is not None:
			for table_name in list(LocalData.table_info.keys()):
				if self.table_exists(table_name):
					self._conn.execute("DROP TABLE %s" % table_name)
			self._conn.commit()

	def table_exists(self, table_name):
		"""
		Checks if a table exists

		Parameters
		----------
		table_name: str
			table name

		Returns
		-------
		bool
			False if the table does not exist, True otherwise.
			Also returns False if there is no database file
		"""
		if self.__dbfile is None:
			return False

		cursor = self._conn.cursor()
		cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		tables = [t[0] for t in cursor.fetchall()]
		return table_name in tables

	def file_loc(self):
		"""
		Get database path

		Returns
		-------
		str
			database file path
		"""
		return self.__dbfile

	def is_db_path_correct(self, path):
		"""
		Checks if the path represents the current data file.

		Parameters
		----------
		path: str
			The path to compare.

		Returns
		-------
		bool
			True if the path is the current data file, False otherwise.
		"""
		if path is None:
			return self.file_loc() is None
		else:
			return self.file_loc() == os.path.abspath(path)
