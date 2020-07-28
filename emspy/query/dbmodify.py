from .query import Query
from .ems import EMS
import pandas as pd
import json


class InsertQuery(Query):
    """
    A class for performing database inserts using the EMS API.
    """
    def __init__(self, conn, ems_name, database_id):
        """
        Query initialization

        Parameters
        ----------
        conn: ems.connection.Connection
            A valid instantiated EMS connection object.
        ems_name: str
            A valid EMS system name.
        database_id: str
            A database id to insert into.
        """
        Query.__init__(self, conn, ems_name)
        self._database_id = database_id
        self.__create_columns = {'createColumns': []}

    def in_json(self):
        """
        Returns data as JSON

        Returns
        -------
        str
            JSON serialization of the create_columns attribute
        """
        return json.dumps(self.__create_columns)

    def reset(self):
        """
        Resets the API query to send. This should be called before creating a new query.

        Returns
        -------
        None
        """
        self.__create_columns = {'createColumns': []}

    def get_create_columns(self):
        """
        A method which returns the values of createColumns attribute.

        Returns
        -------
        dict
            a dictionary corresponding to the current value of self.__create_columns
        """
        return self.__create_columns

    def insert_df(self, df, schema_map=None):
        """
        A method for inserting values from a pandas DataFrame.

        Parameters
        ----------
            df: pd.DataFrame
                A pandas DataFrame of values to input, where the columns are the fieldIds and the
                entries are values to input (unless schema_map is passed, in which case column names
                can be arbitrary as long as they exist in schema_map and map to ems monikers).
            schema_map: dict
                A mapping of named DataFrame columns to field ids, e.g.
                {'column1' = '[-hub][schema]'}. If this is not passed, the columns of df should
                correspond to EMS schemas.

        Raises
        ------
            TypeError
                If `schema_map` is not a dictionary.
            TypeError
                If `df` is not a pandas DataFrame.
            ValueError
                If `schema_map` is passed, but not all `df` column names are present in `schema_map`
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("`df` must be a pandas DataFarme. Found: {0}".format(type(df)))

        # If a schema_map was passed in (mapping column name to ems schema)
        if schema_map is not None:
            # schema_map should be a dictionary.
            if not isinstance(schema_map, dict):
                raise TypeError("`schema_map` must be a dictionary.  Found: {0}".format(type(df)))
            # Find columns that exist in `df` but not in `schema_map`.  Convert to list.
            missing_from_map = list(set(df.columns.values) - set(schema_map.keys()))
            # If any columns are in `df` but not in `schema_map`, error out.
            if len(missing_from_map) > 0:
                raise ValueError("Column(s): '{0}' found in `df`, but not in `schema_map`."
                                 .format(missing_from_map))
            df = df.rename(columns=schema_map)
        else:
            print('Schema_map was not passed. '
                  'Assuming dataframe column names correspond to EMS schema.')

        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            row_list = [
                {'fieldId': fid, 'value': val} for fid, val
                in zip(row_dict.keys(), row_dict.values())
            ]
            self.__create_columns['createColumns'].append(row_list)

    def run(self):
        """
        Method for posting the query to the API

        Returns
        -------
        None
        """
        _, content = self._conn.request(
            rtype='POST',
            uri_keys=('database', 'create'),
            uri_args=(self._ems_id, self._database_id),
            jsondata=self.__create_columns
        )
        # Unintended responses are handled in Connection.request, so we only do verification here.
        rows_added = content['rowsAdded']
        if rows_added == len(self.__create_columns['createColumns']):
            print('Added all rows.')
