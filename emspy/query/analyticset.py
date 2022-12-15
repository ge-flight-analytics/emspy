from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from .asset import Asset
import pandas as pd


class AnalyticSet(Asset):
    # Declaring static variable for the possible colimns in an analytic set
    analytic_set_columns = ['id', 'name', 'description',
        'units', 'metadata','chartIndex', 'chartSize', 
        'customName', 'color', 'customRange', 'customDigitsAfterDecimal', 
        'lineWidth', 'displaySampleMarker', 'sampleMarkerShape', 
        'lineStyle', 'parameterFilteringMode', 'interpolationMode'
    ]
    """
    Manages analytic sets querying
    """

    def __init__(self, conn, ems_id):
        """
        Analytics set asset object initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        ems_id: int
            EMS system id
        """
        Asset.__init__(self, conn, "AnalyticSet")
        self._ems_id = ems_id
        self._analytic_set = {}


    def get_analytic_set(self, analytic_set_name, group_path=['root']):
        """
        Gets analytic set analytic_set_name in the folder defined in the group_path

        Parameters
        ----------
        analytics_set_name: str
            set name
        group_path: list of str
            the path (folders) to the location of the analytic set

        Returns
        -------
        None
        """
        if len(group_path) == 0:
            group_path=['root']
        if (len(group_path) == 1) and ((group_path[0].lower() == 'root') or (group_path[0].lower() == 'parameter sets')):
            group_id = 'root'
        else:
            if (group_path[0].lower() == 'root') or (group_path[0].lower() == 'parameter sets'):
                del group_path[0]
            group_id = ":".join(group_path)
        
        folder_path = "\\".join(group_path)
        print(f'-- Fetching analytic set "{analytic_set_name}" in folder {folder_path}')
        try:
            _, dict_data = self._conn.request(
                uri_keys=('analyticSet', 'analytic_set'),
                uri_args=(self._ems_id, group_id, analytic_set_name)
            )
            self._analytic_set = dict_data

            list_of_entries = self.__parse_analytic_set()
            data_df = pd.DataFrame.from_records(list_of_entries)

            analytics_df = pd.DataFrame(columns=AnalyticSet.analytic_set_columns)

            analytics_df = pd.concat([data_df,analytics_df])
            return analytics_df
        except:
            print('-- Failed to fetch analytic set "{analytic_set_name}" in folder{folder_path}')


    def get_group_content(self, group_path=['root']):
        """
        Returns the group names/ids and analytic names that are contained in the folder
        defined by the group_path list. 

        Parameters
        ----------
        group_path: list of str
            the path (folders) to the location of the analytic set

        Returns
        -------
        Dict with two elements: groups and sets. Both of these are Dataframes
        """
        # Dictinary that will contain the results
        group_dict = {}
        if len(group_path) == 0:
            group_path=['root']
        if (len(group_path) == 1) and ((group_path[0].lower() == 'root') or (group_path[0].lower() == 'parameter sets')):
            group_id = 'root'
        else:
            if (group_path[0].lower() == 'root') or (group_path[0].lower() == 'parameter sets'):
                del group_path[0]
            group_id = ":".join(group_path)
        group_info = self.__get_analytic_set_group(group_id)
        
        if len(group_info['groups']) > 0:
            groups_df = pd.DataFrame.from_records(group_info['groups'])
            group_dict['groups'] = groups_df
        else:
            group_dict['groups'] = pd.DataFrame()
        
        if len(group_info['sets']) > 0:
            sets_df = pd.DataFrame.from_records(group_info['sets'])
            group_dict['sets'] = sets_df
        else:
            group_dict['sets'] = pd.DataFrame()
        
        return group_dict

    
    def __get_analytic_set_group(self, groupId):
        print(f'-- Fetching info for analytic set group {groupId}')
        _, dict_data = self._conn.request(
            uri_keys=('analyticSet', 'analytic_set_group'),
            uri_args=(self._ems_id, groupId)
        )
        return dict_data
    
    def __parse_analytic_set(self):
        """
        Parses an analytic set into a list of flattened dictionaries. Essentially moved the 'analytic' boject items up one level in each analytic
        """
        analytics = self._analytic_set['items']
        for idx, analytic in enumerate(analytics):
            for key, value in analytic['analytic'].items():
                analytics[idx][key] = value
            del analytics[idx]['analytic']
        return analytics
