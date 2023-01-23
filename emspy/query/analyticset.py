from __future__ import absolute_import

import sys
if sys.version_info < (3, 0):
    from future import standard_library
    standard_library.install_aliases()

from .asset import Asset
import pandas as pd
import re


class AnalyticSet(Asset):
    # Declaring static variable for the possible colimns in an analytic set
    __analytic_set_columns = ['id', 'name', 'description',
        'units', 'metadata','chartIndex', 'chartSize', 
        'customName', 'color', 'customRange', 'customDigitsAfterDecimal', 
        'lineWidth', 'displaySampleMarker', 'sampleMarkerShape', 
        'lineStyle', 'parameterFilteringMode', 'interpolationMode'
    ]
    """
    Manages analytic sets querying
    """

    def __init__(self, conn, ems_id, analytic_set_path=None):
        """
        Analytics set asset object initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        ems_id: int
            EMS system id
        analytic_set_path: str
            The path to the analytic set
        """
        Asset.__init__(self, conn, "AnalyticSet")
        self._ems_id = ems_id
        self._analytic_set_path = analytic_set_path
        self.__parse_path()

    def __parse_path(self, path_only=False):
        if not self._analytic_set_path:
            self._group_id = None
            self._analytic_set_id = None
            self._analytic_set_name = None
            self._analytic_set_description = None
            return
        else:
            # Apparently there is no easy why to escape all "\" special characters in python
            # So have to do a find replace first
            ref_dict = {
                '\x07':'\\a',
                '\x08':'\\b',
                '\x0C':'\\f',
                '\n':'\\n',
                '\r':'\\r',
                '\t':'\\t',
                '\x0b':'\\v',                
            }
            # First we need to replace all special backslash characters
            for key,value in ref_dict.items():
                self._analytic_set_path = self._analytic_set_path.replace(key, value)

            # Split on both \ and /
            path_list = re.split(r'[\\,/]', self._analytic_set_path)
            if not path_only:
                self._analytic_set_id = path_list.pop()
                # In case the path ended with "\"
                if self._analytic_set_id == '':
                    raise ValueError('Missing analytic set name. It should be at the end of the path')
            
            # Remove empty items from list
            path_list = [i for i in path_list if i]

            # In case the path starts with "root" or "parameter sets"
            if len(path_list) > 0:
                if (path_list[0].lower() == 'parameter sets') or (path_list[0].lower() == 'root'):
                    path_list.pop(0)

            if len(path_list) == 0:
                self._group_id = 'root'
            else:
                # The group_id is just the folder path separated by ":"
                self._group_id = ":".join(path_list)

    def get_analytic_set(self, analytic_set_path):
        """
        Retrieves the data for the analytic set in the analytic_set_path

        Parameters
        ----------
        analytic_set_path: str
            The path (from root using "\" as separator) to the analytic set

        Returns
        -------
        DataFrame
        """
 
        if not analytic_set_path:
            raise ValueError('No Analytic Set path was passed in. To access the root folder use "\<set name>" or "root\<set name>"')
        self._analytic_set_path = analytic_set_path
        self.__parse_path()
        
        print('-- Fetching analytic set "%s"' % self._analytic_set_path)
        try:
            _, dict_data = self._conn.request(
                uri_keys=('analyticSet', 'analytic_set'),
                uri_args=(self._ems_id, self._group_id, self._analytic_set_id)
            )

            flat_analytic_set_dict = self.__parse_analytic_set(dict_data)
            self._analytic_set_name = flat_analytic_set_dict['name']
            self._analytic_set_description = flat_analytic_set_dict['description']
            data_df = pd.DataFrame.from_records(flat_analytic_set_dict['items'])

            analytic_set_df = pd.DataFrame(columns=AnalyticSet.__analytic_set_columns)

            analytic_set_df = pd.concat([analytic_set_df, data_df])
            return analytic_set_df
        except:
            print('-- Failed to fetch analytic set "%s"' % self._analytic_set_path)


    def get_group_content(self, analytic_group_path=None):
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
        
        if not analytic_group_path:
            analytic_group_path = 'root'
        self._analytic_set_path = analytic_group_path
        self.__parse_path(path_only=True)

        group_info = self.__get_analytic_set_group()
        
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

        
    
    def __get_analytic_set_group(self):
        print('-- Fetching info for analytic set group %s' % self._group_id)
        _, dict_data = self._conn.request(
            uri_keys=('analyticSet', 'analytic_set_group'),
            uri_args=(self._ems_id, self._group_id)
        )
        return dict_data
    
    
    def __parse_analytic_set(self, base_analytic_dict):
        """
        Parses an analytic set into a list of flattened dictionaries. Essentially moved the 'analytic' boject items up one level in each analytic
        """
        analytics = base_analytic_dict['items']
        for idx, analytic in enumerate(analytics):
            for key, value in analytic['analytic'].items():
                analytics[idx][key] = value
            del analytics[idx]['analytic']
        return base_analytic_dict
