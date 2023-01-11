from __future__ import absolute_import

import sys
if sys.version_info < (3, 0):
    from future import standard_library
    standard_library.install_aliases()

import re
import urllib.error
from .asset import Asset
import pandas as pd


class AnalyticSet(Asset):
    """
    Manages analytic sets querying
    """

    def __init__(self, conn, ems_id, exclude_trees=[], exclude_patterns=[]):
        """
        Analytics set asset object initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        ems_id: int
            EMS system id
        exclude_trees: list
            list of trees to exclude from the group update, such as:
            "Engine Services", "ADI", "EMS Library"
        exclude_patterns: list
            list of regex patterns for exclusion of groups, such as:
            r"Engine Services:\d{9}.*" to exclude all SSO subdirectories in Engine Services
        """
        Asset.__init__(self, conn, "AnalyticSet")
        self._ems_id = ems_id
        self._analytic_sets = []
        self._analytics = []
        self.exclude_trees = exclude_trees
        self.exclude_patterns = exclude_patterns
        if len(self.exclude_patterns) > 0:
            self.exclude_patterns = [re.compile(pattern) for pattern in self.exclude_patterns]
        self.update_list()

    def __get_analytic_set_group(self, groupId):
        if groupId in self.exclude_trees:
            print('-- Excluding analytic set group {0}'.format(groupId))
            return
        for pattern in self.exclude_patterns:
            if re.match(pattern, groupId) is not None:
                print('-- Excluding analytic set group {0}'.format(groupId))
                return
        try:
            print('-- Fetching analytic set group {0}'.format(groupId))
            _, dict_data = self._conn.request(
                uri_keys=('analyticSet', 'analytic_set_group'),
                uri_args=(self._ems_id, groupId)
            )
            self._analytic_sets.append(dict_data)
            for group in dict_data['groups']:
                self.__get_analytic_set_group(group['groupId'])
        except urllib.error.HTTPError:
            print('-- Failed to fetch analytic set group {0}'.format(groupId))

    def update_list(self):
        """
        Update analytic sets list

        Returns
        -------
        None
        """
        self.__get_analytic_set_group('Root')
        self._analytic_sets = pd.DataFrame(self._analytic_sets)
        self._analytic_sets = self._analytic_sets.drop(['groups', 'sets'], axis=1)\
            .join(self._analytic_sets['sets'].apply(pd.Series))\
            .melt(id_vars=['name', 'groupId'], value_name='set')\
            .drop('variable', axis=1)\
            .dropna(subset=['set'])
        self._analytic_sets['set'] = self._analytic_sets['set'].apply(lambda d: d['name'])
        self._analytic_sets.sort_values(['name', 'groupId', 'set'], inplace=True)
        self._analytic_sets.reset_index(drop=True, inplace=True)

    def select_set(self, analytics_set_name=None, searchtype='match'):
        """
        Get fleet name from id

        Parameters
        ----------
        analytics_set_name: str
            set name
        searchtype: str
            search type:
                contains: inexact matching (returns shortest)
                match: exact matching

        Returns
        -------
        None
        """
        analytics_set = self.search('set', analytics_set_name, searchtype)
        if analytics_set.shape[0] > 1:
            analytics_set = analytics_set.loc[analytics_set['set'].str.len().idxmin(), :]\
                .to_frame().T
        _, dict_data = self._conn.request(
            uri_keys=('analyticSet', 'analytic_set'),
            uri_args=(self._ems_id, analytics_set.iloc[0]['groupId'], analytics_set.iloc[0]['set'])
        )
        self._analytics = pd.DataFrame(dict_data)
        self._analytics = self._analytics.drop('items', axis=1) \
            .join(self._analytics['items'].apply(pd.Series))
        self._analytics = self._analytics.drop('analytic', axis=1) \
            .join(self._analytics['analytic'].apply(pd.Series), rsuffix='_analytic')
        return self._analytics

    def list_all(self):
        return self._analytics

    def _rename_datacol(self, old, new):
        # Rename a column
        raise NotImplementedError

    def data_colnames(self):
        """
        Returns the columns for the asset dataframe

        Returns
        -------
        pd.Index
            dataframe columns
        """
        return self._analytics.columns

