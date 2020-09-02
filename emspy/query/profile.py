import pandas as pd
from .query import Query


class Profile(Query):
    """
    A class for performing profile queries in the EMS API.
    """
    def __init__(self, conn, ems_name, profile_number=None, profile_name='', searchtype='match'):
        """
        Parameters
        ----------
        conn: ems.connection.Connection
            A valid instantiated EMS connection object.
        ems_name: str
            A valid EMS system name.
        profile_number: int
            A profile number to select.
        profile_name: str
            A profile name.
        searchtype: str
            search type to perform:
                contain: uses the Pandas string method 'contains' on the column as string
                match: uses the Pandas string method 'match' on the column as string

        Raises
        ------
        ValueError
            If neither `profile_number` and `profile_name` are specified.
        LookupError
            If a profile matching either the `profile_number` or `profile_name` is not found.
        """
        Query.__init__(self, conn, ems_name)

        # Set parameters from arguments
        self._conn = conn
        self._ems_name = ems_name
        self._input_profile_number = profile_number
        self._input_profile_name = profile_name

        # To be filled out later
        self._guid = None
        self._glossary = None
        self._events_glossary = None
        self._exact_search = False

        # Get EMS id.
        self._ems_id = self.get_ems_id()

        # Set up template strings.
        self._no_matches_template = "Did not find any profiles that could match {0}: {1}"
        self._many_matches_template = "Found multiple profiles that could match {0}: {1}.\n\n{2}"

        # Check that at least one of the profile number/name arguments was passed.
        if profile_number is None and profile_name == '':
            raise ValueError("Either profile_number, profile_name, or both must be specified.")

        # Decide how to search
        self.searchtype = searchtype
        self._search_type = 'number' if profile_number is not None else 'name'
        # Perform a search using the specified options
        self._search()

    def get_glossary(self):
        """
        Method to get the glossary for the profile

        Returns
        -------
        pd.DataFrame
            A Pandas DataFrame containing a profile glossary.
        """
        if self._guid is not None:
            resp_h, dict_data = self._conn.request(
                uri_keys=('profile', 'glossary'),
                uri_args=(self._ems_id, self._guid)
            )
            a = pd.DataFrame.from_dict(dict_data)
            # convert the dictionaries values within the glossaryItems column into a new DataFrame
            b = a['glossaryItems'].apply(pd.Series)
            # concatenate a and b, drop the old glossaryItems column name
            c = pd.concat([a, b], axis=1).drop('glossaryItems', axis=1)
            self._glossary = c
            return self._glossary
        else:
            print("The search results did not return a profile matching the input profile name and "
                  "number on the given system. Please try to instantiate the profile object with "
                  "different arguments.")
            return

    def get_events_glossary(self):
        """
        Method to get the events glossary for the profile

        Returns
        -------
        pd.DataFrame
            A Pandas datafarme containing the events glossary
        """
        if self._guid is not None:
            _, dict_data = self._conn.request(
                uri_keys=('profile', 'events'),
                uri_args=(self._ems_id, self._guid)
            )
            data = pd.DataFrame(dict_data)
            data.set_index('id', inplace=True)
            self._events_glossary = data
            return self._events_glossary
        else:
            print("The search results did not return a profile matching the input profile name and "
                  "number on the given system. Please try to instantiate the profile object with "
                  "different arguments.")
            return

    def __query_profile_results(self, flight_id):
        if self._guid is not None:
            _, dict_data = self._conn.request(
                uri_keys=('profile', 'profile_results'),
                uri_args=(self._ems_id, flight_id, self._guid)
            )
            return dict_data

    def get_measurements(self, flight_id):
        """
        Get default event measurements for selected flight

        Parameters
        ----------
        flight_id: int
            flight record identifier

        Returns
        -------
        measurements: pd.DataFrame
            event default measurements for selected flight
        """
        profile_results = self.__query_profile_results(flight_id)
        measurements = pd.DataFrame(profile_results['measurements']).sort_values('itemId')
        glossary = self.__filter_glossary('measurement', 'default')
        measurements['name'] = measurements['itemId'].map(glossary['name'].to_dict())
        return measurements

    def get_timepoints(self, flight_id):
        """
        Get default event timepoints for selected flight

        Parameters
        ----------
        flight_id: int
            flight record identifier

        Returns
        -------
        measurements: pd.DataFrame
            event default timepoints for selected flight
        """
        profile_results = self.__query_profile_results(flight_id)
        timepoints = pd.DataFrame(profile_results['timepoints']).sort_values('itemId')
        glossary = self.__filter_glossary('timepoint', 'default')
        timepoints['name'] = timepoints['itemId'].map(glossary['name'].to_dict())
        return timepoints

    def get_events(self, flight_id):
        """
        Get event information for selected flight

        Parameters
        ----------
        flight_id: int
            flight record identifier

        Returns
        -------
        event_data: pd.DataFrame
            selected event field for selected flight
        """
        profile_results = self.__query_profile_results(flight_id)
        events = pd.DataFrame(profile_results['events'])

        # Grab event names and ID's from the glossary.
        event_name_details = self.__filter_glossary('event', 'eventSpecific')[['eventTypeId', 'name']]\
            .astype({'eventTypeId': int})\
            .rename(columns={'name': 'eventName'})
        # Merge in the event names so the user has something human-readable to work with.
        event_data = events\
            .merge(event_name_details, how='left', left_on='eventType', right_on='eventTypeId')\
            .drop(columns='eventTypeId')

        return event_data

    def __filter_glossary(self, record_type, scope):
        # Retrieve the profile glossary if it has not already been loaded.
        if self._glossary is None:
            self.get_glossary()
        glossary = self._glossary.loc[(self._glossary['recordType'] == record_type), :]
        glossary = glossary.loc[(self._glossary['scope'] == scope), :]
        glossary = glossary.set_index('itemId')
        return glossary

    def __set_profile_attributes(self, profile_data):
        if isinstance(profile_data, pd.DataFrame):
            profile_data = profile_data.iloc[0]
        self._guid = profile_data['id']
        self._current_version = profile_data['currentVersion']
        self._library = profile_data['library']
        self._profile_name = profile_data['name']
        self._local_id = profile_data['localId']

    def __request_results(self):
        # get values via API.
        _, dict_data = self._conn.request(
            uri_keys=('profile', 'search'),
            uri_args=self._ems_id,
            body={'search': self._input_profile_name}
        )
        data = pd.DataFrame.from_dict(dict_data)  # create a DataFrame from the response.
        self._search_results = data
        return data

    def __validate_search_results(self, res):
        if len(res) == 0:
            raise LookupError(self._no_matches_template.format(
                'profile name',
                self._input_profile_name
            ))

    def __filter_results(self, res):
        filtered = res  # default assignment
        # search can be of three different types: {name+exact, name+inexact, number}
        if self._search_type == 'name':
            if self.searchtype == 'contain':
                filtered = res.loc[res['name'].str.lower()
                                              .str.contains(self._input_profile_name.lower()), :]
            elif self.searchtype == 'match':
                filtered = res.loc[res['name'].str.lower()
                                              .str.match(self._input_profile_name.lower()), :]
        elif self._search_type == 'number':  # number search
            # locate rows where the localId matches the input profile number.
            filtered = res.loc[res['localId'] == self._input_profile_number]
        if isinstance(filtered, pd.Series):
            filtered = filtered.to_frame(filtered.name).T
        return filtered

    def __validate_filtered(self, filtered):
        # Check the filtered values.
        # If only one row is found, set the class attributes using this row.
        if filtered.shape[0] == 1:
            print('Found a profile with the supplied profile number and name.')
            print('Profile name: {0}, profile number: {1}.'.format(filtered['name'],
                                                                   filtered['localId']))
            self.__set_profile_attributes(filtered)
        # if more than one rows are found (shouldn't be possible) return the profile with
        # the shortest name
        else:
            # if no rows are found, return an error.
            if filtered.empty:
                # choose the right template for the returned error.
                if self._search_type == 'number':
                    raise LookupError(self._no_matches_template.format(
                        'profile number',
                        self._input_profile_number
                    ))
                elif self._search_type == 'name':
                    raise LookupError(self._no_matches_template.format(
                        'profile name',
                        self._input_profile_name
                    ))
            if self._search_type == 'name':
                # Get shortest name
                print('Found {0} profiles matching the supplied profile name.'
                      .format(filtered.shape[0]))
                print('Returning profile with shortest name.')
                filtered =\
                    filtered.loc[filtered['name'].str.len() == filtered['name'].str.len().min(), :]
                if filtered.shape[0] > 1:
                    raise LookupError(self._many_matches_template.format(
                        'profile name',
                        self._input_profile_name,
                        str(filtered.loc[:, ['localId', 'name']])
                    ))
                else:
                    print('Profile name: {0}, profile number: {1}.'.format(filtered['name'],
                                                                   filtered['localId']))
                    self.__set_profile_attributes(filtered)
            elif self._search_type == 'number':
                raise LookupError(self._many_matches_template.format(
                    'profile number',
                    self._input_profile_number,
                    str(filtered.loc[:, ['localId', 'name']])
                ))

    def _search(self):
        res = self.__request_results()
        self.__validate_search_results(res)
        filtered = self.__filter_results(res)
        self.__validate_filtered(filtered)
