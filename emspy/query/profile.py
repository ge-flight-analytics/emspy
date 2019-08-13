import pandas as pd
from .query import Query

'''
A class for profile queries using the EMS REST API
'''


class Profile(Query):
    def __init__(self, conn, ems_name, profile_number=None, profile_name=''):
        Query.__init__(self, conn, ems_name)
        self._conn = conn
        self._ems_name = ems_name
        self._input_profile_number = profile_number
        self._guid = None
        self._glossary = None
        self._events_glossary = None
        self._input_profile_name = profile_name
        self._ems_id = self.get_ems_id()

        if profile_number is None and profile_name == '':
            raise ValueError("Either profile_number, profile_name, or both must be specified.")

        if profile_number is not None:
            self._search_by_number()
        else:
            self._search_by_name()

    def get_glossary(self):
        if self._guid is not None:
            resp_h, dict_data = self._conn.request(uri_keys=('profile', 'glossary'),
                                                   uri_args=(self._ems_id, self._guid))
            a = pd.DataFrame.from_dict(dict_data)
            # convert the dictionaries values within the glossaryItems column of a into a new DataFrame
            b = a['glossaryItems'].apply(pd.Series)
            # concatenate a and b, drop the old glossaryItems column name
            c = pd.concat([a, b], axis=1).drop('glossaryItems', axis=1)
            self._glossary = c
            return self._glossary
        else:
            print("The search results did not return a profile matching the input profile name and number on the given"
                  "system.  Please try to instantiate the profile object with different arguments.")
            return

    def get_events_glossary(self):
        if self._guid is not None:
            resp_h, dict_data = self._conn.request(uri_keys=('profile', 'events'),
                                                   uri_args=(self._ems_id, self._guid))
            a = pd.DataFrame(dict_data)
            a.set_index('id', inplace=True)
            self._events_glossary = a
            return self._events_glossary
        else:
            print("The search results did not return a profile matching the input profile name and number on the given"
                  "system.  Please try to instantiate the profile object with different arguments.")
            return

    def get_profile_results(self, flight_id):
        raise NotImplementedError
        if self._guid is not None:
            resp_h, dict_data = self._conn.request(uri_keys=('profile', 'profile_results'),
                                                   uri_args=(self._ems_id, flight_id, self._guid))
            '''
            TODO: Implement some sort of parsing of resulting data.  Honestly, it's not super useful unless paired
            with some supplementary information.  There are no names.  No descriptions.  Perhaps I could combine this
            information with the information obtained in the "glossary" and create a meaningful output.   
            
            Glossary, however, does not contain the names and ID's of events.  Maybe those will have to be grabbed using
            the other API endpoints.  
            '''
        else:
            print("The search results did not return a profile matching the input profile name and number on the given"
                  "system.  Please try to instantiate the profile object with different arguments.")
            return

    def __set_attributes(self, profile_df):
        self._guid = profile_df['id'].values[0]
        self._current_version = profile_df['currentVersion'].values[0]
        self._library = profile_df['library'].values[0]
        self._profile_name = profile_df['name'].values[0]
        self._local_id = profile_df['localId'].values[0]

    def _search_by_number(self):
        # get values via API.
        resp_h, dict_data = self._conn.request(uri_keys=('profile', 'search'), uri_args=self._ems_id,
                                               body={'search': self._input_profile_name})
        a = pd.DataFrame.from_dict(dict_data)  # create a DataFrame from the response.
        self._search_results = a

        # search for the provided profile number within the returned dataframe
        # locate rows where the localId matches the input profile number.
        filtered = a.loc[a['localId'] == self._input_profile_number]
        # if only one row is found, set the class attributes using this row.
        if len(filtered) == 1:
            print('Found a profile with the supplied profile number and name.')
            self.__set_attributes(filtered)
            print('Profile name: {0}, profile number: {1}.'.format(self._profile_name, self._local_id))
        # if more than one rows are found (shouldn't be possible) return a lookup error.
        elif len(filtered) > 1:
            template = "Found multiple profiles that could match {0}:\n\n {1}"
            match_names = filtered['name'].values
            match_names_formatted = '\t' + '\n\t'.join(match_names)  # format a string for printing
            raise LookupError(template.format(self._input_profile_name, match_names_formatted))
        # if no rows are found, return an error.
        elif len(filtered) == 0:
            template = "Did not find any profiles that could match profile_number: {0}."
            raise LookupError(template.format(self._input_profile_number))

    def _search_by_name(self, exact=False):
        resp_h, dict_data = self._conn.request(uri_keys=('profile', 'search'), uri_args=self._ems_id,
                                               body={'search': self._input_profile_name})
        a = pd.DataFrame.from_dict(dict_data)
        self._search_results = a

        # are we performing an exact string search?  (i.e. using string equality rather than 'contains')
        if exact:
            filtered = a.loc[a['name'] == self._input_profile_name, :]  # locate profiles using exact string match.
        else:
            filtered = a.loc[a['name'].str.contains(self._input_profile_name), :]  # locate profiles with string in name.

        # if only one row is found, set the class attributes using this row.
        if len(filtered) == 1:
            print('Found a profile with the supplied profile number and name.')
            self.__set_attributes(filtered)
            print('Profile name: {0}, profile number: {1}.'.format(self._profile_name, self._local_id))
        # if more than one rows are found, print the matching profiles names and try again using an exact search.
        elif len(filtered) > 1:
            template = "Found multiple profiles that could match {0}:\n\n {1}"
            match_names = filtered['name'].values
            match_names_formatted = '\t' + '\n\t'.join(match_names)  # format a string for printing
            print(template.format(self._input_profile_name, match_names_formatted))
            print('Attempting exact string match.')
            self._search_by_name(exact=True)  # try again using exact string matching to pare down the results.
        # if no profiles are found, return a lookup error.
        elif len(filtered) == 0:
            template = "Did not find any profiles that could match {0}."
            raise LookupError(template.format(self._input_profile_name))
