import pandas as pd
from .query import Query


class Profile(Query):
    """
    A class for performing profile queries in the EMS API.

    Args:
        conn (ems.connection): A valid instantiated EMS connection object.
        ems_name (str): A valid EMS system name.
        profile_number (:obj: `int`, optional): A profile number to select.
        profile_name (:obj: `str`, optional): A profile name.

    Raises:
        ValueError: If neither `profile_number` and `profile_name` are specified.
        LookupError: If a profile matching either the `profile_number` or `profile_name` is not found.
    """

    def __init__(self, conn, ems_name, profile_number=None, profile_name=''):
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
        self._no_matches_name_template = "Did not find any profiles that could match profile_name: {0}."
        self._no_matches_num_template = "Did not find any profiles that could match profile_number: {0}."
        self._many_matches_name_template = "Found multiple profiles that could match {0}:\n\n {1}"
        self._many_matches_num_template = "Found multiple profiles that could match {0}:\n\n {1}"

        # Check that at least one of the profile number/name arguments was passed.
        if profile_number is None and profile_name == '':
            raise ValueError("Either profile_number, profile_name, or both must be specified.")

        # Decide how to search
        self._search_type = 'number' if profile_number is not None else 'name'
        # Perform a search using the specified options
        self._search(self._search_type)

    def get_glossary(self):
        """
        Returns:
            A Pandas DataFrame containing a profile glossary.
        """

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

    def __set_profile_attributes(self, profile_df):
        self._guid = profile_df['id'].values[0]
        self._current_version = profile_df['currentVersion'].values[0]
        self._library = profile_df['library'].values[0]
        self._profile_name = profile_df['name'].values[0]
        self._local_id = profile_df['localId'].values[0]

    def __request_results(self):
        # get values via API.
        resp_h, dict_data = self._conn.request(uri_keys=('profile', 'search'), uri_args=self._ems_id,
                                               body={'search': self._input_profile_name})
        a = pd.DataFrame.from_dict(dict_data)  # create a DataFrame from the response.
        self._search_results = a
        return a

    def __validate_search_results(self, res):
        if len(res) == 0:
            raise LookupError(self._no_matches_name_template.format(self._input_profile_name))

    def __filter_results(self, res):
        filtered = res  # default assignment
        # search can be of three different types: {name+exact, name+inexact, number}
        if self._search_type == 'name':
            if self._exact_search is True:  # name + exact search
                # locate profiles using exact string match.
                filtered = res.loc[res['name'] == self._input_profile_name, :]
            else:  # name + inexact search
                # locate profiles with string in name (case insensitive).
                res['name_lower'] = res['name'].str.lower()
                filtered = res.loc[res['name_lower'].str.contains(self._input_profile_name.lower()), :]
        elif self._search_type == 'number':  # number search
            # locate rows where the localId matches the input profile number.
            filtered = res.loc[res['localId'] == self._input_profile_number]
        return filtered

    def __validate_filtered(self, filtered):
        # Check the filtered values.
        # If only one row is found, set the class attributes using this row.
        if len(filtered) == 1:
            print('Found a profile with the supplied profile number and name.')
            print('Profile name: {0}, profile number: {1}.'.format(filtered['name'].iloc[0],
                                                                   filtered['localId'].iloc[0]))
            self.__set_profile_attributes(filtered)
        # if more than one rows are found (shouldn't be possible) either try again (if we are searching by name in an
        # inexact manner, we will now try an exact search) or return a LookupError.
        # Inexact search could lead to more than one match, leaving the potential for an exact search to narrow to one.
        # It should not be possible to have more than one match if you are searching with a profile number.
        elif len(filtered) > 1:
            match_names = filtered['name'].values
            match_profiles = filtered['localId'].values
            match_strings = ['P' + str(pnum) + ': ' + name for pnum, name in zip(match_profiles, match_names)]
            match_names_formatted = '\t' + '\n\t'.join(match_strings)  # format a string for printing
            print(self._many_matches_name_template.format(self._input_profile_name, match_names_formatted))
            if self._exact_search is False:
                print('Attempting exact string match.')
                self._exact_search = True
                filtered = self.__filter_results(filtered)  # try using exact string matching to pare down results.
                self.__validate_filtered(filtered)  # validate again, using newly obtained exact match results.
            else:
                raise LookupError(self._many_matches_name_template.format(self._input_profile_name,
                                                                          match_names_formatted))
        # if no rows are found, return an error.
        elif len(filtered) == 0:
            # choose the right template for the returned error.
            if self._search_type == 'number':
                raise LookupError(self._no_matches_num_template.format(self._input_profile_number))
            elif self._search_type == 'name':
                raise LookupError(self._no_matches_name_template.format(self._input_profile_name))

    def _search(self, search_type):
        res = self.__request_results()
        self.__validate_search_results(res)
        filtered = self.__filter_results(res)
        self.__validate_filtered(filtered)

