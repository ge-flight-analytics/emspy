from __future__ import absolute_import
from .asset import Asset


class EMS(Asset):
    """
    Retrieves EMS system-specific info
    """

    def __init__(self, conn):
        """
        EMS object initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        """
        Asset.__init__(self, conn, "EMS")

    def update_list(self):
        """
        Method to update available EMS systems list

        Returns
        -------
        None
        """
        Asset.update_list(self, uri_keys=('ems_sys', 'list'))

    def ensure_loaded(self):
        """
        Method to ensure list is loaded in the object

        Returns
        -------
        None
        """
        if not (Asset.list_all(self)):
            self.update_list()

    def get_id(self, name=None):
        """
        A method to get an ems_id using a supplied ems_name.

        If name is None the code retrieves the list of available ems systems, if there is only 1
        available system the id for that system is returned, otherwise a list of available systems
        is returned.

        Parameters
        ----------
        name: str
            An EMS system name to get the ID for.

        Raises
        ------
            LookupError
                If no name is provided and the user has access to more than one ems system
            ValueError
                If no matching systems are found with the specified name
        """

        # Support using integer IDs directly
        if isinstance(name, int):
            return name

        self.ensure_loaded()
        if name is not None:
            ems_systems = self.search('name', name.upper(), searchtype="match")
            if ems_systems.empty:
                sys_names = self.list_all()['name'].to_list()
                raise ValueError(
                    'No matching systems found. You have access to: {0}'.format(sys_names))
            id = ems_systems.iloc[0]['id']
        else:
            ems_systems = self.list_all()
            if ems_systems.shape[0] == 1:
                id = ems_systems.iloc[0]['id']
            else:
                raise LookupError(
                    'Multiple ems systems found. Please select one from the available:\n{0}'
                    .format(ems_systems.loc[:, ['id', 'name']])
                )
        return id

    def get_name(self, id_val=None):
        """
        Method to get an EMS instance name from its id

        Parameters
        ----------
        id_val: int
            value id

        Returns
        -------
        name: str
            matching name
        """
        self.ensure_loaded()
        name = self.search('id', id_val, searchtype="match")['name'].tolist()
        return name if len(name) > 1 else name[0]
