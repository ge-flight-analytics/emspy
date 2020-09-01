from __future__ import absolute_import
from .asset import Asset


class Aircraft(Asset):
    """ Manages aircraft info """

    def __init__(self, conn, ems_id):
        """
        Aircraft asset object initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        ems_id: int
            EMS system id
        """
        Asset.__init__(self, conn, "Aircraft")
        self._ems_id = ems_id
        self.update_list()

    def update_list(self):
        """
        Update aircraft list

        Returns
        -------
        None
        """
        Asset.update_list(self, uri_keys=('aircraft', 'list'), uri_args=self._ems_id)
        self._rename_datacol('description', 'name')

    def get_id(self, name=None):
        """
        Get aircraft id from name

        Parameters
        ----------
        name: str
            aircraft name

        Returns
        -------
        aircraft_id: int
            aircraft id
        """
        aircraft_id = self.search('name', name)['id'].tolist()
        return aircraft_id if len(aircraft_id) > 1 else aircraft_id[0]

    def get_name(self, id_val=None):
        """
        Get aircraft name from id

        Parameters
        ----------
        id_val: str
            aircraft id

        Returns
        -------
        aircraft_name: str
            aircraft name
        """
        aircraft_name = self.search('id', id_val, searchtype="match")['name'].tolist()
        return aircraft_name if len(aircraft_name) > 1 else aircraft_name[0]

    def search_by_fleetid(self, fleetid):
        """
        Search aircraft by fleet id

        Parameters
        ----------
        fleetid: str
            fleet id

        Returns
        -------
        pd.DataFrame
            aircrafts that match fleet id
        """
        assets = self.list_all()
        idx = [row.id for row in assets.itertuples() if fleetid in row.fleetIds]
        return assets[assets['id'].isin(idx)]
