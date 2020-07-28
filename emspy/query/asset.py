from builtins import object
import pandas as pd


class Asset(object):
    """
    Base class of all "asset" classes and the EMS class. It is a template class so
    should not be instantiated directly.
    """

    def __init__(self, conn, asset_type):
        """
        Asset initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        asset_type: str
            asset identifier
        """
        self._conn = conn
        self._asset_type = asset_type
        self._assets = None

    def update_list(self, uri_keys=None, uri_args=None, body=None, colsort=True):
        """
        Base method to update an asset list

        Parameters
        ----------
        uri_keys: tuple
            request uri keys
        uri_args: list, tuple
            request uri arguments
        body: dict
            request body
        colsort: bool
            Whether to sort the columns or not

        Returns
        -------
        None
        """
        _, dict_data = self._conn.request(uri_keys=uri_keys, uri_args=uri_args, body=body)
        assets = pd.DataFrame.from_dict(dict_data)
        if colsort:
            assets = assets[sorted(assets.columns, key=len)]
        self._assets = assets

    def list_all(self):
        """
        Method to list all assets

        Returns
        -------
        list
            listed assets
        """
        return self._assets

    def search(self, col, val=None, searchtype="contain"):
        """
        Method to search for a value in a column

        Parameters
        ----------
        col: str
            column to search in
        val: str
            value to search
        searchtype: str
            search type to perform:
                contain: uses the Pandas string method 'contains' on the column as string
                match: uses the Pandas string method 'match' on the column as string

        Returns
        -------
        str
            returns the found value
        """
        if val is None:
            return self.list_all()
        assets = self._assets
        if type(val) == str:
            if searchtype == "contain":
                return assets[assets[col].str.contains(val, na=False, case=False)]
            if searchtype == "match":
                return assets[assets[col].str.match(val, case=False)]
        return assets[assets[col] == val]

    def get_id(self, name=None, keyword=None):
        """
        Base method for getting an ID
        """
        raise NotImplementedError()

    def get_name(self, id_val=None):
        """
        Base method for getting a name from an ID
        """
        raise NotImplementedError()

    def _rename_datacol(self, old, new):
        # Rename a column
        self._assets = self._assets.rename(columns={old: new})

    def data_colnames(self):
        """
        Returns the columns for the asset dataframe

        Returns
        -------
        pd.Index
            dataframe columns
        """
        return self._assets.columns


def size(an_asset):
    if not issubclass(an_asset, Asset):
        raise TypeError("Input should be an object inherited from Asset class.")
    if an_asset.list_all() is None:
        return 0
    else:
        return an_asset.list_all().shape[0]
