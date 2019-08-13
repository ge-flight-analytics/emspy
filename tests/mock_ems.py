from __future__ import absolute_import
from emspy.query.asset import Asset


class MockEMS(Asset):
    def __init__(self, conn):
        Asset.__init__(self, conn, "EMS")
        self.update_list()


    def update_list(self):
        Asset.update_list(self, uri_keys=('ems_sys', 'list'))


    def get_id(self, name = None):
        return 3


    def get_name(self, id_val = None):
        a = self.search('id', id_val, searchtype="match")['name'].tolist()
        return a if len(a) > 1 else a[0]


