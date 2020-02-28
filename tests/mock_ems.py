from __future__ import absolute_import


class MockEMS(object):

    def __init__(self, conn):
        pass

    def update_list(self):
        pass

    def get_id(self, name = None):
        return 24

    def get_name(self, id_val = None):
        return 'ems24'
