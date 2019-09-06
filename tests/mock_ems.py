from __future__ import absolute_import


class MockEMS(object):
    def update_list(self):
        pass

    def get_id(self, name = None):
        return 3

    def get_name(self, id_val = None):
        return 'ems22'
