from __future__ import print_function
from builtins import object
from emspy.query import LocalData

import networkx as nx
import pandas as pd
import os, sys, re


class Flight(object):
    temp_exclude = ['Download Information', 'Download Review', 'Processing',
                    'Profile 16 Extra Data', 'Flight Review', 'Data Information',
                    'Operational Information', 'Operational Information (ODW2)',
                    'Weather Information', 'Profiles', 'Profile']


    def __init__(self, conn, ems_id, data_file=LocalData.default_data_file):

        self._conn = conn
        self._ems_id = ems_id
        self._db_id  = None
        self._metadata = None
        self._trees  = {'fieldtree': None, 'dbtree': None, 'kvmaps': None}
        self._fields = []
        self.__cntr = 0
        self._uri_root = self.__get_uri_root()

        # Retreive the field tree data from local storage. If it doesn't exist, generate a new
        # default one.
        self.load_tree(data_file)
        # Set default database
        # self.set_database('fdw flight')

    def load_tree(self, file_name=LocalData.default_data_file):

        if self._metadata is None:
            self._metadata = LocalData(file_name)
        elif not self._metadata.is_db_path_correct(file_name):
            self._metadata.close()
            self._metadata = LocalData(file_name)

        self._trees = {'fieldtree': self.__get_fieldtree(),
                       'dbtree'   : self.__get_dbtree(),
                       'kvmaps'   : self.__get_kvmaps()}


    def save_tree(self, file_name=LocalData.default_data_file):

        from shutil import copyfile
        ld = self._metadata
        if not ld.is_db_path_correct(file_name):
            # A different location is given, if a previous file exists copy it to the new location.
            if ld.file_loc() != None and file_name != None:
                copyfile(ld.file_loc(), os.path.abspath(file_name))

            self._metadata.close()
            self._metadata = LocalData(file_name)

        self.__save_fieldtree()
        self.__save_dbtree()
        self.__save_kvmaps()



    def __get_fieldtree(self):
        if self._db_id is None:
            return pd.DataFrame(columns=self._metadata.table_info['fieldtree'])
        else:
            return self._metadata.get_data("fieldtree","ems_id = %d and db_id = '%s'" % (self._ems_id, self._db_id))


    def __save_fieldtree(self):
        if len(self._trees['fieldtree']) > 0:
            self._metadata.delete_data("fieldtree", "ems_id = %d and db_id = '%s'" % (self._ems_id, self._db_id))
            self._metadata.append_data("fieldtree", self._trees['fieldtree'])


    def __get_dbtree(self):
        T = self._metadata.get_data("dbtree", self.__get_filter('dbtree'))
        if len(T) < 1:
            dbroot = {
                'uri_root': self._uri_root,
                'ems_id': self._ems_id,
                'id': "[-hub-][entity-type-group][[--][internal-type-group][root]]",
                'name': "<root>",
                'nodetype': "root",
                'parent_id': None
            }
            self._trees['dbtree'] = pd.DataFrame([dbroot])
            self.__update_children(dbroot, treetype = "dbtree")
            self.update_tree("fdw", treetype = "dbtree", exclude_tree = ["APM Events"])
            self.__save_dbtree()
            T = self._trees['dbtree']
        return T


    def __save_dbtree(self):
        if len(self._trees['dbtree']) > 0:
            self._metadata.delete_data("dbtree", self.__get_filter('dbtree'))
            self._metadata.append_data("dbtree", self._trees['dbtree'])


    def __get_kvmaps(self):
        T = self._metadata.get_data("kvmaps", self.__get_filter('kvmaps'))
        return T


    def __save_kvmaps(self):
        if len(self._trees['kvmaps']) > 0:
            self._metadata.delete_data("kvmaps", self.__get_filter('kvmaps'))
            self._metadata.append_data("kvmaps", self._trees['kvmaps'])


    def set_database(self, name):

        tr = self._trees['dbtree']
        self._db_id = tr[(tr.nodetype == 'database') & tr.name.str.contains(treat_spchar(name), case=False)]['id'].values[0]
        self._trees['fieldtree'] = self.__get_fieldtree()

        if self._trees['fieldtree'].empty:
            self.__update_children(self.get_database(), treetype = "fieldtree")

        print("Using database '%s'." % self.get_database()['name'])


    def get_database(self):

        tr = self._trees['dbtree']
        return tr[(tr.nodetype=="database") & (tr.id == self._db_id)].iloc[0].to_dict()



    def __db_request(self, parent):
        body = None
        if parent['nodetype'] == "database_group":
            body = {'groupId': parent['id']}
        resp_h, d = self._conn.request(uri_keys=('database', 'group'),
                                       uri_args=self._ems_id,
                                       body=body)
        d1 = []
        if len(d['databases']) > 0:
            d1 = [{'uri_root': parent['uri_root'] if 'uri_root' in parent.keys() else '',
                   'ems_id': parent['ems_id'],
                   'id': x['id'],
                   'nodetype': 'database',
                   'name': x['pluralName'],
                   'parent_id': parent['id']} for x in d['databases']]
            for _d in d1:
                if _d['uri_root'] == '':
                    _d.pop('uri_root')
        d2 = []
        if len(d['groups']) > 0:
            d2 = [{'uri_root': parent['uri_root'] if 'uri_root' in parent.keys() else '',
                   'ems_id': parent['ems_id'],
                   'id': x['id'],
                   'nodetype': 'database_group',
                   'name': x['name'],
                   'parent_id': parent['id']} for x in d['groups']]
            for _d in d2:
                if _d['uri_root'] == '':
                    _d.pop('uri_root')
        return d1, d2


    def __fl_request(self, parent):
        body = None
        if parent['nodetype'] == "field_group":
            body = {'groupId': parent['id']}
        resp_h, d = self._conn.request(uri_keys=('database', 'field_group'),
                                       uri_args=(self._ems_id, self._db_id),
                                       body=body)
        d1 = []
        if len(d['fields']) > 0:
            d1 = [{'uri_root': parent['uri_root'] if 'uri_root' in parent.keys() else '',
                   'ems_id': parent['ems_id'],
                   'db_id' : self._db_id,
                   'id': x['id'],
                   'nodetype': 'field',
                   'type': x['type'],
                   'name': x['name'],
                   'parent_id': parent['id']} for x in d['fields']]
            for _d in d1:
                if _d['uri_root'] == '':
                    _d.pop('uri_root')
        d2 = []
        if len(d['groups']) > 0:
            d2 = [{'uri_root': parent['uri_root'] if 'uri_root' in parent.keys() else '',
                   'ems_id': parent['ems_id'],
                   'db_id': self._db_id,
                   'id': x['id'],
                   'nodetype': 'field_group',
                   'type': None,
                   'name': x['name'],
                   'parent_id': parent['id']} for x in d['groups']]
            for _d in d2:
                if _d['uri_root'] == '':
                    _d.pop('uri_root')
        return d1, d2


    def __add_subtree(self, parent, exclude_tree=[], treetype = 'fieldtree'):

        print("On " + parent['name'] + "(" + parent['nodetype'] + ")" + "...")

        if treetype=="dbtree":
            searchtype = 'database'
            d1, d2 = self.__db_request(parent)

        else:
            searchtype = "field"
            d1, d2 = self.__fl_request(parent)

        if len(d1) > 0:
            self._trees[treetype] = self._trees[treetype].append(d1, ignore_index=True)
            plural = "s" if len(d1) > 1 else ""
            print("-- Added %d %s%s" % (len(d1), searchtype, plural))

        for x in d2:
            self._trees[treetype] = self._trees[treetype].append(x, ignore_index=True)
            if len(exclude_tree) > 0:
                if all([y not in x['name'] for y in exclude_tree]):
                    self.__add_subtree(x, exclude_tree, treetype)
            else:
                self.__add_subtree(x, exclude_tree, treetype)


    def __get_children(self, parent_id, treetype='fieldtree'):
        tr = self._trees[treetype]
        # tr = tr[tr.nodetype != ('field' if treetype=='fieldtree' else 'dbtree')]

        if isinstance(parent_id, (list, tuple, pd.Series)):
            return tr[tr.parent_id.isin(parent_id)]
        return tr[tr.parent_id == parent_id]


    def __remove_subtree(self, parent, treetype = 'fieldtree'):
        tr = self._trees[treetype]
        chld = tr[tr.parent_id == parent['id']]

        # Update the instance tree by deleting children
        self._trees[treetype] = tr[tr.parent_id != parent['id']]

        # Iterate and do recursive removal of children of children
        leaftype = 'field' if treetype=='fieldtree' else 'database'
        for i, x in chld[chld.nodetype!=leaftype].iterrows():
            self.__remove_subtree(x, treetype = treetype)


    # def __remove_subtree(self, parent, rm_parent=True, treetype='fieldtree'):

    #     rm_list = list()
    #     if rm_parent:
    #         rm_list.append(parent['id'])
    #     parent_id = [parent['id']]

    #     cntr = 0
    #     while len(parent_id) > 0:
    #         child_id = self.__get_children(parent_id, treetype=treetype)['id'].tolist()
    #         rm_list += child_id
    #         parent_id = child_id
    #         cntr += 1
    #         if cntr > 1e4:
    #             sys.exit("Something's wrong. Subtree removal went over 10,000 iterations.")
    #     if len(rm_list) > 0:
    #         tr = self._trees[treetype]
    #         self._trees[treetype] = tr[~tr.id.isin(rm_list)]
    #         print("Removed the subtree of %s (%s) with total of %d nodes (fields/databases/groups)." % (
    #             parent['name'], parent['nodetype'], len(rm_list)))



    def __update_children(self, parent, treetype='fieldtree'):
        '''
        This function updates the direct children of a parent node.
        '''
        print("On " + parent['name'] + "(" + parent['nodetype'] + ")" + "...")

        if treetype=="dbtree":
            searchtype = 'database'
            d1, d2 = self.__db_request(parent)

        else:
            searchtype = "field"
            d1, d2 = self.__fl_request(parent)

        T = self._trees[treetype]
        self._trees[treetype] = T[~((T.nodetype == searchtype) & (T.parent_id == parent['id']))]

        if len(d1) > 0:
            self._trees[treetype] = self._trees[treetype].append(d1, ignore_index=True)
            plural = "s" if len(d1) > 1 else ""
            print("-- Added %d %s%s" % (len(d1), searchtype, plural))

        # If there is an array of groups as children add any that appeared new and remove who does not.
        old_groups = T[(T.nodetype == '%s_group' % searchtype) & (T.parent_id == parent['id'])]
        old_ones = old_groups["id"].tolist()
        new_ones = [x['id'] for x in d2]

        rm_id = listdiff(old_ones, new_ones)
        if len(rm_id) > 0:
            [self.__remove_subtree(x.to_dict(), treetype=treetype) for i, x in old_groups.iterrows() if x['id'] in rm_id]

        add_id = listdiff(new_ones, old_ones)
        if len(add_id) > 0:
            self._trees[treetype] = self._trees[treetype].append([x for x in d2 if x['id'] in add_id])



    def update_tree(self, *args, **kwargs):
        '''
        Optional arguments
        ------------------

        treetype :
            "fieldtree" or "dbtree"

        exclude_tree:
            Exact name strings (case sensitive) of the field groups you don't want to search through
            Ex. ['Profiles', 'Weather Information']
        '''
        treetype     = kwargs.get("treetype", "fieldtree")
        exclude_tree = kwargs.get("exclude_tree", [])
        searchtype   = "field" if treetype == "fieldtree" else "database"

        if treetype not in ("fieldtree", "dbtree"):
            raise ValueError("treetype = '%s': there is no such data table." % treetype)

        fld_path = [s.lower() for s in args]

        for i, p in enumerate(fld_path):
            p = treat_spchar(p)
            if i == 0:
                T = self._trees[treetype]
                parent = T[T.name.str.contains(p, case=False)]
            else:
                self.__update_children(parent, treetype=treetype)
                chld_df = self.__get_children(parent['id'], treetype= treetype)
                child = chld_df[chld_df.name.str.contains(p, case=False)]
                parent = child
            if len(parent) == 0:
                raise ValueError("Search keyword '%s' did not return any %s group." % (p, searchtype))
            ptype  = "%s_group" % searchtype
            parent = parent[parent.nodetype == ptype]
            parent = get_shortest(parent)

        print("=== Starting to update subtree from '%s (%s)' ===" % (parent['name'], parent['nodetype']))
        self.__remove_subtree(parent, treetype=treetype)

        self.__add_subtree(parent, exclude_tree, treetype=treetype)



    def make_default_tree(self):
        dbnode = self.get_database()
        self.__remove_subtree(dbnode, treetype="fieldtree")
        self.__add_subtree(self.get_database(), exclude_tree=Flight.temp_exclude, treetype='fieldtree')
        # self.update_tree(self.get_database()['name'], exclude_tree=Flight.temp_exclude, treetype='fieldtree')


    def search_fields(self, *args, **kwargs):
        '''
        This function search through the field names and returns a list of tuples with
        (field_name, field_info). The input arg is a "keyword" for the field name. You can
        also add parent field group names as n-tuple.

        ex)
        ---
        search_fields("Takeoff Airport Code", "Landing Airport Code")
        search_fields(("Takeoff","Airport","Code"), ("Landing","Airport","Airport Code"))       '''

        unique = kwargs.get("unique", True)

        res = pd.DataFrame()

        for f in args:
            if type(f) is tuple and len(f) > 1:
                # If the given keyword is a tuple, search through the tree
                chld = self._trees['fieldtree']
                for i, ff in enumerate(f):
                    ff = treat_spchar(ff)
                    if i < (len(f)-1):
                        chld      = chld[chld.nodetype == "field_group"]
                        parent_id = chld[chld.name.str.contains(ff, case=False)]['id'].tolist()
                        tr        = self._trees['fieldtree']
                        chld      = tr[tr.parent_id.isin(parent_id)]
                    else:
                        chld = chld[(chld.nodetype == "field") & chld.name.str.contains(ff, case=False)]
                fres = chld
            else:
                # Simple keyword search
                T = self._trees['fieldtree']
                fres = T[(T.nodetype == "field") & T.name.str.contains(treat_spchar(f), case=False)]

            if fres.shape[0] == 0:
                # No returned value. Raise error.
                raise ValueError("No field found with field keyword %s." % f)
            elif fres.shape[0] > 1:
                if unique:
                    # If more than one value returned, choose one with the shortest name.
                    fres = get_shortest(fres)
            res = res.append(fres, ignore_index=True)

        # Convert the search result to a list of dicts
        res = [x[1].to_dict() for x in res.iterrows()]
        return res

    def list_allvalues(self, field=None, field_id=None, in_dict=False, in_df=False):
        '''
        List all available values for a discrete field. Will raise error if the type of
        a given field is not discrete.
        '''
        if field_id is None:
            fld = self.search_fields(field)[0]
            fld_type = fld['type']
            fld_id = fld['id']
            fld_name = fld['name']
            if fld_type != 'discrete':
                sys.exit("Queried field should be discrete to get the list of possible values.")
        else:
            fld_id = field_id
            tr     = self._trees['fieldtree']
            fld_name = tr.loc[tr.id == fld_id, 'name'].values[0]

        T = self._trees['kvmaps']
        kmap = T[(T.ems_id == self._ems_id) & (T.id == fld_id)]

        if len(kmap) == 0:
            print("%s: Getting key-value mappings from API. (Caution: Some fields take a very long time)" % fld_name)

            resp_h, content = self._conn.request(uri_keys=('database', 'field'),
                                                 uri_args=(self._ems_id, self._db_id, fld_id))
            km = content['discreteValues']
            kmap = pd.DataFrame({
                'uri_root': self._uri_root,
                'ems_id': self._ems_id,
                'id': fld_id,
                'key': list(km.keys()),
                'value': list(km.values())
            })
            kmap['key'] = pd.to_numeric(kmap['key'])

            self._trees['kvmaps'] = self._trees['kvmaps'].append(kmap, ignore_index=True)
            self.__save_kvmaps()

        if in_dict:
            res = dict()
            for i, r in kmap.iterrows():
                res[r['key']] = r['value']
            return res

        if in_df:
            return kmap[['key', 'value']]
        return kmap['value'].tolist()

    def get_value_id(self, value, field=None, field_id=None):
        '''
        Return the key (Id) of the values of a discrete field.
        '''
        kvmap = self.list_allvalues(field=field, field_id=field_id, in_df=True)
        key = kvmap[kvmap.value == value]['key']

        if len(key) == 0:
            raise ValueError("%s could not be found from the list of the field values." % value)
        return int(key.values[0])

    def __get_filter(self, tree):
        filter = "ems_id = %d" % self._ems_id
        if 'uri_root' in self._metadata.get_data(tree).columns:
            filter += " AND uri_root = '%s'" % self._uri_root
        return filter

    def __get_uri_root(self):
        return [value for key, value in self._conn.__dict__.items() if 'uri_root' in key][0]


def get_shortest(fields):
    if isinstance(fields, pd.DataFrame) is False:
        sys.exit("Input should be a Pandas dataframe.")
    return fields.loc[fields.name.str.len().idxmin()].to_dict()


def listdiff(a, b):
    return [x for x in a if x not in b]


def treat_spchar(s):
    sp_chr = ("." ,"^" ,"(" ,")" ,"[" ,"]" ,"{", "}","<", ">","-", "+", "?", "!", "*", "$", "|", "&","%")
    for x in sp_chr:
        s = s.replace(x, "\\"+x)
    return s
