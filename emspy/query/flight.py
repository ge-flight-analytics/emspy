from __future__ import print_function
from builtins import object
from emspy.query import LocalData

import networkx as nx
import pandas as pd
import os, sys, re


class Flight(object):
    temp_exclude = [
        'Download Information',
        'Download Review',
        'Processing',
        'Profile 16 Extra Data',
        'Flight Review',
        'Data Information',
        'Operational Information',
        'Operational Information (ODW2)',
        'Weather Information',
        'Profiles',
        'Profile'
    ]

    def __init__(self, conn, ems_id, data_file=LocalData.default_data_file, searchtype='contain'):
        """
        Flight object initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        ems_id: int
            EMS system id
        data_file: str
            path to database file
        searchtype: str
            search type to perform:
                contain: uses the Pandas string method 'contains'
                match: uses the Pandas string method 'match'
        """
        self.searchtype = searchtype
        self._conn = conn
        self._ems_id = ems_id
        self._db_id = None
        self._metadata = None
        self._trees = {'fieldtree': None, 'dbtree': None, 'kvmaps': None}
        self._fields = []
        self.__cntr = 0
        self._uri_root = conn._uri_root

        # Retrieve the field tree data from local storage. If it doesn't exist, generate a new
        # default one.
        self.load_tree(data_file)
        # Set default database
        # self.set_database('fdw flight')

    def load_tree(self, file_name=LocalData.default_data_file):
        """
        Load trees into the object

        Parameters
        ----------
        file_name: str
            path to database file

        Returns
        -------
        None
        """
        if self._metadata is None:
            self._metadata = LocalData(file_name)
        elif not self._metadata.is_db_path_correct(file_name):
            self._metadata.close()
            self._metadata = LocalData(file_name)

        self._trees = {
            'fieldtree': self.__get_fieldtree(),
            'dbtree': self.__get_dbtree(),
            'kvmaps': self.__get_kvmaps()
        }

    def save_tree(self, file_name=LocalData.default_data_file):
        """
        Method to save trees to a defined database file

        Parameters
        ----------
        file_name: str
            path to database file

        Returns
        -------
        None
        """
        from shutil import copyfile
        ld = self._metadata
        if not ld.is_db_path_correct(file_name):
            # A different location is given, if a previous file exists copy it to the new location.
            if ld.file_loc() is not None and file_name is not None:
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
            return self._metadata.get_data(
                "fieldtree",
                "ems_id = %d and db_id = '%s'" % (self._ems_id, self._db_id)
            )

    def __save_fieldtree(self):
        if len(self._trees['fieldtree']) > 0:
            self._metadata.delete_data(
                "fieldtree",
                "ems_id = %d and db_id = '%s'" % (self._ems_id, self._db_id)
            )
            self._metadata.append_data("fieldtree", self._trees['fieldtree'])

    def __get_dbtree(self):
        tree = self._metadata.get_data("dbtree", self.__get_filter('dbtree'))
        if len(tree) < 1:
            dbroot = {
                'uri_root': self._uri_root,
                'ems_id': self._ems_id,
                'id': "[-hub-][entity-type-group][[--][internal-type-group][root]]",
                'name': "<root>",
                'nodetype': "root",
                'parent_id': None
            }
            self._trees['dbtree'] = pd.DataFrame([dbroot])
            self.__update_children(dbroot, treetype="dbtree")
            self.update_tree("fdw", treetype="dbtree", exclude_tree=["APM Events"])
            self.__save_dbtree()
            tree = self._trees['dbtree']
        return tree

    def __save_dbtree(self):
        if len(self._trees['dbtree']) > 0:
            self._metadata.delete_data("dbtree", self.__get_filter('dbtree'))
            self._metadata.append_data("dbtree", self._trees['dbtree'])

    def __get_kvmaps(self):
        tree = self._metadata.get_data("kvmaps", self.__get_filter('kvmaps'))
        return tree

    def __save_kvmaps(self):
        if len(self._trees['kvmaps']) > 0:
            self._metadata.delete_data("kvmaps", self.__get_filter('kvmaps'))
            self._metadata.append_data("kvmaps", self._trees['kvmaps'])

    def set_database(self, name):
        """
        Method to select a database from the available databases in dbtree

        Parameters
        ----------
        name: str
            database name

        Returns
        -------
        None
        """
        tree = self._trees['dbtree']
        if self.searchtype == 'contain':
            dbs = tree[(tree.nodetype == 'database')
                       & tree.name.str.contains(_treat_spchar(name), case=False)]
            # Return database with shortest name
            self._db_id = dbs.loc[dbs['name'].str.len().idxmin(), 'id']
        # Exact string matching
        elif self.searchtype == 'match':
            self._db_id = tree[(tree.nodetype == 'database') &
                tree.name.str.match(_treat_spchar(name), case=False)]['id'].values[0]
        self._trees['fieldtree'] = self.__get_fieldtree()
        if self._trees['fieldtree'].empty:
            self.__update_children(self.get_database(), treetype="fieldtree")
        print("Using database '%s'." % self.get_database()['name'])

    def get_database(self):
        """
        Method to get currently selected database from dbtree

        Returns
        -------
        dict
            dictionary for the selected database
        """
        tree = self._trees['dbtree']
        return tree[(tree.nodetype == "database") & (tree.id == self._db_id)].iloc[0].to_dict()

    def __db_request(self, parent):
        body = None
        if parent['nodetype'] == "database_group":
            body = {'groupId': parent['id']}
        resp_h, d = self._conn.request(
            uri_keys=('database', 'group'),
            uri_args=self._ems_id,
            body=body
        )
        d1 = []
        if len(d['databases']) > 0:
            d1 = [{'uri_root': parent['uri_root'] if 'uri_root' in parent.keys() else '',
                   'ems_id': parent['ems_id'],
                   'id': x['id'],
                   'nodetype': 'database',
                   'name': x['pluralName'],
                   'parent_id': parent['id']
                   } for x in d['databases']]
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
        resp_h, d = self._conn.request(
            uri_keys=('database', 'field_group'),
            uri_args=(self._ems_id, self._db_id),
            body=body
        )
        d1 = []
        if len(d['fields']) > 0:
            d1 = [{'uri_root': parent['uri_root'] if 'uri_root' in parent.keys() else '',
                   'ems_id': parent['ems_id'],
                   'db_id': self._db_id,
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

    def __add_subtree(self, parent, exclude_tree=[], treetype='fieldtree', exclude_subtrees=False):
        print("On " + parent['name'] + "(" + parent['nodetype'] + ")" + "...")
        if treetype == "dbtree":
            searchtype = 'database'
            d1, d2 = self.__db_request(parent)
        else:
            searchtype = "field"
            d1, d2 = self.__fl_request(parent)

        if len(d1) > 0:
            self._trees[treetype] = pd.concat([self._trees[treetype], pd.DataFrame.from_dict(d1)], axis=0, join='outer', ignore_index=True)
            plural = "s" if len(d1) > 1 else ""
            print("-- Added %d %s%s" % (len(d1), searchtype, plural))

        for x in d2:
            if exclude_subtrees:
                print("-- Excluded subtree: %s" % x['name'])
            else:
                self._trees[treetype] = pd.concat([self._trees[treetype], pd.DataFrame.from_dict([x])], axis=0, join='outer', ignore_index=True)
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

    def __remove_subtree(self, parent, treetype='fieldtree'):
        tr = self._trees[treetype]
        chld = tr[tr.parent_id == parent['id']]

        # Update the instance tree by deleting children
        self._trees[treetype] = tr[tr.parent_id != parent['id']]

        # Iterate and do recursive removal of children of children
        leaftype = 'field' if treetype == 'fieldtree' else 'database'
        for i, x in chld[chld.nodetype != leaftype].iterrows():
            self.__remove_subtree(x, treetype=treetype)

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
    #         print("Removed the subtree of %s (%s) with total of %d nodes "
    #               "(fields/databases/groups)."
    #               % (parent['name'], parent['nodetype'], len(rm_list)))

    def __update_children(self, parent, treetype='fieldtree'):
        # This function updates the direct children of a parent node.
        print("On " + parent['name'] + "(" + parent['nodetype'] + ")" + "...")
        if treetype == "dbtree":
            searchtype = 'database'
            d1, d2 = self.__db_request(parent)
        else:
            searchtype = "field"
            d1, d2 = self.__fl_request(parent)

        tree = self._trees[treetype]
        self._trees[treetype] = tree[~((tree.nodetype == searchtype)
                                       & (tree.parent_id == parent['id']))]

        if len(d1) > 0:
            # Make sure we have a dataframe if d1 is a list (it is in some cases)
            if isinstance(d1, pd.DataFrame) is False:
                d1 = pd.DataFrame(d1)
            self._trees[treetype] = pd.concat([self._trees[treetype], d1], axis=0, join='outer', ignore_index=True)
            plural = "s" if len(d1) > 1 else ""
            print("-- Added %d %s%s" % (len(d1), searchtype, plural))

        # If there is an array of groups as children add any that appeared new
        # and remove who does not.
        old_groups = tree[(tree.nodetype == '%s_group' % searchtype)
                          & (tree.parent_id == parent['id'])]
        old_ones = old_groups["id"].tolist()
        new_ones = [x['id'] for x in d2]

        rm_id = _listdiff(old_ones, new_ones)
        if len(rm_id) > 0:
            [self.__remove_subtree(x.to_dict(), treetype=treetype)
             for i, x in old_groups.iterrows() if x['id'] in rm_id]

        add_id = _listdiff(new_ones, old_ones)
        if len(add_id) > 0:
            if isinstance(d2, pd.DataFrame) is False:
                d2 = pd.DataFrame(d2)
            self._trees[treetype] = pd.concat([self._trees[treetype], d2], axis=0, join='outer', ignore_index=True)

    def update_tree(self, *args, **kwargs):
        """
        Parameters
        ----------
        args:
            paths
        kwargs:
            keyword arguments

        Returns
        -------
        None

        Keyword arguments
        -----------------
        treetype: str
            "fieldtree" or "dbtree"
        exclude_tree: list
            Exact name strings (case sensitive) of the field groups you don't want to search through
            Ex. ['Profiles', 'Weather Information']
        exclude_subtrees: bool
            whether to exclude all subtrees or not
        """
        treetype = kwargs.get("treetype", "fieldtree")
        exclude_tree = kwargs.get("exclude_tree", [])
        exclude_subtrees = kwargs.get("exclude_subtrees", False)
        searchtype = "field" if treetype == "fieldtree" else "database"

        if treetype not in ("fieldtree", "dbtree"):
            raise ValueError("treetype = '%s': there is no such data table." % treetype)

        fld_path = [s.lower() for s in args]

        for idx, path in enumerate(fld_path):
            path = _treat_spchar(path)
            if idx == 0:
                tree = self._trees[treetype]
                if self.searchtype == 'contain':
                    parent = tree[tree.name.str.contains(path, case=False)]
                elif self.searchtype == 'match':
                    parent = tree[tree.name.str.match(path, case=False)]
            else:
                self.__update_children(parent, treetype=treetype)
                chld_df = self.__get_children(parent['id'], treetype=treetype)
                if self.searchtype == 'contain':
                    child = chld_df[chld_df.name.str.contains(path, case=False)]
                elif self.searchtype == 'match':
                    child = chld_df[chld_df.name.str.match(path, case=False)]
                parent = child
            if len(parent) == 0:
                raise ValueError("Search keyword '%s' did not return any %s group."
                                 % (path, searchtype))
            ptype = "%s_group" % searchtype
            parent = parent[parent.nodetype == ptype]
            parent = _get_shortest(parent)

        print("=== Starting to update subtree from '%s (%s)' ==="
              % (parent['name'], parent['nodetype']))
        self.__remove_subtree(parent, treetype=treetype)
        self.__add_subtree(
            parent,
            exclude_tree,
            treetype=treetype,
            exclude_subtrees=exclude_subtrees
        )

    def make_default_tree(self):
        """
        Method to create a default tree

        Returns
        -------
        None
        """
        dbnode = self.get_database()
        self.__remove_subtree(dbnode, treetype="fieldtree")
        self.__add_subtree(
            self.get_database(),
            exclude_tree=Flight.temp_exclude,
            treetype='fieldtree'
        )
        # self.update_tree(
        #     self.get_database()['name'],
        #     exclude_tree=Flight.temp_exclude,
        #     treetype='fieldtree'
        # )

    def search_fields(self, *args, **kwargs):
        """
        This function searches through the field names and returns a list of tuples with
        (field_name, field_info). The input arg is a "keyword" for the field name. You can
        also add parent field group names as n-tuple.

        Examples
        --------
        search_fields("Takeoff Airport Code", "Landing Airport Code")

        search_fields(("Takeoff","Airport","Code"), ("Landing","Airport","Airport Code"))

        Parameters
        ----------
        args:
            fields
        kwargs:
            keyword arguments

        Keyword arguments
        -----------------
        unique: bool
            Indicates if the value with the shortest name will be returned

        Returns
        -------
        res: list
            list of dictionaries
        """
        unique = kwargs.get("unique", True)
        res = pd.DataFrame()
        for field in args:
            if isinstance(field, tuple) and len(field) > 1:
                # If the given keyword is a tuple, search through the tree
                child = self._trees['fieldtree']
                for idx, ff in enumerate(field):
                    ff = _treat_spchar(ff)
                    if idx < (len(field)-1):
                        child = child[child.nodetype == "field_group"]
                        if self.searchtype == 'contain':
                            parent_id = child[child.name.str.contains(ff, case=False)]['id'].tolist()
                        elif self.searchtype == 'match':
                            parent_id = child[child.name.str.match(ff, case=False)]['id'].tolist()
                        tr = self._trees['fieldtree']
                        child = tr[tr.parent_id.isin(parent_id)]
                    else:
                        if self.searchtype == 'contain':
                            child = child[(child.nodetype == "field")
                                          & child.name.str.contains(ff, case=False)]
                        elif self.searchtype == 'match':
                            child = child[(child.nodetype == "field")
                                          & child.name.str.match(ff, case=False)]
                fres = child
            else:
                # Simple keyword search
                tree = self._trees['fieldtree']
                if self.searchtype == 'contain':
                    fres = tree[(tree.nodetype == "field")
                                & tree.name.str.contains(_treat_spchar(field), case=False)]
                elif self.searchtype == 'match':
                    fres = tree[(tree.nodetype == "field")
                                & tree.name.str.match(_treat_spchar(field), case=False)]

            if fres.shape[0] == 0:
                # No returned value. Raise error.
                raise ValueError("No field found with field keyword %s." % field)
            elif fres.shape[0] > 1:
                if unique:
                    # If more than one value returned, choose one with the shortest name.
                    fres = _get_shortest(fres)

            # Make sure we have a dataframe if fres is a dict (it is in some cases)
            if isinstance(fres, pd.DataFrame) is False:
                fres = pd.DataFrame(fres, index=[0])

            res = pd.concat([res, fres], axis=0, join='outer', ignore_index=True)

        # Convert the search result to a list of dicts
        res = [x[1].to_dict() for x in res.iterrows()]
        return res

    def list_allvalues(self, field=None, field_id=None, in_dict=False, in_df=False):
        """
        List all available values for a discrete field. Will raise error if the type of
        a given field is not discrete.

        Parameters
        ----------
        field: str
            field name
        field_id: str
            field id
        in_dict: bool
            returns output as a dictionary if set to True
        in_df: bool
            returns output as a pandas dataframe if set to True

        Returns
        -------
        dict | pd.DataFrame
            listed fields
        """
        if field_id is None:
            field = self.search_fields(field)[0]
            field_type = field['type']
            field_id = field['id']
            field_name = field['name']
            if field_type != 'discrete':
                sys.exit("Queried field should be discrete to get the list of possible values.")
        else:
            field_id = field_id
            tree = self._trees['fieldtree']
            field_name = tree.loc[tree.id == field_id, 'name'].values[0]

        tree = self._trees['kvmaps']
        kmap = tree[(tree.ems_id == self._ems_id) & (tree.id == field_id)]

        if len(kmap) == 0:
            print("%s: Getting key-value mappings from API. "
                  "(Caution: Some fields take a very long time)" % field_name)

            _, content = self._conn.request(
                uri_keys=('database', 'field'),
                uri_args=(self._ems_id, self._db_id, field_id)
            )
            km = content['discreteValues']
            kmap = pd.DataFrame({
                'uri_root': self._uri_root,
                'ems_id': self._ems_id,
                'id': field_id,
                'key': list(km.keys()),
                'value': list(km.values())
            })
            kmap['key'] = pd.to_numeric(kmap['key'])
            self._trees['kvmaps'] = pd.concat([self._trees['kvmaps'], kmap], axis=0, join='outer', ignore_index=True)
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
        """
        Return the key (id) of the values of a discrete field.

        Parameters
        ----------
        value: str
            value name
        field: str
            field name
        field_id: str
            field id

        Returns
        -------
        int
            value id
        """
        kvmap = self.list_allvalues(field=field, field_id=field_id, in_df=True)
        key = kvmap[kvmap.value == value]['key']
        if len(key) == 0:
            raise ValueError("%s could not be found from the list of the field values." % value)
        return int(key.values[0])

    def __get_filter(self, tree):
        query_filter = "ems_id = %d" % self._ems_id
        if 'uri_root' in self._metadata.get_data(tree).columns:
            query_filter += " AND uri_root = '%s'" % self._uri_root
        return query_filter


def _get_shortest(fields):
    if isinstance(fields, pd.DataFrame) is False:
        sys.exit("Input should be a Pandas dataframe.")
    fields.reset_index(inplace=True)
    return fields.loc[fields.name.str.len().idxmin()].to_dict()


def _listdiff(a, b):
    return [x for x in a if x not in b]


def _treat_spchar(s):
    sp_chr = (
        ".^()[]{}<>-+?!*$|&%"
    )
    for x in sp_chr:
        s = s.replace(x, "\\"+x)
    return s
