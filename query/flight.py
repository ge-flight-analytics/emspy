import networkx as nx
import os, sys, re
import emspy

class Flight:

    temp_exclude = ['Download Information','Download Review','Processing',
                    'Profile 16 Extra Data','Operational Information',
                    'Operational Information (ODW2)','Profiles']
    save_file_fmt = os.path.join(emspy.__path__[0],"data","FDW_flt_data_tree_ems_id_%s.cpk")

    def __init__(self, conn, ems_id, new_data = False):

        self._conn   = conn
        self._ems_id = ems_id
        self._tree   = None
        self._fields = []
        self.__cntr  = 0
        self._key_maps = dict()
        
        if self.__treefile_exists() and not new_data:
            self.load_tree()
        else:
            print("No file found for default data-field tree. Will start generating one...")
            self.generate_tree()
            print("done.")
        self.__database  = self.get_database()


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

        res = []
        g   = self._tree

        for f in args:
            if type(f) is tuple and len(f) > 1:
                f = [x.lower() for x in f]
                node_id = [n[0] for n in g.nodes_iter(data=True)\
                          if n[1]['nodetype']=='field_group' and f[0] in n[1]['name'].lower()]
                for kwd in f[1:]:
                    tn = []
                    for i in node_id:
                        tn += [n for n in g.successors(i) if kwd in g.node[n]['name'].lower()]
                    node_id = tn                    
                fres = [(g.node[n]['name'], g.node[n]) for n in node_id]
            else:
                f = f.lower()
                fres = [(n[1]['name'], n[1]) for n in g.nodes_iter(data=True)\
                        if n[1]['nodetype']=='field' and f in n[1]['name'].lower()]
            if len(fres)!=0:
                if unique:
                    res.append(get_shortest(fres)) 
                else:
                    res += fres
            else:
                raise ValueError("Nothing with Field keyword '%s' was found in the EMS." % f)
        if len(res) == 0: res = None
        elif len(res) == 1: res = res[0]
        return res

    
    def list_allvalues(self, field=None, field_id = None, in_dict=False):
        '''
        List all available values for a discrete field. Will raise error if the type of
        a given field is not discrete.
        '''
        if field_id is None:
            fld = self.search_fields(field)
            fld_type = fld[1]['type']
            fld_id   = fld[1]['id']
            if fld_type != 'discrete':
                sys.exit("Queried field should be discrete to get the list of possible values.")
        else:
            fld_id = field_id

        if self._key_maps.has_key(fld_id):
            kmap = self._key_maps[fld_id]
        else:
            db_id = self.get_database()['id']
            print("Getting key-value mappings from API. (Caution: runway ID takes much longer)")

            resp_h, content = self._conn.request( uri_keys=('database','field'),
                                                  uri_args=(self._ems_id, db_id, fld_id))
            kmap = content['discreteValues']
            self._key_maps[fld_id] = kmap

        if in_dict:
            return kmap

        return kmap.values()


    def get_value_id(self, value, field=None, field_id = None):
        '''
        Return the key (Id) of the values of a discrete field.
        '''
        val_map = self.list_allvalues(field=field, field_id=field_id, in_dict=True)
        for k, v in val_map.iteritems():
            if v == value:
                return int(k)
        else:
            raise ValueError("%s could not be found from the list of the field values." % value)


    def get_database(self):

        for n in self._tree.nodes_iter():
            if self._tree.node[n]['nodetype'] == 'database':
                return self._tree.node[n]
        return None


    def generate_tree(self):

        print("==== Start generating default data-field tree ====")
        # New tree
        self._tree = nx.DiGraph()
        
        # Find source id of FDW Flight and put it as root of tree
        resp_h, d = self._conn.request(
            uri_keys = ('database','group'), 
            uri_args = self._ems_id
            )
        for x in d['groups']:
            if x['name'] == 'FDW':
                fdw = x
                break
        resp_h, d = self._conn.request(
            uri_keys = ('database','group'),
            uri_args = self._ems_id,
            body     = {'groupId': fdw['id']}
            )
        for x in d['databases']:
            if x['singularName'] == "FDW Flight":
                fdw_flt             = x
                fdw_flt['name']     = fdw_flt['singularName']
                fdw_flt['nodetype'] = 'database'
                break
        self._tree.add_node(fdw_flt['id'], attr_dict=fdw_flt)

        # Cache the data source node reference just for convenience
        self.__database = self.get_database()

        # Add rest of the fields/groups recursively
        self.__add_subtree(fdw_flt)  
        # Final save
        self.save_tree()


    def update_tree(self, *args):
        
        grp_path = [s.lower() for s in args]
        G = self._tree

        for i, p in enumerate(grp_path):
            if i == 0:
                parent = [ G.node[n] for n in G.nodes_iter() \
                                    if re.search(p, G.node[n]['name'].lower()) or\
                                       p in G.node[n]['name'].lower() ]     
            else:
                self.__update_children(parent)
                parent = [ G.node[n] for n in G.successors(parent['id']) \
                                    if re.search(p, G.node[n]['name'].lower()) or\
                                       p in G.node[n]['name'].lower() ]
            if len(parent) == 0:
                raise ValueError("Search keyword '%s' did not return any field group." % args[i])
            parent = get_shortest(parent)

        print "=== Starting to update subtree from '%s' ===" % parent['name']
        self.__remove_subtree(parent, rm_parent=False)
        self.__add_subtree(parent)



    def save_tree(self, file_name = None):

        import cPickle as p

        if file_name is None: 
            file_name = Flight.save_file_fmt % self._ems_id
        p.dump(self._tree, open(file_name, "wb"))


    def load_tree(self, file_name = None):

        import cPickle as p

        if file_name is None: 
            file_name = Flight.save_file_fmt % self._ems_id
        self._tree = p.load(open(file_name, "rb"))


    def __add_subtree(self, parent):

        # Temp patch. save the tree for every 10 recursive calls
        # self.__cntr += 1
        # if self.__cntr >= 50:
        #     self.save_tree() 
        #     self.__cntr = 0

        print("On " + parent['name'] + "...")
        body = None
        if parent['nodetype'] == 'field_group':
            body = {'groupId': parent['id']}

        resp_h, d = self._conn.request(uri_keys = ('database','field_group'),
                                       uri_args = (self._ems_id, self.__database['id']),
                                       body = body
                                       )
        if len(d['fields']) > 0:
            for x in d['fields']: 
                x['nodetype'] = 'field'
            self._tree.add_nodes_from([ (x['id'], x) for x in d['fields'] ])
            self._tree.add_edges_from([ (parent['id'], x['id']) for x in d['fields'] ])
            plural = "s" if len(d['fields']) > 1 else ""
            print("-- Added %d field%s" % (len(d['fields']), plural))


        if len(d['groups']) > 0:
            for x in d['groups']:
                x['nodetype'] = 'field_group'
                self._tree.add_node(x['id'], attr_dict = x)
                self._tree.add_edge(parent['id'], x['id'])
                # Temp patch: exclude less usefule field group to save request calls
                if x['name'] not in Flight.temp_exclude:
                    self.__add_subtree(x)


    def __remove_subtree(self, parent, rm_parent = True):
        
        subtree_nodes = nx.descendants(self._tree, parent['id'])
        if rm_parent:
            subtree_nodes.add(parent['id'])
        if len(subtree_nodes) == 0:
            # Do nothing and leave if there is no node to remove
            return None
        print('Removing all fields and groups under "%s"...' % parent['name'])
        self._tree.remove_nodes_from(subtree_nodes)
        print("Done")
        


    def __update_children(self, parent):
        
        # If node type is "field_group", pass the field group id to the GET request to get the
        # field-group specific information.
        body = None
        if parent['nodetype'] == 'field_group':
            body = {'groupId': parent['id']}

        resp_h, d = self._conn.request(uri_keys = ('database','field_group'),
                                       uri_args = (self._ems_id, self.__database['id']),
                                       body = body)
        # For the fields, simply remove and re-create the children fields as an update
        old_fld = [n for n in self._tree.successors(parent['id']) \
                    if self._tree.node[n]['nodetype'] == 'field']
        self._tree.remove_nodes_from(old_fld)
        if len(d['fields']) > 0:
            for x in d['fields']: 
                x['nodetype'] = 'field'
            self._tree.add_nodes_from([ (x['id'], x) for x in d['fields'] ])
            self._tree.add_edges_from([ (parent['id'], x['id']) for x in d['fields'] ])
            
        # If there is an array of field group as children add them to the tree and call the function
        # recursively until reaches the fields (leaves).
        if len(d['groups']) > 0:
            for x in d['groups']:
                x['nodetype'] = 'field_group'
            old         = [n for n in self._tree.successors(parent['id']) 
                                if self._tree.node[n]['nodetype'] == 'field_group']
            new         = [n['id'] for n in d['groups']]
            id_to_rm    = listdiff(old, new)
            [self.__remove_subtree(n) for n in id_to_rm]
            id_to_add   = listdiff(new, old)
            self._tree.add_nodes_from([ (x['id'], x) for x in d['groups'] if x['id'] in id_to_add ])
            self._tree.add_edges_from([ (parent['id'], x) for x in id_to_add ])



    def __treefile_exists(self):

        return os.path.exists(Flight.save_file_fmt % self._ems_id)


def name_only(fields):

    if type(fields[0]) == dict:
        return [f['name'] for f in fields]
    elif type(fields[0]) in [tuple, list]:
        return [f[0] for f in fields]


def get_shortest(fields):

    names = name_only(fields)
    l = [len(n) for n in names]
    return fields[l.index(min(l))]


def listdiff(a, b):
    return [x for x in a if x not in b]


# def search_in_graph(G, **kwargs):
















