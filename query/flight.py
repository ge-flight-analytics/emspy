import networkx as nx
import os, pems

class Flight:

	temp_exclude = ['Download Information','Download Review','Processing',
					'Profile 16 Extra Data','Operational Information',
					'Operational Information (ODW2)','Austin Digital',
					'FCS Users','Training','FES Customer-Created Profiles','GRC RIO']
	save_file_fmt = os.path.join(pems.__path__[0],"data","FDW_flt_data_tree_ems_id_%s.cpk")

	def __init__(self, conn, ems_id):

		self._conn   = conn
		self._ems_id = ems_id
		self._tree   = None
		self._fields = []
		self.__cntr = 0
		if self.__treefile_exists():
			self.__load_tree()


	def search_fields(self, *args, **kwargs):
		'''
		This function search through the field names and returns a list of tuples with 
		(field_name, field_info). The input arg is a "keyword" for the field name. You can 
		also add parent field group names as n-tuple.
		
		ex)
		---
		search_fields(["Takeoff Airport Code", "Landing Airport Code"])
		search_fields([("Takeoff","Airport","Code"), ("Landing","Airport","Airport Code")])		'''

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
					res.append(get_best_match(fres)) 
				else:
					res += fres
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

		dsrc_id = self.get_datasource()[1]['id']


		resp_h, content = self._conn.request( uri_keys=('data_src','field'),
											  uri_args=(self._ems_id, dsrc_id), 
											  body = {'fieldId': fld_id})
		if in_dict:
			return content['discreteValues']
		values = content['discreteValues'].values()
		return values


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


	def get_datasource(self):

		for n in self._tree.nodes_iter(data=True):
			if n[1]['nodetype'] == 'data_source':
				return n
		return None


	def _update_tree(self):

		# New tree
		self._tree = nx.DiGraph()
		
		# Find source id of FDW Flight and put it as root of tree
		resp_h, d = self._conn.request(
			uri_keys = ('data_src','group'), 
			uri_args = self._ems_id
			)
		for x in d['groups']:
			if x['name'] == 'FDW':
				fdw = x
				break
		resp_h, d = self._conn.request(
			uri_keys = ('data_src','group'),
			uri_args = self._ems_id,
			body     = {'dataSourceGroupId': fdw['id']}
			)
		for x in d['dataSources']:
			if x['singularName'] == "FDW Flight":
				fdw_flt 			= x
				fdw_flt['name']     = fdw_flt['singularName']
				fdw_flt['nodetype'] = 'data_source'
				break
		self._tree.add_node(fdw_flt['id'], attr_dict=fdw_flt)

		# Add rest of the fields/groups recursively
		self.__recur_addnodes(fdw_flt, fdw_flt)  
		# Final save
		self.__save_tree()



	def __recur_addnodes(self, parent, root):

		# Temp patch. save the tree for every 10 recursive calls
		self.__cntr += 1
		if self.__cntr >= 50:
			self.__save_tree() 
			self.__cntr = 0

		print("On " + parent['name'] + "...")
		body = None
		if parent['nodetype'] == 'field_group':
			body = {'fieldGroupId': parent['id']}

		resp_h, d = self._conn.request(uri_keys = ('data_src','field_group'),
									   uri_args = (self._ems_id, root['id']),
									   body = body
									   )
		if len(d['fields']) > 0:
			for x in d['fields']: 
				x['nodetype'] = 'field'
			self._tree.add_nodes_from([ (x['id'], x) for x in d['fields'] ])
			self._tree.add_edges_from([ (parent['id'], x['id']) for x in d['fields'] ])

		if len(d['groups']) > 0:
			for x in d['groups']:
				x['nodetype'] = 'field_group'
				# Temp patch: exclude less usefule field group to save request calls
				if x['name'] not in Flight.temp_exclude:
					self._tree.add_node(x['id'], attr_dict = x)
					self._tree.add_edge(parent['id'], x['id'])
					self.__recur_addnodes(x, root)


	def __save_tree(self):

		import cPickle as p
		p.dump(self._tree, open(Flight.save_file_fmt % self._ems_id, "wb"))


	def __load_tree(self):

		import cPickle as p
		self._tree = p.load(open(Flight.save_file_fmt % self._ems_id, "rb"))


	def __treefile_exists(self):

		return os.path.exists(Flight.save_file_fmt % self._ems_id)







def name_only(fields):

	return [f[0] for f in fields]


def get_best_match(fields):

	names = name_only(fields)
	l = [len(n) for n in names]
	return fields[l.index(min(l))]















