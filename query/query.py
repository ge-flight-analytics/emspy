from emspy.query import * 
import pandas as pd
import sys, json


class Query:


	def __init__(self, conn, ems_name):

		self._conn 		= conn
		self._ems_name  = ems_name
		self.__ems 		= None
		self.__fleet 	= None
		self.__aircraft = None
		self.__airport 	= None
		self.__flightphase = None
		self.__init_ems()
		self._ems_id 	= self.__ems.get_id(ems_name)
		self._init_assets()
		self.reset()

	
	def __init_ems(self):

		self.__ems = EMS(self._conn)

	
	def _init_assets(self):

		# self.__fleet = Fleet(self._conn, self._ems_id)
		# self.__aircraft = Aircraft(self._conn, self._ems_id)
		# self.__airport = Airport(self._conn, self._ems_id)
		# self.__flightphase = FlightPhase(self._conn, self._ems_id)
		self.__flight = Flight(self._conn, self._ems_id)


	def reset(self):

		self.__columns = []

		self.__queryset = {
			"select": [],
			"groupBy": [],
			"orderBy": [],
			"distinct": True,
			"top": 10,
			"format": "none"
		}



	def select(self, *args, **kwargs):
		'''
		Functionally equivalent to SQL's select statement

		Example
		-------
		Following is the example of select method to query three fields and one more with
		aggregation function applied. The values are appended until the whole query is 
		reset.

		>> query.select("customer id", "takeoff valid", "takeoff airport iata code")
		>> query.select("p301: fuel burned by all engines during cruise", aggregate="avg")
		'''
		aggs = ['none', 'avg', 'count', 'max', 'min', 'stdev', 'sum', 'var']
		aggregate = kwargs.get('aggregate', 'none')
		if aggregate not in aggs:
			sys.exit("Wrong aggregation selected. Use one of %s." % aggs)
		fields = self.__flight.search_fields(*args)
		if type(fields)!=list: fields = [fields]

		for field in fields:
			d = {}
			d['fieldId'] = field[1]['id']
			d['aggregate'] = aggregate
			self.__queryset['select'].append(d)
			self.__columns.append(field)


	def group_by(self, *args):
		'''Functionally equivalent to SQL's groupby'''
		for field in self.__flight.search_fields(*args):
			self.__queryset['groupBy'].append({'fieldId': field[1]['id']})


	def order_by(self, field, order='asc'):
		'''Functional equivalent of SQL's order by''',
		if order not in ['asc', 'desc']:
			sys.exit("Ordering option must be one of %s." % ['asc', 'desc'])
		self.__queryset['orderBy'].append({
			'fieldId': self.__flight.search_fields(field)[1]['id'],
			'order': order,
			'aggregate': 'none'})


	def filter(self, expr):
		'''
		Translate the give filtering conditions to json queries.
		'''
		if not self.__queryset.has_key('filter'):
			self.__queryset['filter'] = {
				'operator': 'and',
				'args': []
			}
		expr_vec = self.__split_expr(expr)
		jsonobj  = self.__translate_expr(expr_vec)
		self.__queryset['filter']['args'].append(jsonobj)



	def __split_expr(self, expr):

		import re

		for pattern in ['[=!<>]=?'] + sp_ops.keys():
			a = re.search(pattern, expr)
			if a is not None:
				break

		if a is None:
			sys.exit("Cannot find any valid conditional operator from the given string expression.")
		splitted = expr.partition(a.group())
		return splitted


	def __translate_expr(self, expr_vec):

		fld_loc = [False, False]
		fld_info = None
		fld_type = None
		val_info = None
		op = expr_vec[1]

		for i, s in enumerate([expr_vec[0], expr_vec[2]]):
			x = eval(s)

			if i == 0:
				fld = self.__flight.search_fields(x)
				if fld is not None:
					fld_info = [{'type':'field', 'value': fld[1]['id']}]
					fld_type = fld[1]['type']
					fld_loc[i] = True
				else:
					raise ValueError("No field was found with the keyword %s. Please double-check if it is a right keyword." % x)
			else:
				if type(x) != list:
					x = [x]				
				val_info = [{'type':'constant','value':v} for v in x]

		if fld_loc[1]:
			if '<' in expr_vec[1]: 
				op = expr_vec[1].replace('<','>')
			else:
				op = expr_vec[1].replace('>','<')			

		arg_list = fld_info + val_info

		if fld_type=="boolean":
			fltr = _boolean_filter(op, arg_list)
		elif fld_type=="discrete":
			fltr = _discrete_filter(op, arg_list, self.__flight)
		elif fld_type=="number":
			fltr = _number_filter(op, arg_list)
		elif fld_type=="string":
			fltr = _string_filter(op, arg_list)
		elif fld_type=="dateTime":
			fltr = _datetime_filter(op, arg_list)
		else:
			raise ValueError("%s has an unknown field data type %s." % (fld[0], fld_type))
		return fltr


	def distinct(self, x=True):

		self.__queryset['distinct'] = x


	def get_top(self, n):

		if n > 5000: n = 5000
		self.__queryset['top'] = n


	# def readable_output(self, x=False):

	# 	if x: 
	# 		y = "display"
	# 	else:
	# 		y = "none"
	# 	self.__queryset['format'] = y


	def in_json(self):

		return json.dumps(self.__queryset)


	def in_dict(self):

		return self.__queryset


	def run(self, output = "dataframe"):
		'''
		output type = ["raw", "dataframe"]
		More types to add.
		'''

		resp_h, content = self._conn.request(	
			rtype="POST", 
			uri_keys=('data_src','query'),
			uri_args=(self._ems_id, self.__flight.get_datasource()['id']),
			jsondata= self.__queryset
			)	

		if output == "raw":
			return content
		elif output == "dataframe":
			return self.__to_dataframe(content)
		else:
			raise ValueError("Requested an unknown output type.")


	def __to_dataframe(self, json_output):

		print("Raw JSON output to Pandas dataframe...")
		col      = [h['name'] for h in json_output['header']]
		coltypes = [c[1]['type'] for c in self.__columns]
		col_id   = [c[1]['id'] for c in self.__columns]
		val      = json_output['rows']

		df = pd.DataFrame(data = val, columns = col)

		is_readable_on = self.__queryset['format']

		for cid, cname, ctype in zip(col_id, col, coltypes):
			try:
				if ctype=='number':				
					df[cname] = pd.to_numeric(df[cname])
				elif ctype=='discrete':
					k_map = self.__flight.list_allvalues(field_id = cid, in_dict = True)
					df[cname] = df[cname].astype(str)
					df = df.replace({cname: k_map})
				elif ctype=='boolean':
					df[cname] = df[cname].astype(bool)
				elif ctype=='dateTime':
					df[cname] = pd.to_datetime(df[cname])
			except ValueError:
				pass
		return df


	def update_datatree(self, *args):
		self.__flight.update_tree(*args)


	def save_datatree(self, file_name = None):
		self.__flight.save_tree(file_name)


	def load_datatree(self, file_name = None):
		self.__flight.load_tree(file_name)







## Experimental...

basic_ops = {
	'==': 'equal', '!=': 'notEqual', '<': 'lessThan', 
	'<=': 'lessThanOrEqual', '>': 'greaterThan',
	'>=': 'greaterThanOrEqual'
}
sp_ops = {
	' in ': 'in',
	' not in ': 'notIn'
}
# '=Null': 'isNull', '!=Null': 'isNotNull', 'and': 'And', 'or': 'Or', 'in': 'in', 'not in': 'notIn'


def _filter_fmt1(op, *args):
	fltr = {
		"type": "filter",
		"value": {
			"operator": op,
			"args": []
		}
	}
	for x in args:
		fltr['value']['args'].append({'type': x['type'], 'value': x['value']})

	return fltr 


def _boolean_filter(op, d):
	
	fld_info = d[0]
	val_info = d[1]
	if type(val_info['value'])!=bool: raise ValueError("%s: use a boolean value." % val_info['value'])
	if op == "==": 
		t_op = 'is'+str(val_info['value'])
	elif op == "!=":
		t_op = 'is'+str(not val_info['value'])
	else:
		raise ValueError("Conditional operator %s is given. Booleans shoule be only with boolean operators." % op)

	fltr = _filter_fmt1(t_op, fld_info)
	return fltr


def _discrete_filter(op, d, flt):
	fld_info = d[0]	

	if op in basic_ops.keys():
		# Single input basic coniditonal operation
		t_op = basic_ops[op]
		val_info = d[1]
		vid  = flt.get_value_id(val_info['value'], field_id = fld_info['value'])
		val_info['value'] = vid
		fltr = _filter_fmt1(t_op, fld_info, val_info)

	elif op in [" in ", " not in "]:
		t_op = sp_ops[op]
		val_list = [{'type':x['type'], 'value':flt.get_value_id(x['value'], field_id=fld_info['value'])}\
					 for x in d[1:]]
		inp = [fld_info] + val_list			 
		fltr = _filter_fmt1(t_op, *inp)
	else:
		raise ValueError("%s: Unsupported conditional operator for discrete field type." % op)	
	return fltr


def _number_filter(op, d):

	if basic_ops.has_key(op):
		t_op = basic_ops[op]
		fltr = _filter_fmt1(t_op, d[0], d[1])
	else: 
		raise ValueError("%s: Unsupported conditional operator for number field type." % op)
	return fltr


def _string_filter(op, d):

	if op in ["==", "!="]:
		t_op = basic_ops[op]
		fltr = _filter_fmt1(t_op, d[0], d[1])

	elif op in [" in ", " not in "]:
		t_op = sp_ops[op]
		fltr = _filter_fmt1(t_op, *d)

	else:
		raise ValueError("%s: Unsupported conditional operator for string field type." % op)		
	return fltr


def _datetime_filter(op, d):

	from datetime import datetime

	date_ops = {
		"<": "dateTimeBefore",
		">=": "dateTimeOnAfter"
	}

	if op in date_ops.keys():
		t_op = date_ops[op]
		fltr = _filter_fmt1(t_op, d[0], d[1])
		# Additional json attribute to specify this is UTC time
		fltr['value']['args'].append({'type':'constant', 'value': 'Utc'})
	else:
		raise ValueError("%s: Unsupported conditional operator for datetime field type." % op)		
	return fltr		






