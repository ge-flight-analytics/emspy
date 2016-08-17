
uri_root = 'https://fas.efoqa.com/api'
uris = {
	'sys': {
		'auth': '/token'
	},
	'ems_sys': {
		'list': '/v2/ems-systems',
		'ping': '/v2/ems-systems/%s/ping', 	# (ems-system_id)
		'info': '/v2/ems-systems/%s/info'	# (ems-system_id)	
	},
	'fleet': {
		'list'	: '/v2/ems-systems/%s/assets/fleets', 	#(ems-system_id)
		'info'	: '/v2/ems-systems/%s/assets/fleets/%s'	#(ems-system_id, fleet_id)	
	},
	'aircraft': {
		'list'	: '/v2/ems-systems/%s/assets/aircraft',	#(ems-system_id)
		'info'	: '/v2/ems-systems/%s/assets/aircraft/%s'#(ems-system_id, aircraft_id)
	},
	'flt_phase': {
		'list'	: '/v2/ems-systems/%s/assets/flight-phases', 	#(ems-system_id)
		'info'	: '/v2/ems-systems/%s/assets/flight-phases/%s' 	#(ems-system_id, flt_phase_id)
	},
	'airport': {
		'list'	: '/v2/ems-systems/%s/assets/airports',		#(ems-system_id)
		'info'	: '/v2/ems-systems/%s/assets/airports/%s'	#(ems-system_id, airport_id)
	},
	'database': {
		'group'			: '/v2/ems-systems/%s/database-groups',			#(ems-system_id)
		'field_group'	: '/v2/ems-systems/%s/databases/%s/field-groups',	#(ems-system_id, data_src_id)
		'field'			: '/v2/ems-systems/%s/databases/%s/fields',
		'query'			: '/v2/ems-systems/%s/databases/%s/query'
	},
	'analytic': {
		'search'	: '/v2/ems-systems/%s/analytics',    # (emsSystemId)
		'search_f'	: '/v2/ems-systems/%s/flights/%s/analytics', # (emsSystemId, flightId)
		'group'		: '/v2/ems-systems/%s/analytic-groups',    # (emsSystemId)
		'group_f'	: '/v2/ems-systems/%s/flights/%s/analytic-groups', # (emsSystemId, flightId)
		'query'		: '/v2/ems-systems/%s/flights/%s/analytics/query', # (emsSystemId, flightId)
		'metadata'  : '/v2/ems-systems/%s/flights/%s/analytics/metadata' # (emsSystemId, flightId)
	}
	# 'param': {
	# 	'info'		: '/v1/ems/%s/parameter/%s',		# (ems-system_id, prm_id)
	# 	'group'		: '/v1/ems/%s/parameter-group',		# (ems-system_id)
	# 	'search'	: '/v1/ems/%s/parameters',			# (ems-system_id)
	# 	'byrange'	: '/v1/ems/%s/flight/%s/parameter/%s/multiple-by-range', 	#(ems-system_id, flt_id, prm_id)
	# 	'byoffset'	: '/v1/ems/%s/flight/%s/parameter/%s/multiple-by-offset',   #(ems-system_id, flt_id, prm_id)
	# 	'byrange2'	: '/v1/ems/%s/flight/%s/parameter/%s/multiple-sampled-values',
	# 	'byoffset2' : '/v1/ems/%s/flight/%s/parameter/%s/multiple-sampled-offsets'
	# }
}
