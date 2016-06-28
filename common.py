
uri_root = 'https://fas.efoqa.com/api'
uris = {
	'sys': {
		'auth': '/token'
	},
	'ems': {
		'list': '/v1/ems',
		'ping': '/v1/ems/%s/ping', 	# (ems_id)
		'info': '/v1/ems/%s/info'	# (ems_id)	
	},
	'fleet': {
		'list'	: '/v1/ems/%s/asset/fleet', 	#(ems_id)
		'info'	: '/v1/ems/%s/asset/fleet/%s'	#(ems_id, fleet_id)	
	},
	'aircraft': {
		'list'	: '/v1/ems/%s/asset/aircraft',	#(ems_id)
		'info'	: '/v1/ems/%s/asset/aircraft/%s'#(ems_id, aircraft_id)
	},
	'flt_phase': {
		'list'	: '/v1/ems/%s/asset/flight-phase', 		#(ems_id)
		'info'	: '/v1/ems/%s/asset/flight-phase/%s' 	#(ems_id, flt_phase_id)
	},
	'airport': {
		'list'	: '/v1/ems/%s/asset/airport',		#(ems_id)
		'info'	: '/v1/ems/%s/asset/airport/%s'		#(ems_id, airport_id)
	},
	'data_src': {
		'group'			: '/v1/ems/%s/data-source-group',			#(ems_id)
		'field_group'	: '/v1/ems/%s/data-source/%s/field-group',	#(ems_id, data_src_id)
		'field'			: '/v1/ems/%s/data-source/%s/field',
		'query'			: '/v1/ems/%s/data-source/%s/query'
	},
	'param': {
		'info'		: '/v1/ems/%s/parameter/%s',		# (ems_id, prm_id)
		'group'		: '/v1/ems/%s/parameter-group',		# (ems_id)
		'search'	: '/v1/ems/%s/parameters',			# (ems_id)
		'byrange'	: '/v1/ems/%s/flight/%s/parameter/%s/multiple-by-range', 	#(ems_id, flt_id, prm_id)
		'byoffset'	: '/v1/ems/%s/flight/%s/parameter/%s/multiple-by-offset',   #(ems_id, flt_id, prm_id)
		'byrange2'	: '/v1/ems/%s/flight/%s/parameter/%s/multiple-sampled-values',
		'byoffset2' : '/v1/ems/%s/flight/%s/parameter/%s/multiple-sampled-offsets'
	}
}
