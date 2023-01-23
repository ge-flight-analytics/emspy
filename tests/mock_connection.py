from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import sys
if sys.version_info < (3, 0):
    from future import standard_library
    standard_library.install_aliases()

from urllib.error import HTTPError
import pprint as pp
from emspy.connection import Connection


RESPONSE_HEADERS = [
    ('Content-Type', 'application/json; charset=utf-8'),
    ('Content-Encoding', 'gzip'),
]


class MockConnection(Connection):
    """
    Object for connection to EMS API
    """

    def __init__(self, user, pwd):
        super(MockConnection, self).__init__(user=user, pwd=pwd)

    def connect(self, user, pwd, proxies, verbose):
        return None

    def request(self, rtype="GET", uri=None, uri_keys=None, uri_args=None, headers=None, body=None,
                data=None, jsondata=None, proxies=None, verbose=False):
        resp_h, content = [], []
        if uri_keys == ('profile', 'search'):
            if body['search'] == 'A PROFILE THAT SHOULD NEVER EXIST':
                content = []
            else:
                content = [
                    {'localId': 88,
                     'id': '5b8dc8cb-c8cb-c8cb-c8cb-c8cb39e6c8cb',
                     'name': 'Duplicate Profile',
                     'treeLocation': [
                         {'id': 'ffffffff-0000-0000-0000-000000000000',
                          'name': 'APM Profiles'},
                         {'id': '1918e33f-a7f5-469a-8c2d-310b506ab797',
                          'name': 'Standard Library Profiles'},
                         {'id': 'e3ae6cb3-8200-487d-9ebd-8fd6cd68b6b2',
                          'name': 'Efficiency'},
                         {'id': 'a1d76ee9-ec62-4cd0-bb53-9cc92228f13b',
                          'name': 'Block-Cost Model'}
                     ],
                     'library': True,
                     'currentVersion': 9},
                    {'localId': 108,
                     'id': '12e212e2-12e2-12e2-12e2-ad12e2a14acc',
                     'name': 'Single Real Profile 2',
                     'treeLocation': [
                         {'id': 'ffffffff-0000-0000-0000-000000000000',
                          'name': 'APM Profiles'},
                         {'id': '1918e33f-a7f5-469a-8c2d-310b506ab797',
                          'name': 'Standard Library Profiles'},
                         {'id': 'e3ae6cb3-8200-487d-9ebd-8fd6cd68b6b2',
                          'name': 'Efficiency'},
                         {'id': 'a1d76ee9-ec62-4cd0-bb53-9cc92228f13b',
                          'name': 'Block-Cost Model'}
                     ],
                     'library': True,
                     'currentVersion': 9},
                    {'localId': 56,
                     'id': 'f163eeee-63ee-63ee-63ee-1b363eed63ee',
                     'name': 'Single Profile 3',
                     'treeLocation': [
                         {'id': 'ffffffff-0000-0000-0000-000000000000',
                          'name': 'APM Profiles'},
                         {'id': '1918e33f-a7f5-469a-8c2d-310b506ab797',
                          'name': 'Standard Library Profiles'},
                         {'id': 'e3ae6cb3-8200-487d-9ebd-8fd6cd68b6b2',
                          'name': 'Efficiency'},
                         {'id': 'a1d76ee9-ec62-4cd0-bb53-9cc92228f13b',
                          'name': 'Block-Cost Model'}],
                     'library': True,
                     'currentVersion': 8},
                    {'localId': 99,
                     'id': 'c152c3dd-496a-496a-496a-bd5fc3ddbd5f',
                     'name': 'Single Real Profile',
                     'treeLocation': [
                         {'id': 'ffffffff-0000-0000-0000-000000000000',
                          'name': 'APM Profiles'},
                         {'id': '1918e33f-a7f5-469a-8c2d-310b506ab797',
                          'name': 'Standard Library Profiles'},
                         {'id': 'e3ae6cb3-8200-487d-9ebd-8fd6cd68b6b2',
                          'name': 'Efficiency'},
                         {'id': 'a1d76ee9-ec62-4cd0-bb53-9cc92228f13b',
                          'name': 'Block-Cost Model'}],
                     'library': True,
                     'currentVersion': 8},
                    {'localId': 500000,
                     'id': '5b8d6db7-c8cb-474f-b951-4c8e39e6eba1',
                     'name': 'Duplicate Profile',
                     'treeLocation': [
                         {'id': 'ffffffff-0000-0000-0000-000000000000',
                          'name': 'APM Profiles'},
                         {'id': '1918e33f-a7f5-469a-8c2d-310b506ab797',
                          'name': 'Standard Library Profiles'},
                         {'id': 'e3ae6cb3-8200-487d-9ebd-8fd6cd68b6b2',
                          'name': 'Efficiency'},
                         {'id': 'a1d76ee9-ec62-4cd0-bb53-9cc92228f13b',
                          'name': 'Block-Cost Model'}],
                     'library': True,
                     'currentVersion': 10}
                ]
        elif uri_keys == ('analytic', 'search') and jsondata is not None:
            print(jsondata['id'])
            if jsondata['id'] == 'fake-bar-alt-id-that-exists=':
                content = {
                    'id': 'fake-bar-alt-id-that-exists=',
                    'name': 'Baro-Corrected Altitude (ft)',
                    'description': 'Altitude above mean sea level (MSL)',
                    'units': 'ft'
                }
            elif jsondata['id'] == 'fake-pressure-alt-id-that-exists=':
                content = {
                    'id': 'fake-pressure-alt-id-that-exists=',
                    'name': 'Pressure Altitude (ft)',
                    'description': 'Altitude derived from static air pressure.',
                    'units': 'ft'
                }
            elif jsondata['id'] == 'fake-pressure-alt-id-that-DOES-NOT-exist=':
                raise HTTPError('url', 'code', 'msg', 'hdrs', None)
        elif uri_keys == ('database', 'field_group'):
            if body is None:
                content = {
                    'id': '[-hub-][field-group]'
                          '[[[ems-core][entity-type][foqa-flights]]'
                          '[[--][internal-field-group][root]]]',
                    'name': '<root>',
                    'groups': [
                        {'id': '[-hub-][field-group]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][internal-field-group][flight-info]]]',
                         'name': 'Flight Information'},
                        {'id': '[-hub-][field-group]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][internal-field-group][aircraft-info]]]',
                         'name': 'Aircraft Information'},
                    ], 'fields': []
                }
            elif body['groupId'] == '[-hub-][field-group]' \
                                    '[[[ems-core][entity-type][foqa-flights]]' \
                                    '[[ems-core][internal-field-group][aircraft-info]]]':
                content = {
                    'id': '[-hub-][field-group]'
                          '[[[ems-core][entity-type][foqa-flights]]'
                          '[[ems-core][internal-field-group][aircraft-info]]]',
                    'name': 'Aircraft Information',
                    'groups': [
                        {'id': '[-hub-][field-group]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[airframe-information][internal-field-group]'
                               '[airframe-information]]]',
                         'name': 'Airframe Information'},
                        {'id': '[-hub-][field-group]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[esn-information][internal-field-group][engine-information]]]',
                         'name': 'Engine Information'},
                    ],
                    'fields': [
                        {'id': '[-hub-][field]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[airframe-engine-field-set][base-field][engine-series-2]]]',
                         'type': 'discrete',
                         'name': 'Engine Series'
                         }
                    ]
                }
            elif body['groupId'] == '[-hub-][field-group]' \
                                    '[[[ems-core][entity-type][foqa-flights]]' \
                                    '[[ems-core][internal-field-group][flight-info]]]':
                content = {
                    'id': '[-hub-][field-group]'
                          '[[[ems-core][entity-type][foqa-flights]]'
                          '[[ems-core][internal-field-group][flight-info]]]',
                    'name': 'Flight Information',
                    'groups': [
                        {'id': '[-hub-][field-group]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][internal-field-group][foqa-flights-processing]]]',
                         'name': 'Processing'
                         },
                        {'id': '[-hub-][field-group]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[fdw-date-times][internal-field-group][date-times]]]',
                         'name': 'Date Times'
                         },
                    ],
                    'fields': [
                        {'id': '[-hub-][field]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][base-field][flight.uid]]]',
                         'type': 'number',
                         'name': 'Flight Record'
                         },
                        {'id': '[-hub-][field]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][base-field][flight.exact-date]]]',
                         'type': 'dateTime',
                         'name': 'Flight Date (Exact)'},
                        {'id': '[-hub-][field]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][base-field][flight.date-confidence]]]',
                         'type': 'discrete',
                         'name': 'Flight Date Confidence'
                         },
                        {'id': '[-hub-][field]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][base-field][flight.flight-num-str]]]',
                         'type': 'string',
                         'name': 'Flight Number String'
                         },
                        {'id': '[-hub-][field]'
                               '[[[ems-core][entity-type][foqa-flights]]'
                               '[[ems-core][base-field][flight.exist-takeoff]]]',
                         'type': 'boolean',
                         'name': 'Takeoff Valid'
                         }
                    ]
                }
            elif body['groupId'] == '[-hub-][field-group]' \
                                    '[[[ems-core][entity-type][foqa-flights]]' \
                                    '[[source-flight-info][internal-field-group]' \
                                    '[source-system-information]]]':
                content = {
                    'id': '[-hub-][field-group]'
                          '[[[ems-core][entity-type][foqa-flights]]'
                          '[[source-flight-info][internal-field-group]'
                          '[source-system-information]]]',
                    'name': 'Source System Information',
                    'groups': [],
                    'fields': []
                }
        elif uri_keys == ('database', 'field'):
            if uri_args[2] == '[-hub-][field]' \
                              '[[[ems-core][entity-type][foqa-flights]]' \
                              '[[ems-core][base-field][flight.date-confidence]]]':
                content = {
                    'type': 'discrete',
                    'discreteValues': {
                        '1': 'Low',
                        '0': 'Unknown',
                        '2': 'High'
                    },
                    'name': 'Flight Date Confidence',
                    'id': '[-hub-][field]'
                          '[[[ems-core][entity-type][foqa-flights]]'
                          '[[ems-core][base-field][flight.date-confidence]]]'
                }
        elif uri_keys == ('aircraft', 'list'):
            content = [
                {
                    'id': 0,
                    'description': 'UNKNOWN',
                    'fleetIds': [0],
                    'isActive': True,
                    'isApproved': True
                }, {
                    'id': 1,
                    'description': '000001',
                    'fleetIds': [1, 2],
                    'isActive': True,
                    'isApproved': True
                }, {
                    'id': 2,
                    'description': '000002',
                    'fleetIds': [2, 3],
                    'isActive': True,
                    'isApproved': True
                }, {
                    'id': 3,
                    'description': '000003',
                    'fleetIds': [1, 3],
                    'isActive': False,
                    'isApproved': True
                }, {
                    'id': 4,
                    'description': '000004',
                    'fleetIds': [4],
                    'isActive': True,
                    'isApproved': False
                }, {
                    'id': 5,
                    'description': '000005',
                    'fleetIds': [2, 4],
                    'isActive': False,
                    'isApproved': False
                }
            ]
        elif uri_keys == ('airport', 'list'):
            content = [
                {
                    'id': 0,
                    'codeIata': '',
                    'codeIcao': 'UNKN',
                    'codeFaa': 'UNK',
                    'codePreferred': 'UNKNOWN',
                    'name': 'UNKNOWN',
                    'city': 'UNKNOWN',
                    'country': '',
                    'latitude': 0.0,
                    'longitude': 0.0,
                    'elevation': 0.0
                }, {
                    'id': 1,
                    'codeIata': 'XXX',
                    'codeIcao': 'XXXX',
                    'codeFaa': 'XXX',
                    'codePreferred': 'XXXX',
                    'name': 'Mock Airport 1',
                    'city': 'Mock City 1',
                    'country': 'Mock Country',
                    'latitude': 0.0,
                    'longitude': 0.0,
                    'elevation': 0.0
                }, {
                    'id': 2,
                    'codeIata': 'YYY',
                    'codeIcao': 'YYYY',
                    'codeFaa': 'YYY',
                    'codePreferred': 'YYYY',
                    'name': 'Mock Airport 2',
                    'city': 'Mock City 1',
                    'country': 'Mock Country',
                    'latitude': 0.0,
                    'longitude': 0.0,
                    'elevation': 0.0
                }, {
                    'id': 3,
                    'codeIata': 'ZZZ',
                    'codeIcao': 'ZZZZ',
                    'codeFaa': 'ZZZ',
                    'codePreferred': 'ZZZZ',
                    'name': 'Mock Airport 3',
                    'city': 'Mock City 2',
                    'country': 'Mock Country',
                    'latitude': 0.0,
                    'longitude': 0.0,
                    'elevation': 0.0
                }
            ]
        elif uri_keys == ('fleet', 'list'):
            content = [
                {
                    'id': 0,
                    'description': 'UNKNOWN'
                }, {
                    'id': 1,
                    'description': 'Mock Fleet'
                }
            ]
        elif uri_keys == ('flt_phase', 'list'):
            content = [
                {
                    'id': 0,
                    'description': 'unknown state'
                }, {
                    'id': 1,
                    'description': 'B) Taxi Out'
                }, {
                    'id': 2,
                    'description': 'C) Takeoff'
                }, {
                    'id': 3,
                    'description': 'D) Rejected Takeoff'
                }, {
                    'id': 4,
                    'description': 'E) Initial Climb'
                }, {
                    'id': 5,
                    'description': 'F) Climb'
                }, {
                    'id': 6,
                    'description': 'G) Enroute'
                }, {
                    'id': 7,
                    'description': 'H) Descent'
                }, {
                    'id': 8,
                    'description': 'I) Approach'
                }, {
                    'id': 11,
                    'description': 'J) Go Around'
                }, {
                    'id': 12,
                    'description': 'K) Roll Out'
                }, {
                    'id': 14,
                    'description': 'M) Taxi In'
                }, {
                    'id': 15,
                    'description': 'A) Start & Push'
                }, {
                    'id': 16,
                    'description': 'N) Parking'
                }
            ]
        elif uri_keys == ('analyticSet', 'analytic_set'):
            ems_id, group_id, analytic_set_id = uri_args
            if analytic_set_id == 'A PARAMETER SET THAT DOES NOT EXIST':
                content = {
                    "message": "Invalid Analytic Set",
                    "messageDetail": "Unable to find an analytic set with the name {analytic_set_id} in group {group_id}".format(analytic_set_id=analytic_set_id, group_id=group_id),
                    "unexpected": False
                }
            else:
                content = {
                    "name": analytic_set_id,
                    "description": "mock parameter set description",
                    "items": [
                        {
                            "chartIndex": 0,
                            "analytic": {
                                "id": "fake-pres-alt-id-that-exists=",
                                "name": "Pressure Altitude (ft)",
                                "description": "Altitude derived from static air pressure.  A value of zero should always correspond to static air pressure = 29.92 inches of mercury.",
                                "units": "ft",
                                "metadata": None
                            },
                            "color": "#000000"
                        },
                        {
                            "chartIndex": 1,
                            "analytic": {
                                "id": "fake-rad-alt-id-that-exists=",
                                "name": "Radio Altitude (1, left, Capt or Only) (ft)",
                                "description": "This is the recorded radio altitude value (in feet) of the 'primary' radio altimeter.  Preferably the 'first' or 'left' but also the 'captain' or only available recorded altimeter.",
                                "units": "ft",
                                "metadata": None
                            },
                            "color": "#000000"
                        },
                        {
                            "chartIndex": 1,
                            "analytic": {
                                "id": "fake-gear_height-id-that-exists=",
                                "name": "Best Estimate of Main Gear Height AGL (ft)",
                                "description": "This is the best estimate of the height above ground level of the main gear.  If available, it will use the pitch, altimeter height and geometry constants to compute this height.  If these are not available, it will use the recorded/biased radar altimeter value as an estimate.",
                                "units": "ft",
                                "metadata": None
                            },
                            "color": "#0000FF"
                        }
                    ]
                }
        elif uri_keys == ('analyticSet', 'analytic_set_group'):
            ems_id, group_id = uri_args
            if group_id == 'root':
                content = {
                    "name": "Misc",
                    "groupId": "Root",
                    "groups": [
                        {
                            "name": "Mock Folder 1",
                            "groupId": "Mock Folder 1",
                            "groups": [],
                            "sets": [],
                            "collections": []
                        },
                        {
                            "name": "Mock Folder 2",
                            "groupId": "Mock Folder 2",
                            "groups": [],
                            "sets": [],
                            "collections": []
                        }
                    ],
                    "sets": [
                        {
                            "name": "Mock root parameter set 1"
                        },
                        {
                            "name": "Mock root parameter set 2"
                        }
                    ],
                    "collections": []
                }
            else:
                content = {
                    "name": group_id,
                    "groupId": group_id,
                    "groups": [
                        {
                            "name": "Mock Folder 1",
                            "groupId": "{}:Mock Folder 1".format(group_id),
                            "groups": [],
                            "sets": [],
                            "collections": []
                        },
                        {
                            "name": "Mock Folder 2",
                            "groupId": "{}:Mock Folder 2".format(group_id),
                            "groups": [],
                            "sets": [],
                            "collections": []
                        }
                    ],
                    "sets": [
                        {
                            "name": "Mock parameter set 1"
                        },
                        {
                            "name": "Mock parameter set 2"
                        }
                    ],
                    "collections": []
                }


        return RESPONSE_HEADERS, content


def print_resp(resp):
    for r in resp:
        pp.pprint(r)
