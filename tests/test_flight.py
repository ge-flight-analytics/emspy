from emspy.query import flight

import pandas as pd

def test_get_shortest_field():
    airframe_fields_dict = {
        'ems_id': {29: 8, 30: 8},
        'db_id': {29: '[ems-core][entity-type][foqa-flights]', 30: '[ems-core][entity-type][foqa-flights]'},
        'id': {29: '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[airframe-engine-field-set][base-field][airframe]]]', 30: '[-hub-][field][[[ems-core][entity-type][foqa-flights]][[airframe-engine-field-set][base-field][airframe-engine-series]]]'},
        'nodetype': {29: 'field', 30: 'field'},
        'type': {29: 'discrete', 30: 'discrete'},
        'name': {29: 'Airframe', 30: 'Airframe Engine Series'},
        'parent_id': {29: '[-hub-][field-group][[[ems-core][entity-type][foqa-flights]][[ems-core][internal-field-group][aircraft-info]]]', 30: '[-hub-][field-group][[[ems-core][entity-type][foqa-flights]][[ems-core][internal-field-group][aircraft-info]]]'}}
    airframe_fields_dataframe = pd.DataFrame.from_dict(airframe_fields_dict)
    airframe_field = flight._get_shortest(airframe_fields_dataframe)
    assert(airframe_field['name'] == "Airframe")
