import sys
import pandas as pd
sys.path.insert(0, '..')
from emspy.query import InsertQuery
from mock_connection import MockConnection
from mock_ems import MockEMS

if sys.version_info[0] == 2:
    from mock import patch
else:
    from unittest.mock import patch

@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_all_df_rows_exist_in_create_columns_without_schema_map():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    entity_id = 'foo'
    i_query = InsertQuery(connection, ems_system, entity_id)

    dummy_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c'], 'C': [4, 5, 6]})
    i_query.insert_df(dummy_df)

    create_columns = i_query.get_create_columns()
    create_columns = create_columns['createColumns']
    # Rows should have been added in order.
    i = 0
    for idx, row in dummy_df.iterrows():
        row_dict = row.to_dict()
        j = 0
        for item in row_dict.items():
            col = item[0]
            val = item[1]
            create_columns_entry = create_columns[i][j]
            assert (create_columns_entry['fieldId'] == col)
            assert (create_columns_entry['value'] == val)
            j = j + 1
        i = i + 1


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_all_create_columns_exist_in_df_without_schema_map():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    entity_id = 'foo'
    i_query = InsertQuery(connection, ems_system, entity_id)

    dummy_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c'], 'C': [4, 5, 6]})
    i_query.insert_df(dummy_df)

    create_columns = i_query.get_create_columns()
    create_columns = create_columns['createColumns']
    # Rows should have been added in order.
    i = 0
    for row in create_columns:
        row_df = dummy_df.iloc[i, :]  # create_column row # should correspond to dataframe row #
        j = 0
        for item in row:
            fieldId = item['fieldId']
            value = item['value']
            assert (value == row_df[fieldId])
            j = j + 1
        i = i + 1


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_all_df_rows_exist_in_create_columns_with_schema_map():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    entity_id = 'foo'
    i_query = InsertQuery(connection, ems_system, entity_id)

    dummy_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c'], 'C': [4, 5, 6]})
    schema_map = {'A': '[-hub][A]', 'B': '[-hub][B]', 'C': '[-hub][C]'}
    i_query.insert_df(dummy_df, schema_map=schema_map)

    dummy_df = dummy_df.rename(columns=schema_map)
    create_columns = i_query.get_create_columns()
    create_columns = create_columns['createColumns']
    # Rows should have been added in order.
    i = 0
    for idx, row in dummy_df.iterrows():
        row_dict = row.to_dict()
        j = 0
        for item in row_dict.items():
            col = item[0]
            val = item[1]
            create_columns_entry = create_columns[i][j]
            assert (create_columns_entry['fieldId'] == col)
            assert (create_columns_entry['value'] == val)
            j = j + 1
        i = i + 1


@patch('emspy.query.query.EMS', MockEMS)
@patch('emspy.Connection', MockConnection)
def test_all_create_columns_exist_in_df_with_schema_map():
    ems_system = 'ems24-app'
    connection = MockConnection(user='', pwd='')
    entity_id = 'foo'
    i_query = InsertQuery(connection, ems_system, entity_id)

    dummy_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c'], 'C': [4, 5, 6]})
    schema_map = {'A': '[-hub][A]', 'B': '[-hub][B]', 'C': '[-hub][C]'}
    i_query.insert_df(dummy_df, schema_map=schema_map)

    dummy_df = dummy_df.rename(columns=schema_map)
    create_columns = i_query.get_create_columns()
    create_columns = create_columns['createColumns']
    # Rows should have been added in order.
    i = 0
    for row in create_columns:
        row_df = dummy_df.iloc[i, :]  # create_column row # should correspond to dataframe row #
        j = 0
        for item in row:
            fieldId = item['fieldId']
            value = item['value']
            assert (value == row_df[fieldId])
            j = j + 1
        i = i + 1
