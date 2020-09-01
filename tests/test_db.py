import os
import sqlite3
from mock_query import MockFilterQuery
import pytest


def remove_tmpDB():
    if os.path.exists('tmpDB.db'):
        os.remove('tmpDB.db')


@pytest.fixture(autouse=True)
def cleanup():
    remove_tmpDB()
    yield
    remove_tmpDB()


def create_tmpDB(legacy=False):
    if legacy:
        sql = """
            BEGIN TRANSACTION;
            CREATE TABLE dbtree (
                ems_id INTEGER,
                id TEXT,
                name TEXT,
                nodetype TEXT,
                parent_id TEXT
            );
            CREATE TABLE kvmaps(
                ems_id INTEGER,
                id TEXT,
                key TEXT,
                value TEXT
            );
            INSERT INTO dbtree(ems_id, id, name, nodetype, parent_id) 
            VALUES(
                1,
                '[-hub-][entity-type-group][[--][internal-type-group][root]]',
                '<root>',
                'root',
                NULL
            );
            INSERT INTO dbtree(ems_id, id, name, nodetype, parent_id) 
            VALUES(
                1,
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]',
                'FDW',
                'database_group',
                '[-hub-][entity-type-group][[--][internal-type-group][root]]'
            );
            INSERT INTO dbtree(ems_id, id, name, nodetype, parent_id) 
            VALUES(
                1,
                '[ems-core][entity-type][foqa-flights]',
                'FDW Flights',
                'database',
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]'
            );
            INSERT INTO kvmaps(ems_id, id, key, value) 
            VALUES(
                1,
                '[-hub-][field][[[ems-core][entity-type][foqa-flights]]
                [[airframe-engine-field-set][base-field][engine-series-2]]]',
                0,
                'Unknown'
            );
            INSERT INTO dbtree(ems_id, id, name, nodetype, parent_id) 
            VALUES(
                3,
                '[-hub-][entity-type-group][[--][internal-type-group][root]]',
                '<root>',
                'root',
                NULL
            );
            INSERT INTO dbtree(ems_id, id, name, nodetype, parent_id) 
            VALUES(
                3,
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]',
                'FDW',
                'database_group',
                '[-hub-][entity-type-group][[--][internal-type-group][root]]'
            );
            INSERT INTO dbtree(ems_id, id, name, nodetype, parent_id) 
            VALUES(
                3,
                '[ems-core][entity-type][foqa-flights]',
                'FDW Flights',
                'database',
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]'
            );
            INSERT INTO kvmaps(ems_id, id, key, value) 
            VALUES(
                3,
                '[-hub-][field][[[ems-core][entity-type][foqa-flights]]
                [[airframe-engine-field-set][base-field][engine-series-2]]]',
                0,
                'Unknown'
            );
            COMMIT;
        """
    else:
        sql = """
            BEGIN TRANSACTION;
            CREATE TABLE dbtree (
                uri_root TEXT,
                ems_id INTEGER,
                id TEXT,
                name TEXT,
                nodetype TEXT,
                parent_id TEXT
            );
            CREATE TABLE kvmaps(
                uri_root TEXT,
                ems_id INTEGER,
                id TEXT,
                key TEXT,
                value TEXT
            );
            INSERT INTO dbtree(uri_root, ems_id, id, name, nodetype, parent_id) 
            VALUES(
                'https://ems.efoqa.com/api',
                1,
                '[-hub-][entity-type-group][[--][internal-type-group][root]]',
                '<root>',
                'root',
                NULL
            );
            INSERT INTO dbtree(uri_root, ems_id, id, name, nodetype, parent_id) 
            VALUES(
                'https://ems.efoqa.com/api',
                1,
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]',
                'FDW',
                'database_group',
                '[-hub-][entity-type-group][[--][internal-type-group][root]]'
            );
            INSERT INTO dbtree(uri_root, ems_id, id, name, nodetype, parent_id) 
            VALUES(
                'https://ems.efoqa.com/api',
                1,
                '[ems-core][entity-type][foqa-flights]',
                'FDW Flights',
                'database',
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]'
            );
            INSERT INTO kvmaps(uri_root, ems_id, id, key, value) 
            VALUES(
                'https://ems.efoqa.com/api',
                1,
                '[-hub-][field][[[ems-core][entity-type][foqa-flights]]
                [[airframe-engine-field-set][base-field][engine-series-2]]]',
                0,
                'Unknown'
            );
            INSERT INTO dbtree(uri_root, ems_id, id, name, nodetype, parent_id) 
            VALUES(
                'https://fas.efoqa.com/api',
                1,
                '[-hub-][entity-type-group][[--][internal-type-group][root]]',
                '<root>',
                'root',
                NULL
            );
            INSERT INTO dbtree(uri_root, ems_id, id, name, nodetype, parent_id) 
            VALUES(
                'https://fas.efoqa.com/api',
                1,
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]',
                'FDW',
                'database_group',
                '[-hub-][entity-type-group][[--][internal-type-group][root]]'
            );
            INSERT INTO dbtree(uri_root, ems_id, id, name, nodetype, parent_id) 
            VALUES(
                'https://fas.efoqa.com/api',
                1,
                '[ems-core][entity-type][foqa-flights]',
                'FDW Flights',
                'database',
                '[-hub-][entity-type-group][[ems-core][internal-type-group][fdw-types]]'
            );
            INSERT INTO kvmaps(uri_root, ems_id, id, key, value) 
            VALUES(
                'https://fas.efoqa.com/api',
                1,
                '[-hub-][field][[[ems-core][entity-type][foqa-flights]]
                [[airframe-engine-field-set][base-field][engine-series-2]]]',
                0,
                'Unknown'
            );
            COMMIT;
        """
    with sqlite3.connect('tmpDB.db') as conn:
        cursor = conn.cursor()
        cursor.executescript(sql)


def assert_tree(query, tree, legacy=False):
    if legacy:
        assert 'uri_root' not in tree.columns
    else:
        assert 'uri_root' in tree.columns
        assert len(tree['uri_root'].unique()) == 1
        assert all(tree['uri_root'] == query._conn._uri_root)
    assert len(tree['ems_id'].unique()) == 1
    assert all(tree['ems_id'] == query._ems_id)


def test_dbtree():
    create_tmpDB(legacy=False)
    query = MockFilterQuery('Engine Series', 'tmpDB.db')
    dbtree = query._FltQuery__flight._Flight__get_dbtree()
    assert_tree(query, dbtree, legacy=False)


def test_kvmaps():
    create_tmpDB(legacy=False)
    query = MockFilterQuery('Engine Series', 'tmpDB.db')
    kvmaps = query._FltQuery__flight._Flight__get_kvmaps()
    assert_tree(query, kvmaps, legacy=False)


def test_legacyDB_dbtree():
    create_tmpDB(legacy=True)
    query = MockFilterQuery('Engine Series', 'tmpDB.db')
    dbtree = query._FltQuery__flight._Flight__get_dbtree()
    assert_tree(query, dbtree, legacy=True)


def test_legacyDB_kvmaps():
    create_tmpDB(legacy=True)
    query = MockFilterQuery('Engine Series', 'tmpDB.db')
    kvmaps = query._FltQuery__flight._Flight__get_kvmaps()
    assert_tree(query, kvmaps, legacy=True)
