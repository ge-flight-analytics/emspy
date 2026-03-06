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

    conn = sqlite3.connect('tmpDB.db')
    cursor = conn.cursor()
    cursor.executescript(sql)
    cursor.close()
    conn.close()

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


def test_sql_injection_single_quote():
    """Test that database IDs with single quotes don't cause SQL injection"""
    sql = """
        BEGIN TRANSACTION;
        CREATE TABLE fieldtree (
            uri_root TEXT,
            ems_id INTEGER,
            db_id TEXT,
            id TEXT,
            nodetype TEXT,
            type TEXT,
            name TEXT,
            parent_id TEXT
        );
        INSERT INTO fieldtree(uri_root, ems_id, db_id, id, nodetype, type, name, parent_id)
        VALUES(
            'https://ems.efoqa.com/api',
            1,
            'test-db-id',
            'field-1',
            'field',
            'number',
            'Normal Field',
            NULL
        );
        INSERT INTO fieldtree(uri_root, ems_id, db_id, id, nodetype, type, name, parent_id)
        VALUES(
            'https://ems.efoqa.com/api',
            1,
            'GPWS: Don''t Sink',
            'field-2',
            'field',
            'number',
            'GPWS Field',
            NULL
        );
        COMMIT;
    """
    with sqlite3.connect('tmpDB.db') as conn:
        cursor = conn.cursor()
        cursor.executescript(sql)

    # Import LocalData to test directly
    from emspy.query import LocalData
    ld = LocalData('tmpDB.db')

    # Test 1: Normal database ID (should work)
    result1 = ld.get_data("fieldtree", ("ems_id = ? and db_id = ?", (1, 'test-db-id')))
    assert len(result1) == 1
    assert result1.iloc[0]['name'] == 'Normal Field'

    # Test 2: Database ID with single quote (the original issue - should work now)
    result2 = ld.get_data("fieldtree", ("ems_id = ? and db_id = ?", (1, "GPWS: Don't Sink")))
    assert len(result2) == 1
    assert result2.iloc[0]['name'] == 'GPWS Field'

    # Test 3: SQL injection attempt - should return no results, not error
    result3 = ld.get_data("fieldtree", ("ems_id = ? and db_id = ?", (1, "' OR '1'='1")))
    assert len(result3) == 0  # Should not return all rows

    # Test 4: Verify total row count hasn't changed (data integrity)
    all_results = ld.get_data("fieldtree")
    assert len(all_results) == 2  # Only our 2 test rows

    ld.close()


def test_sql_injection_delete():
    """Test that delete operations with single quotes don't cause SQL injection"""
    sql = """
        BEGIN TRANSACTION;
        CREATE TABLE fieldtree (
            uri_root TEXT,
            ems_id INTEGER,
            db_id TEXT,
            id TEXT,
            nodetype TEXT,
            type TEXT,
            name TEXT,
            parent_id TEXT
        );
        INSERT INTO fieldtree(uri_root, ems_id, db_id, id, nodetype, type, name, parent_id)
        VALUES(
            'https://ems.efoqa.com/api',
            1,
            'test-db-1',
            'field-1',
            'field',
            'number',
            'Field 1',
            NULL
        );
        INSERT INTO fieldtree(uri_root, ems_id, db_id, id, nodetype, type, name, parent_id)
        VALUES(
            'https://ems.efoqa.com/api',
            1,
            'GPWS: Don''t Sink',
            'field-2',
            'field',
            'number',
            'Field 2',
            NULL
        );
        INSERT INTO fieldtree(uri_root, ems_id, db_id, id, nodetype, type, name, parent_id)
        VALUES(
            'https://ems.efoqa.com/api',
            1,
            'test-db-3',
            'field-3',
            'field',
            'number',
            'Field 3',
            NULL
        );
        COMMIT;
    """
    with sqlite3.connect('tmpDB.db') as conn:
        cursor = conn.cursor()
        cursor.executescript(sql)

    from emspy.query import LocalData
    ld = LocalData('tmpDB.db')

    # Test 1: Delete with single quote in db_id
    ld.delete_data("fieldtree", ("ems_id = ? and db_id = ?", (1, "GPWS: Don't Sink")))

    # Verify only the correct row was deleted
    remaining = ld.get_data("fieldtree")
    assert len(remaining) == 2  # Should have 2 rows left
    assert 'Field 2' not in remaining['name'].values  # Field 2 should be deleted
    assert 'Field 1' in remaining['name'].values  # Field 1 should remain
    assert 'Field 3' in remaining['name'].values  # Field 3 should remain

    # Test 2: SQL injection attempt in delete - should not delete anything
    ld.delete_data("fieldtree", ("ems_id = ? and db_id = ?", (1, "' OR '1'='1")))

    # Verify nothing was deleted
    still_remaining = ld.get_data("fieldtree")
    assert len(still_remaining) == 2  # Should still have 2 rows

    ld.close()
