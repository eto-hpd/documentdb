SET search_path TO documentdb_core, documentdb_api, public;
SET documentdb.next_collection_id TO 9300;
SET documentdb.next_collection_index_id TO 9300;

-- Build three databases with different data volumes.
SELECT documentdb_api.create_collection('test_1', 'coll');

SELECT documentdb_api.create_collection('test_2', 'coll');
SELECT COUNT(documentdb_api.insert_one(
    'test_2',
    'coll',
    FORMAT('{ "_id": %s, "a": %s, "b": %s }', i, i, i * 2)::bson))
FROM generate_series(1, 100) AS i;

SELECT documentdb_api.create_collection('test_3', 'coll');
SELECT COUNT(documentdb_api.insert_one(
    'test_3',
    'coll',
    FORMAT('{ "_id": %s, "a": %s, "b": %s, "c": %s }', i, i, i * 2, i * 3)::bson))
FROM generate_series(1, 1000) AS i;

-- Test nameOnly.
SELECT documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "nameOnly": true,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

-- Test authorizedDatabases.
SELECT documentdb_api_catalog.bson_dollar_project(
           documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases'),
           '{ "databases.name": 1, "databases.sizeOnDisk": 1, "databases.empty": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "authorizedDatabases": true,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

SELECT documentdb_api_catalog.bson_dollar_project(
           list_databases,
           '{ "totalSize": 1, "ok": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "authorizedDatabases": true,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }');

-- Test filter.
SELECT documentdb_api_catalog.bson_dollar_project(
           documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases'),
           '{ "databases.name": 1, "databases.sizeOnDisk": 1, "databases.empty": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

SELECT documentdb_api_catalog.bson_dollar_project(
           list_databases,
           '{ "totalSize": 1, "ok": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_3" }
              ]
            }
          }');

-- Cross test nameOnly + filter.
SELECT documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "nameOnly": true,
            "filter": {
              "$or": [
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

-- Cross test nameOnly + authorizedDatabases.
SELECT documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "nameOnly": true,
            "authorizedDatabases": true,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

-- Cross test nameOnly + authorizedDatabases + filter.
SELECT documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "nameOnly": true,
            "authorizedDatabases": true,
            "filter": {
              "name": "test_3"
            }
          }')
ORDER BY 1;

-- Insert more data and test listDatabases again.
SELECT COUNT(documentdb_api.insert_one(
    'test_1',
    'coll',
    FORMAT('{ "_id": %s, "a": %s, "b": %s }', i, i, i * 2)::bson))
FROM generate_series(1, 50) AS i;

SELECT COUNT(documentdb_api.insert_one(
    'test_2',
    'coll',
    FORMAT('{ "_id": %s, "a": %s, "b": %s }', i, i, i * 2)::bson))
FROM generate_series(101, 300) AS i;

SELECT documentdb_api_catalog.bson_dollar_project(
           documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases'),
           '{ "databases.name": 1, "databases.sizeOnDisk": 1, "databases.empty": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

SELECT documentdb_api_catalog.bson_dollar_project(
           list_databases,
           '{ "totalSize": 1, "ok": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }');

-- Drop a collection and test listDatabases again.
SELECT documentdb_api.drop_collection('test_2', 'coll');

SELECT documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "nameOnly": true,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

SELECT documentdb_api_catalog.bson_dollar_project(
           documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases'),
           '{ "databases.name": 1, "databases.sizeOnDisk": 1, "databases.empty": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

-- Drop a database and test listDatabases again.
SELECT documentdb_api.drop_database('test_3');

SELECT documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "nameOnly": true,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

SELECT documentdb_api_catalog.bson_dollar_project(
           documentdb_api_catalog.bson_dollar_unwind(list_databases, '$databases'),
           '{ "databases.name": 1, "databases.sizeOnDisk": 1, "databases.empty": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }')
ORDER BY 1;

SELECT documentdb_api_catalog.bson_dollar_project(
           list_databases,
           '{ "totalSize": 1, "ok": 1 }')
FROM documentdb_api.list_databases(
         '{
            "listDatabases": 1,
            "filter": {
              "$or": [
                { "name": "test_1" },
                { "name": "test_2" },
                { "name": "test_3" }
              ]
            }
          }');
