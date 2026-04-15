CREATE OR REPLACE FUNCTION __API_SCHEMA_V2__.list_databases(
    p_list_databases_spec __CORE_SCHEMA__.bson)
 RETURNS __CORE_SCHEMA__.bson
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $$command_list_databases$$;

#ifdef __RBAC_SCHEMA_ENABLED__
CREATE OR REPLACE FUNCTION documentdb_api_v2.list_databases(
    p_list_databases_spec __CORE_SCHEMA__.bson)
 RETURNS __CORE_SCHEMA__.bson
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $$command_list_databases$$;

CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL_V2__.list_databases_worker(
    IN p_database_names text[],
    IN p_collection_offsets integer[],
    IN p_collection_ids bigint[])
RETURNS __CORE_SCHEMA__.bson
LANGUAGE C
AS 'MODULE_PATHNAME', $function$command_list_databases_worker$function$;

CREATE OR REPLACE FUNCTION documentdb_api_internal_readonly.list_databases_worker(
    IN p_database_names text[],
    IN p_collection_offsets integer[],
    IN p_collection_ids bigint[])
RETURNS __CORE_SCHEMA__.bson
LANGUAGE C
AS 'MODULE_PATHNAME', $function$command_list_databases_worker$function$;
