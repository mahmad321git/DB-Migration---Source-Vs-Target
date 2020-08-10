from managers import source_manager


def convert_result_to_dictionary(results):
    dict_list = []
    for row in results:
        dict_list.append({
            "constraint_name": row[0],
            "search_condition": row[1],
            "schema_name": row[2]
        })
    return dict_list


def convert_result_set_to_dictionary(results):
    dict_list = []
    for row in results:
        dict_list.append({
            "table_name": row[0],
            "column_name": row[1],
            "schema_name": row[2]
        })
    return dict_list


def convert_pk_or_uk_or_fk_result_set_to_dictionary(results):
    dict_list = []
    for row in results:
        dict_list.append({
            "schema_name": row[0],
            "table_name": row[1],
            "column_name": row[2],
            "constraint_name": row[3]
        })
    return dict_list


def get_schemas_list(source_schemas):
    source_schemas = source_schemas.split('.')
    schemas_list = "("

    for schema in source_schemas:
        schemas_list = schemas_list + "'" + schema.upper() + "',"

    schemas_list = schemas_list[:-1]
    schemas_list = schemas_list + ")"
    return schemas_list


def get_check_constraints_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_check_columns_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    result_converted_to_dict = convert_result_to_dictionary(result)
    return result_converted_to_dict


def get_not_null_columns_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_not_null_columns_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    result_converted_to_dict = convert_result_set_to_dictionary(result)
    return result_converted_to_dict


def get_primary_keys_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_primary_keys_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    return convert_pk_or_uk_or_fk_result_set_to_dictionary(result)


def get_unique_keys_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_unique_keys_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    return convert_pk_or_uk_or_fk_result_set_to_dictionary(result)


def get_foreign_keys_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_foreign_keys_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    return convert_pk_or_uk_or_fk_result_set_to_dictionary(result)


def get_tables_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_tables_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    dict_list = []
    for row in result:
        dict_list.append({
            "table_name": row[0],
            "schema_name": row[1]
        })
    return dict_list


def get_views_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_views_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    dict_list = []
    for row in result:
        dict_list.append({
            "view_name": row[0],
            "schema_name": row[1]
        })
    return dict_list


def get_columns_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_columns_list(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    dict_list = []
    for row in result:
        dict_list.append({
            "table_name": row[0],
            "column_name": row[1],
            "schema_name": row[2]
        })
    return dict_list


def get_indexes_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_indexes_from_source(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    return result


def get_sequences_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_sequences_from_source(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    return result


def get_triggers_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    result = source_manager.get_triggers_from_source(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    return result


def get_partitions_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid):
    schemas_list = get_schemas_list(source_schemas)
    partitioned_tables = source_manager.get_tables_that_has_partitions(schemas_list, source_host, source_port, source_username, source_password, source_sid)
    source_partitions_list = []
    for table in partitioned_tables:
        table_name = table[0]
        schema_name = table[1]
        partitions_list = source_manager.get_partitons_from_source(table_name, schemas_list, source_host, source_port, source_username, source_password, source_sid)
        source_partitions_list = source_partitions_list + partitions_list
    return source_partitions_list