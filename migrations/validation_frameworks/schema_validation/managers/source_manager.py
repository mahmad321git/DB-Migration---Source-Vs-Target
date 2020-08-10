import ast
import hashlib
import json

import cx_Oracle
import os

# os.chdir('C:\oracle_instant_client\instantclient_19_6')


def get_connection(hostname, port, user, password, sid):
    dsn = '%s:%s/%s' % (hostname, port, sid)
    connection = cx_Oracle.connect(user, password, dsn)
    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor


def get_query_results(cursor):
    return cursor.fetchall()


def get_not_null_columns_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query_string = "SELECT TABLE_NAME , COLUMN_NAME, OWNER FROM ALL_TAB_COLUMNS  WHERE OWNER in {} AND " \
                   "ALL_TAB_COLUMNS.NULLABLE= 'N' AND TABLE_NAME NOT LIKE '%_V' AND TABLE_NAME NOT LIKE '%_V2' ".format(schemas_list)
    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query_string)
    results = get_query_results(cursor)
    return results


def get_check_columns_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query_string = "select constraint_name, search_condition, owner from all_constraints " \
                   "where constraint_type='C' and owner in {} ".format(schemas_list)
    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query_string)
    results = get_query_results(cursor)
    return results


def get_primary_keys_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "select all_constraints.owner, all_cons_columns.table_name, all_cons_columns.column_name, all_constraints.CONSTRAINT_NAME " \
            "from all_constraints, all_cons_columns where all_constraints.constraint_type = 'P' " \
            "and all_constraints.constraint_name = all_cons_columns.constraint_name " \
            "and all_constraints.owner in {} AND all_cons_columns.owner in {} " \
            "and all_constraints.owner = all_cons_columns.owner".format(schemas_list, schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_unique_keys_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "select all_constraints.owner, all_cons_columns.table_name, all_cons_columns.column_name, all_constraints.CONSTRAINT_NAME " \
            "from all_constraints, all_cons_columns where all_constraints.constraint_type = 'U' " \
            "and all_constraints.constraint_name = all_cons_columns.constraint_name " \
            "and all_constraints.owner in {} AND all_cons_columns.owner in {} " \
            "and all_constraints.owner = all_cons_columns.owner".format(schemas_list, schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_foreign_keys_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "select all_cons_columns.owner, all_cons_columns.table_name, all_cons_columns.column_name, all_cons_columns.constraint_name " \
            "from all_constraints, all_cons_columns " \
            "where all_constraints.constraint_type = 'R' " \
            "and all_constraints.constraint_name = all_cons_columns.constraint_name " \
            "and all_constraints.owner in {} AND all_cons_columns.owner in {} " \
            "and all_constraints.owner = all_cons_columns.owner".format(schemas_list, schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_tables_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT table_name, owner FROM all_tables WHERE owner in {}".format(schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_views_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT VIEW_NAME, owner FROM all_views WHERE owner in {}".format(schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_columns_list(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT upper(table_name), upper(column_name), upper(owner) FROM all_tab_columns WHERE owner in {} " \
            "and table_name not like '%_V' and table_name not like '%_V2'".format(schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_data_types_from_source(table_name, schemas_list, source_host, source_port, source_username, source_password,
                               source_sid):
    query = "SELECT upper(column_name), data_type FROM all_tab_columns " \
            "WHERE upper(table_name) = '{}' and upper(owner) = {} order by column_name ASC".format(table_name,
                                                                                                     schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_indexes_from_source(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT lower(table_name), lower(index_name), lower(column_name), lower(table_owner) FROM all_ind_columns WHERE table_owner in {}".format(
        schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_sequences_from_source(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT lower(SEQUENCE_NAME), MIN_VALUE, INCREMENT_BY, LAST_NUMBER FROM DBA_SEQUENCES WHERE SEQUENCE_OWNER in {}".format(
        schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_tables_that_has_partitions(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT DISTINCT LOWER(TABLE_NAME), LOWER(TABLE_OWNER) FROM ALL_TAB_SUBPARTITIONS WHERE TABLE_OWNER in {}".format(
        schemas_list)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_partitons_from_source(table_name, schemas_list, source_host, source_port, source_username, source_password,
                              source_sid):
    query = "SELECT LOWER(TABLE_NAME) AS table_name, LOWER(PARTITION_NAME) AS partition_name, " \
            "LOWER(SUBPARTITION_NAME) AS subpartition_name " \
            "FROM ALL_TAB_SUBPARTITIONS " \
            "WHERE upper(TABLE_OWNER) in {} AND lower(TABLE_NAME) = '{}' " \
            "ORDER BY PARTITION_NAME, SUBPARTITION_NAME".format(schemas_list, table_name)

    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_triggers_from_source(schemas_list, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT TRIGGER_NAME, TABLE_NAME FROM DBA_TRIGGERS WHERE UPPER(owner) IN {}".format(schemas_list)
    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results


def get_row_count_of_subpartition_from_source(table, sel_id, dl_libid, schemas, source_host, source_port, source_username, source_password, source_sid):
    query = "SELECT count(*) FROM {}.{} where selection_id = '{}' and dl_libid = '{}'".format(
        schemas, table, sel_id, dl_libid)
    source_conn = get_connection(source_host, source_port, source_username, source_password, source_sid)
    cursor = execute_query(source_conn, query)
    results = get_query_results(cursor)
    return results[0][0]


def load_mapping_file(file_name):
    dir_path = os.path.dirname(__file__)
    rel_path = "../configs/{}".format(file_name)
    abs_path = os.path.join(dir_path, rel_path)
    with open(abs_path) as mapping_file:
        return ast.literal_eval((mapping_file.read()))


def diff(li1, li2):
    return list(set(li1) - set(li2))


def create_oracle_connection(host, port, sid, username, password):
    dsn_tns = cx_Oracle.makedsn(host, port,sid)
    conn = cx_Oracle.connect(user=username, password=password, dsn=dsn_tns)
    c = conn.cursor()
    return conn, c


def get_source_views_data(view_name, oracle_host, oracle_port, oracle_database, oracle_schema, oracle_user,
                          oracle_password, sample_rows):
    oracle_connection, oracle_cursor = create_oracle_connection(oracle_host, oracle_port, oracle_database, oracle_user,
                                                                oracle_password)
    view_column_mapping = {
        "bb_codons_v": ["codon_id", "DL_LIBID"],
        "libraries_v": ["dl_libid"],
        "pools_v": ["pool_id"],
        "pools_v2": ["pool_id"],
        "results_v": ["selection_id"],
        "selections_stats_v": ["selection_id"],
        "selections_v": ["selection_id"],
        "templates_v": ["template_id"]
    }
    value = view_column_mapping[view_name]
    result = []
    try:
        column_names = [x.upper() for x in value]

        oracle_cursor.execute('''select *
                    from {schema_name}.{view_name} WHERE ROWNUM < 1'''.format(schema_name=oracle_schema,
                                                                              view_name=view_name))
        column_names_list = [x[0] for x in oracle_cursor.description]
        concat_columns = diff(column_names_list, column_names)
        concat_columns.sort()
        oracle_columns = ','.join(column_names) + "," + '||'.join(concat_columns) + ' as ROWDATA'

        oracle_cursor.execute('''select {columns}
                    from DNATAG.{view_name} WHERE ROWNUM <= {sample_rows}'''.format(columns=oracle_columns,
                                                                                    view_name=view_name,
                                                                                    sample_rows=sample_rows))

        oracle_index_column = []
        for col in column_names:
            oracle_index_column.append([])
        oracle_hash_column = []
        for row in oracle_cursor:
            oracle_view_data = []
            for i in range(len(column_names)):
                oracle_index_column[i].append(str(row[i]))
                oracle_view_data.append(str(row[i]))
            oracle_view_data.append(hashlib.md5(row[len(column_names)].encode('utf-8')).hexdigest())
            result.append(oracle_view_data)
    except Exception as ex:
        print('exception at source side')
    return result
# print(get_row_count_of_subpartition_from_source('LIGANDS_STATS', 15, 5, 'DNATAG', '172.24.20.242', '1521',
#                                                 'dms_user', 'dmswerty', 'ORA12C'))
