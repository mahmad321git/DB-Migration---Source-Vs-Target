import ast
import hashlib
import json
import os

import psycopg2

from helpers.redshift_helper import redshift_helper


class ColumnValidationManager():
    def __init__(self):
        self.redshift_helper = redshift_helper()

    def column_nullable_validation(self, resultant_schemas_name, resultant_table, column_name, target_dbname,
                                   target_host, target_port, target_username, target_password):

        sql = "select upper(table_name), upper(column_name), is_nullable from information_schema.columns " \
              "where upper(table_schema) in {} and is_nullable = 'NO' and upper(column_name) = '{}' " \
              "and upper(table_name) = '{}';".format(resultant_schemas_name, column_name, resultant_table)

        resultant_frame = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port,
                                                              target_username, target_password)
        test_status = False
        if len(resultant_frame) > 0:
            test_status = True
            result_str = "NOT NULL Constraint is implemented on column '{}' of table '{}' on target side ".format(
                column_name, resultant_table)
        else:
            result_str = "NOT NULL Constraint is not implemented on column '{}' of table '{}' on target side ".format(
                resultant_table, column_name)
        return test_status, result_str

    def primary_key_validation(self, resultant_schemas_name, resultant_table_name, resultant_column_name,
                               resultant_constraint_name, target_dbname, target_host, target_port, target_username,
                               target_password):

        sql = "SELECT tc.table_schema, tc.table_name, kc.column_name,tc.constraint_name " \
              "FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kc  " \
              "ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name " \
              "WHERE tc.constraint_type = 'PRIMARY KEY' and upper(tc.constraint_schema) in {} " \
              "and upper(tc.table_name) = '{}' and upper(kc.column_name) = '{}' " \
              "and upper(tc.constraint_name) = '{}'  ".format(resultant_schemas_name, resultant_table_name,
                                                              resultant_column_name, resultant_constraint_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        test_status = False
        if len(result) > 0:
            test_status = True
            result_str = "Primary Key Constraint is implemented on column '{}' of table '{}' on target side".format(
                resultant_column_name, resultant_table_name)
        else:
            result_str = "Primary Key Constraint is not implemented on column '{}' of table '{}' on target side".format(
                resultant_column_name, resultant_table_name)
        return test_status, result_str

    def unique_key_validation(self, resultant_schemas_name, resultant_table_name, resultant_column_name,
                              resultant_constraint_name, target_dbname, target_host, target_port, target_username,
                              target_password):

        sql = "SELECT tc.table_schema, tc.table_name, kc.column_name,tc.constraint_name " \
              "FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kc  " \
              "ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name " \
              "WHERE tc.constraint_type = 'UNIQUE' and upper(tc.constraint_schema) in {} " \
              "and upper(tc.table_name) = '{}' and upper(kc.column_name) = '{}' " \
              "and upper(tc.constraint_name) = '{}'  ".format(resultant_schemas_name, resultant_table_name,
                                                              resultant_column_name, resultant_constraint_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        test_status = False
        if len(result) > 0:
            test_status = True
            result_str = "Unique Key Constraint is implemented on column '{}' of table '{}' on target side".format(
                resultant_column_name, resultant_table_name)
        else:
            result_str = "Unique Key Constraint is not implemented on column '{}' of table '{}' on target side".format(
                resultant_column_name, resultant_table_name)
        return test_status, result_str

    def check_constraint_validation(self, constraint, condition, resultant_schemas_name, target_dbname, target_host,
                                    target_port, target_username, target_password):

        sql = "select constraint_name, check_clause from information_schema.check_constraints cc where upper(constraint_schema) in {} " \
              "and upper(constraint_name) = '{}' ".format(resultant_schemas_name, constraint)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        test_status = False
        if len(result) > 0:
            test_status = True
            result_str = 'Check Constraint "{}" is implemented on target side'.format(constraint)
        else:
            result_str = 'Check Constraint "{}" is not implemented on target side'.format(constraint)
        return test_status, result_str

    def foreign_key_validation(self, resultant_schemas_name, resultant_table_name, resultant_column_name,
                               resultant_constraint_name, target_dbname, target_host, target_port, target_username,
                               target_password):

        sql = "select distinct tc.constraint_schema, tc.table_name, kcu.column_name, tc.constraint_name " \
              "FROM information_schema.table_constraints AS tc  " \
              "join information_schema.key_column_usage AS kcu on tc.constraint_name = kcu.constraint_name " \
              "join information_schema.constraint_column_usage AS ccu on ccu.constraint_name = tc.constraint_name " \
              "where tc.constraint_type = 'FOREIGN KEY' " \
              "and upper(tc.table_name) = '{}' and upper(kcu.column_name) = '{}' " \
              "and upper(tc.constraint_name) = '{}' " \
              "and upper(tc.constraint_schema) in {} ".format(resultant_table_name, resultant_column_name,
                                                               resultant_constraint_name, resultant_schemas_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        test_status = False
        if len(result) > 0:
            test_status = True
            result_str = "Foreign Key Constraint is implemented on column '{}' of table '{}' on target side".format(
                resultant_column_name, resultant_table_name)
        else:
            result_str = "Foreign Key Constraint is not implemented on column '{}' of table '{}' on target side".format(
                resultant_column_name, resultant_table_name)
        return test_status, result_str

    def table_physical_existance_validation(self, table_name, schemas_list, target_dbname, target_host, target_port,
                                            target_username, target_password):

        sql = "select exists(select from information_schema.tables where upper(table_schema) in {} and upper(table_name) = '{}')".format(
            schemas_list, table_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        if result[0][0]:
            result_str = "Table '{}' exists on target side".format(table_name)
        else:
            result_str = "Table '{}' does not exist on target side".format(table_name)
        return result[0][0], result_str

    def view_physical_existance_validation(self, view_name, schemas_list, target_dbname, target_host, target_port,
                                           target_username, target_password):
        sql = "select exists(select from information_schema.views where upper(table_schema) in {} and upper(table_name) = '{}'); ".format(
            schemas_list, view_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        if result[0][0]:
            result_str = "View '{}' exists on target side".format(view_name)
        else:
            result_str = "View '{}' does not exist on target side".format(view_name)
        return result[0][0], result_str

    def column_names_match_validation(self, table_name, column_name, schemas_name, target_dbname, target_host,
                                      target_port, target_username, target_password):
        sql = "select table_name, column_name from information_schema.columns " \
              "where upper(table_name) = '{}' and upper(column_name) = '{}' " \
              "and upper(table_schema) in {} ".format(table_name, column_name, schemas_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        test_status = False
        if len(result) > 0:
            test_status = True
            result_str = "Column '{}' name matched in table '{}' on target side".format(column_name, table_name)
        else:
            result_str = "Column '{}' does not exist in table '{}' on target side".format(column_name, table_name)
        return test_status, result_str

    def get_data_types_from_target(self, table_name, schemas_name, target_dbname, target_host, target_port,
                                   target_username, target_password):
        sql = "select upper(column_name), data_type from information_schema.columns where upper(table_name)='{}' " \
              "and upper(table_schema) in {} order by column_name ASC ".format(table_name, schemas_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_indexes_from_targets(self):
        sql = "select upper(indexname), upper(indexdef) from pg_indexes where schemaname = 'dnatag_qa' ORDER by indexname"

        result = self.redshift_helper.query_redshift(sql)
        return result

    def get_indexes_from_target(self, schemas_name, target_dbname, target_host, target_port, target_username,
                                target_password):
        sql = "SELECT idx.indrelid :: REGCLASS AS table_name, i.relname AS index_name, " \
              "ARRAY( SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE) " \
              "FROM generate_subscripts(idx.indkey, 1) AS k ORDER BY k " \
              ") AS index_keys " \
              "FROM pg_index AS idx JOIN pg_class AS i ON i.oid = idx.indexrelid " \
              "JOIN pg_am AS am ON i.relam = am.oid JOIN pg_namespace AS NS ON i.relnamespace = NS.OID " \
              "JOIN pg_user AS U ON i.relowner = U.usesysid WHERE upper(ns.nspname) in {}  " \
              "order by idx.indrelid :: REGCLASS;".format(schemas_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_sequences_from_target(self, schemas_name, target_dbname, target_host, target_port, target_username,
                                  target_password):
        sql = "select sequencename, min_value, increment_by,start_value from pg_sequences where upper(sequenceowner) in {}".format(schemas_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_all_sequences_from_target(self, schemas_list, target_dbname, target_host, target_port, target_username, target_password):
        sql = "select sequence_name, minimum_value, increment, sequence_schema from information_schema.sequences " \
              "where upper(sequence_schema) in {} ".format(schemas_list)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_index_against_foreign_key_column(self, column_name, schemas_name, target_dbname, target_host, target_port,
                                             target_username, target_password):
        sql = "select * from pg_indexes where upper(schemaname) in {}" \
              "and upper(indexdef) like '%{}%'".format(schemas_name, column_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        if len(result) > 0:
            return True
        else:
            return False

    def get_partitons_from_target(self, table_name, target_dbname, target_host, target_port, target_username,
                                  target_password):
        sql = "select parent.relname as TABLE_NAME, child.relname as PARTITION_NAME, " \
              "most_child.relname as SUBPARTITION_NAME " \
              "from( " \
              "select p2.inhparent as parent, " \
              "p1.inhparent as child, " \
              "p1.inhrelid as most_child " \
              "from " \
              "pg_inherits p1 " \
              "right join pg_inherits p2 on " \
              "p1.inhparent = p2.inhrelid " \
              "where " \
              "p1.inhparent is not null) as master_table " \
              "left join pg_class parent on " \
              "master_table.parent = parent.oid " \
              "left join pg_class child on " \
              "master_table.child = child.oid " \
              "left join pg_class most_child on " \
              "master_table.most_child = most_child.oid " \
              "where " \
              "parent.relname = '{}' " \
              "order by child.relname, " \
              "most_child.relname".format(table_name)

        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_row_count_of_subpartition_from_target(self, schema_name, table_name, target_dbname, target_host, target_port, target_username, target_password):
        query = "select count(*) from {}.{}".format(schema_name, table_name)

        result = self.redshift_helper.query_redshift(query, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result[0][0]

    def call_part_subpart(self, sel_id, dl_libid, target_dbname, target_host, target_port, target_username,
                          target_password):
        print("GOING TO CREATE A PART/SUBPART on SEL_ID=" + sel_id + ", LIB_ID=" + dl_libid)
        sql = f''' call nrxinc."nrx_manresults.addsubpart" ('{dl_libid}','{sel_id}','Hello'); '''
        # sql = "call nrxinc.nrx_manresults.addsubpart ('{}','{}','Hello'); ".format(dl_libid, sel_id)

        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result

    def call_delpart(self, sel_id, target_dbname, target_host, target_port, target_username, target_password):
        print("GOING TO DELETE A PART/SUBPART on SEL_ID=" + sel_id)
        sql = f''' call nrxinc."nrx_manresults.delpart" ('{sel_id}','Hello'); '''
        # sql = 'call nrxinc."nrx_manresults.delpart" ("{}","Hello"); '.format(sel_id)

        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result

    def call_part_subpart(self, sel_id, dl_libid, target_dbname, target_host, target_port, target_username,
                          target_password):
        print("GOING TO DELETE A PART/SUBPART on SEL_ID=" + sel_id + ", LIB_ID=" + dl_libid)
        sql = f''' call nrxinc."nrx_manresults.delsubpart" ('{dl_libid}','{sel_id}','Hello'); '''
        # sql = 'call nrxinc."nrx_manresults.delsubpart" ("{}","{}","Hello");'.format(dl_libid, sel_id)

        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result

    def validate_partition_deletion(self, sel_id, target_dbname, target_host, target_port, target_username,
                                    target_password):
        sql = "select " \
              "count(*) " \
              "from " \
              "	pg_class t " \
              "       join pg_namespace ns on " \
              "       	t.relnamespace = ns.oid " \
              "       join pg_inherits p on " \
              "       	t.oid = p.inhparent " \
              "       join pg_class pt on " \
              "       	p.inhrelid = pt.oid " \
              "       where " \
              "       	ns.nspname = 'dnatag_devtest' " \
              "       	and t.relname = 'results' " \
              "       	and pt.relname = 'p' || '{}' limit 1 ".format(sel_id)
        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result[0][0]

    def validate_partition_subpartition_deletion(self, sel_id, dl_libid, target_dbname, target_host, target_port,
                                                 target_username, target_password):
        sql = "select " \
              "count(*) " \
              "from " \
              "pg_class t " \
              "join pg_namespace ns on " \
              "t.relnamespace = ns.oid " \
              "join pg_inherits p on " \
              "t.oid = p.inhparent " \
              "join pg_inherits sp on " \
              "p.inhrelid = sp.inhparent " \
              "join pg_class spt on " \
              "sp.inhrelid = spt.oid " \
              "where " \
              "ns.nspname = 'dnatag_devtest' " \
              "and t.relname = 'results' " \
              "and spt.relname = 's' || ( " \
              "select id " \
              "from " \
              "nrxinc.nrx_lib_sel " \
              "where " \
              "dl_libid = '{}' " \
              "and selection_id = '{}') limit 1 ".format(dl_libid, sel_id)
        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result[0][0]

    def validate_subpartition_added(self, sel_id, dl_libid, target_dbname, target_host, target_port, target_username,
                                    target_password):
        sql = "select " \
              "  count(*) " \
              "   from " \
              "       pg_class t " \
              "   join pg_namespace ns on " \
              "       t.relnamespace = ns.oid " \
              "   join pg_inherits p on " \
              "       t.oid = p.inhparent " \
              "   join pg_inherits sp on " \
              "       p.inhrelid = sp.inhparent " \
              "   join pg_class spt on " \
              "       sp.inhrelid = spt.oid " \
              "   where " \
              "       ns.nspname = 'dnatag_devtest' " \
              "       and t.relname = 'results' " \
              "       and spt.relname = 's' || ( " \
              "           select id " \
              "       from " \
              "           nrxinc.nrx_lib_sel " \
              "       where " \
              "           dl_libid = '{}' " \
              "           and selection_id = '{}') limit 1 ".format(dl_libid, sel_id)

        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result[0][0]

    def validate_partition_added(self, sel_id, target_dbname, target_host, target_port, target_username,
                                 target_password):
        sql = "select " \
              "      count(*) " \
              "       from " \
              "           pg_class t " \
              "       join pg_namespace ns on " \
              "           t.relnamespace = ns.oid " \
              "       join pg_inherits p on " \
              "           t.oid = p.inhparent " \
              "       join pg_class pt on " \
              "           p.inhrelid = pt.oid " \
              "       where " \
              "           ns.nspname = 'dnatag_devtest' " \
              "           and t.relname = 'results' " \
              "           and pt.relname = 'p' || ( " \
              "               select selection_id " \
              "           from " \
              "               nrxinc.nrx_lib_sel " \
              "           where " \
              "           selection_id = '{}' " \
              "           limit 1)".format(sel_id)

        result = self.redshift_helper.query_pg(sql, target_dbname, target_host, target_port, target_username,
                                               target_password)
        return result[0][0]

    def get_sequence_start_val(self, sequence_schema, sequence_name, target_dbname, target_host, target_port, target_username, target_password):
        sql = "select start_value from pg_sequences " \
              "where sequencename = '{}' and upper(schemaname) in {} ".format(sequence_name, sequence_schema)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result[0][0]

    def get_start_val(self, sequence_schema, sequence_name, target_dbname, target_host, target_port, target_username,
                     target_password):
        sql = "select start_value from pg_sequences " \
              "where sequencename = '{}' and upper(schemaname) in {} ".format(sequence_name, sequence_schema)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result[0][0]

    def get_max_val(self, seq_schema, table_name, column_name, target_dbname, target_host, target_port, target_username,
                    target_password):
        sql = "select coalesce (max({}), 0) from {}.{}".format(column_name, seq_schema, table_name)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result[0][0]

    def get_triggers_from_target(self, schemas_list, target_dbname, target_host, target_port, target_username, target_password):
        sql = "select event_object_table, trigger_name, action_statement, trigger_schema " \
              "from information_schema.triggers where upper(trigger_schema) in {} ".format(schemas_list)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_data_to_be_inserted(self):
        sql = "select lib_type_id + 1, concat(library_type,'1')::varchar(128) from dnatag_qa.library_types " \
              "order by lib_type_id desc limit 1;"
        result = self.redshift_helper.query_redshift(sql)
        return result

    def insert_data_into_table(self):
        sql = "insert into dnatag_qa.library_types " \
              "select null, concat(library_type,'1')::varchar(128) from dnatag_qa.library_types" \
              " order by lib_type_id desc limit 1;"
        self.redshift_helper.insert_row(sql)

    def get_last_record_of_table(self):
        sql = "select * from dnatag_qa.library_types " \
              "order by lib_type_id desc limit 1;"
        result = self.redshift_helper.query_redshift(sql)
        return result

    def get_table_columns_from_target(self, schemas_list, table, target_dbname, target_host, target_port, target_username,
                                      target_password):
        sql = "SELECT column_name FROM information_schema.columns where upper(table_schema) in {} " \
              "and table_name = '{}' ".format(schemas_list, table)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_data_types_of_columns_from_target(self, schemas_list, table, target_dbname, target_host, target_port, target_username,
                                              target_password):
        sql = "SELECT data_type FROM information_schema.columns where upper(table_schema) in {} " \
              "and table_name = '{}' ".format(schemas_list, table)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_max_row_from_target(self, schema, table, column, target_dbname, target_host, target_port, target_username,
                                target_password):
        sql = "SELECT * FROM {}.{} order by {} desc limit 1".format(schema, table, column)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result

    def get_min_row_from_target(self, schema, table, column):
        sql = "SELECT * FROM {}.{} order by {} asc limit 1".format(schema, table, column)
        result = self.redshift_helper.query_redshift(sql)
        return result

    def insert_new_row_in_table(self, sql, target_dbname, target_host, target_port, target_username, target_password):
        self.redshift_helper.insert_row(sql, target_dbname, target_host, target_port, target_username, target_password)

    def get_sequence_next_value(self, sequence, schema, target_dbname, target_host, target_port, target_username,
                                target_password):
        sql = "select nextval ('{}.{}')".format(schema, sequence)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result[0][0]

    def get_sequence_last_value(self, schema, sequence, target_dbname, target_host, target_port, target_username,
                                target_password):
        sql = "select coalesce(last_value,0) from pg_sequences where sequencename = '{}' and upper(schemaname) in {}".format(sequence, schema)
        result = self.redshift_helper.query_redshift(sql, target_dbname, target_host, target_port, target_username,
                                                     target_password)
        return result[0][0]

    def create_temp_table_at_target(self, dest_schema, temp_table, trigger_schema, trigger_table, target_dbname,
                                    target_host, target_port, target_username, target_password):

        create_table_statement = "create table {}.{} " \
                                 "as select * from {}.{} limit 1".format(dest_schema, temp_table, trigger_schema,
                                                                         trigger_table)

        self.redshift_helper.execute_query(create_table_statement, target_dbname, target_host, target_port,
                                           target_username, target_password)

    def create_trigger_at_target(self, trigger_name, dest_schema, temp_table, trigger_action, target_dbname,
                                 target_host, target_port, target_username, target_password):

        create_trigger_statement = "create trigger {} before " \
                                   "insert on {}.{} " \
                                   "for each row {};".format(trigger_name, dest_schema, temp_table, trigger_action)

        self.redshift_helper.execute_query(create_trigger_statement, target_dbname, target_host, target_port,
                                           target_username, target_password)

    def drop_table_at_target(self, schema_name, table_name, target_dbname, target_host, target_port, target_username,
                             target_password):
        drop_table_statement = "drop table {}.{}".format(schema_name, table_name)

        self.redshift_helper.execute_query(drop_table_statement, target_dbname, target_host, target_port,
                                           target_username, target_password)

    def load_json_file_containing_triggers_mappings(self, table, file_name):
        dir_path = os.path.dirname(__file__)
        rel_path = "../configs/{}".format(file_name)
        abs_path = os.path.join(dir_path, rel_path)
        with open(abs_path) as mapping_file:
            return ast.literal_eval((mapping_file.read()))[table]

    def diff(self, li1, li2):
        return list(set(li1) - set(li2))

    def create_postgres_connection(self, host, port, database, username, password):
        connection = psycopg2.connect(user=username,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
        cursor = connection.cursor()
        return connection, cursor

    def get_target_views_data(self, view_name, postgres_host, postgres_port, postgres_database, postgres_schema, postgres_user,
                              postgres_password, sample_rows):
        postgres_connection, postgres_cursor = self.create_postgres_connection(postgres_host, postgres_port, postgres_database,
                                                                          postgres_user, postgres_password)
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
        numeric_columns = {
            "bb_codons_v": ["position"],
            "results_v": ["ligand_freq_ratio"],
            "selections_stats_v": ["freq_ratio_mean", "freq_ratio_stddevp", "freq_ratio_mean_gt1", "freq_ratio_stddevp_gt1", "standard_threshold"]
        }

        value = view_column_mapping[view_name]
        result = []

        try:
            postgres_cursor.execute('''select * from {schema_name}.{view_name} limit 1'''.format(schema_name=postgres_schema,
                                                                                                 view_name=view_name))

            column_names = [x.lower() for x in value]
            column_names_list = [x[0] for x in postgres_cursor.description]
            concat_columns = self.diff(column_names_list, column_names)
            concat_columns.sort()
            for col_key, col_value in numeric_columns.items():
                if col_key == view_name:
                    for i in range(len(col_value)):
                        for x in range(len(concat_columns)):
                            if concat_columns[x] == col_value[i]:
                                concat_columns[x] = 'cast(' + col_value[i] + ' as int)'
            postgres_columns = ','.join(column_names) + "," + "concat(" + ','.join(concat_columns) + ') as ROWDATA'
            postgres_cursor.execute('''select {columns}
                        from dnatag.{view_name} limit {sample_rows}'''.format(columns=postgres_columns, view_name=view_name,
                                                                              sample_rows=sample_rows))
            pg_index_column = []
            for col in column_names:
                pg_index_column.append([])
            for row in postgres_cursor:
                for i in range(len(column_names)):
                    pg_index_column[i].append(str(row[i]))

            for i in range(len(column_names)):
                if i > 0:
                    postgres_where_columns = postgres_where_columns + " and " + column_names[i] + " in (" + (', '.join(
                        "'" + str(item) + "'" for item in
                        (pg_index_column[i] if len(pg_index_column[i]) > 0 else ['-999999']))) + ")"
                else:
                    postgres_where_columns = column_names[i] + " in (" + (', '.join("'" + str(item) + "'" for item in (
                        pg_index_column[i] if len(pg_index_column[i]) > 0 else ['-999999']))) + ")"

            postgres_cursor.execute('''select {columns}
                        from dnatag.{view_name} WHERE {postgres_where_columns}'''.format(schema_name=postgres_schema,
                                                                                         columns=postgres_columns,
                                                                                         view_name=view_name,
                                                                                         postgres_where_columns=postgres_where_columns))
            postgres_index_column = []
            postgres_hash_column = []

            for col in column_names:
                postgres_index_column.append([])
            for row in postgres_cursor:
                oracle_view_data = []
                for i in range(len(column_names)):
                    postgres_index_column[i].append(str(row[i]))
                    oracle_view_data.append(str(row[i]))
                oracle_view_data.append(hashlib.md5(row[len(column_names)].encode('utf-8')).hexdigest())
                result.append(oracle_view_data)
        except Exception as ex:
            print('exception at target side')
        return result


if __name__ == '__main__':
    obj = ColumnValidationManager()
    print(obj.get_row_count_of_subpartition_from_target('dnatag', 'sp_15_5', 'nurix',
        'nurix-nurixcluster-mgr.cluster-ctuj3l6cvpao.us-west-2.rds.amazonaws.com', '5432',
                                                        'naman', 'test123'))
