import json

import boto3
import psycopg2
from pyspark.conf import SparkConf
#from pyspark.sql import SparkSession
from pyspark.sql import *
import findspark
import threading
import sys
import argparse

from pyspark.sql.types import *

from db_helper import DbHelper

findspark.init()
client = boto3.client('emr')


parser = argparse.ArgumentParser()

parser.add_argument("--CONFIG_JSON", help="Config File Description")
args = parser.parse_args()
CONFIG_JSON = args.CONFIG_JSON

def generate_numeric_ranges(lower_bound, upper_bound, num_partitions):
    number_ranges = []

    min_num = int(lower_bound)
    max_num = int(upper_bound)

    if min_num > max_num:
        raise Exception('Min number ({}) is greater than Max number ({})'.format(min_num, max_num))

    difference = (max_num - min_num) + 1

    if difference == 0 or difference < num_partitions:
        number_ranges.append((min_num, max_num))
    else:
        partition_numbers = int(difference / num_partitions)
        for partition in range(num_partitions):
            partition_min_number = min_num + (partition_numbers * partition)
            if partition == num_partitions - 1:
                partition_max_num = max_num
            else:
                partition_max_num = partition_min_number + (partition_numbers - 1)
            number_ranges.append((partition_min_number, partition_max_num))

    return number_ranges


def generate_batches_from_range(lower_bound, upper_bound, num_partitions=1):
    return generate_numeric_ranges(lower_bound, upper_bound, num_partitions)


def generate_batches_for_range(column, lower_bound, upper_bound, num_partitions=1):
    batches = []
    for min_val, max_val in generate_batches_from_range(lower_bound, upper_bound, num_partitions):
        batch = '''{min},{max}'''
        batch = batch.format(min=min_val, max=max_val).strip()
        batches.append(batch)
    return batches


def generate_predicates_for_range(source_table, column, lower_bound, upper_bound, num_partitions=1):
    predicates = []
    table_name = source_table.split('.')
    alias = table_name[1] + '.'
    where_clause = '''{alias_placeholder}{column} IS NOT NULL AND {alias_placeholder}{column} >= {min_val} AND {alias_placeholder}{column} <= {max_val}'''.strip()
    for min_val, max_val in generate_batches_from_range(lower_bound, upper_bound, num_partitions):
        predicate = where_clause.format(alias_placeholder=alias, column=column, min_val=min_val, max_val=max_val)
        predicates.append(predicate)

    return predicates


def execute_query_on_target(query):
    con = psycopg2.connect(dbname='nurix',
                           host='nurix-nurixcluster-mgr.cluster-ctuj3l6cvpao.us-west-2.rds.amazonaws.com',
                           port='5432',
                           user='ahmed',
                           password='test123')
    con.autocommit = True
    cur = con.cursor()
    cur.execute(query)


def read_data_from_source_table2(table, source_predicates, custom_dtypes, spark):
    df = spark.read \
        .option('fetchsize', 100000) \
        .option('customSchema', custom_dtypes) \
        .jdbc(url="jdbc:oracle:thin:dms_user/dmswerty@172.24.20.242:1521/ORA12C", table=table,
              predicates=source_predicates)
    return df


def write_data_to_target(source_df, temp_table, numPartition):
    source_df.write.option("numPartitions", numPartition) \
        .jdbc(
        url="jdbc:postgresql://nurix-nurixcluster-mgr.cluster-ctuj3l6cvpao.us-west-2.rds.amazonaws.com:5432/nurix",
        table=temp_table,
        mode='overwrite',
        properties={
            'user': 'ahmed',
            'password': 'test123',
        })


def generate_where_clause(column, lower_bound, upper_bound):
    where_clause = '''where {column} >= {lower} AND {column} <= {upper}'''
    where_clause = where_clause.format(column=column, lower=lower_bound, upper=upper_bound)
    return where_clause


def query_redshift(query):
    db_helper = DbHelper()
    host = 'nurix-nurixcluster-mgr.cluster-ctuj3l6cvpao.us-west-2.rds.amazonaws.com'
    conn_string1 = db_helper.generate_connection_string("postgresql", "ahmed", "test123", host, "5432", "nurix",
                                                        '')
    connection = db_helper.create_conn(conn_string1)
    df = db_helper.query_execution(query)

    return df


def query_redshift_using_cursor(query):
    con = psycopg2.connect(dbname='nurix',
                           host='nurix-nurixcluster-mgr.cluster-ctuj3l6cvpao.us-west-2.rds.amazonaws.com',
                           port='5432',
                           user='ahmed',
                           password='test123')
    cur = con.cursor()
    cur.execute(query)
    result = cur.fetchall()
    return result


def delete_temp_table(temp_table):
    query = 'DROP TABLE {}'.format(temp_table)
    execute_query_on_target(query)


def get_invalid_dest_rows(temp_table, target_table, where_clause):
    sql = 'select *  FROM {} {} EXCEPT select *  FROM {}'.format(target_table, where_clause, temp_table)
    dest_df = query_redshift(sql)
    return dest_df


def format_row(rows, column_names):
    formatted_list = ''
    for column_name, value in zip(column_names, rows):
        formatted_list = formatted_list + f'[{column_name} : {value}] '

    return formatted_list


def insert_invalid_rows(source_df, temp_table, target_table, lower_bound, upper_bound, where_clause):
    source_rows = source_df.values.tolist()
    column_names_list = source_df.columns.values
    dest_df = get_invalid_dest_rows(temp_table, target_table, where_clause)
    target_table_schema = target_table.split('.')
    target_schema = target_table_schema[0]
    dest_rows = dest_df.values.tolist()
    # step_id = get_current_step_id()

    for s_row in source_rows:
        s_row_status = 'Missing'
        for d_row in dest_rows:
            if target_table == target_schema + '.ligands':
                if s_row[0] == d_row[0] and s_row[1] == d_row[1] and s_row[2] == d_row[2] and s_row[3] == d_row[3]:
                    s_row_status = 'Mismatch'
                    sql = "insert into dnatag_wip.invalid_records (table_name, lower_bound, upper_bound, " \
                          "source_row, dest_row, status) values('" + target_table + "'," + lower_bound + "," + upper_bound \
                          + ",'" + format_row(s_row, column_names_list) + "','" + format_row(d_row,
                                                                                             column_names_list) + "','" + s_row_status + "') "

                    execute_query_on_target(sql)

            elif target_table != target_schema + '.ligands':
                if s_row[0] == d_row[0]:
                    s_row_status = 'Mismatch'
                    sql = "insert into dnatag_wip.invalid_records (table_name, lower_bound, upper_bound, " \
                          "source_row, dest_row, status) values('" + target_table + "'," + lower_bound + "," + upper_bound \
                          + ",'" + format_row(s_row, column_names_list) + "','" + format_row(d_row,
                                                                                             column_names_list) + "','" + s_row_status + "') "

                    execute_query_on_target(sql)

        if s_row_status == 'Missing':
            sql = "insert into dnatag_wip.invalid_records (table_name, lower_bound, upper_bound, " \
                  "source_row, dest_row, status) values('" + target_table + "'," + lower_bound + "," + upper_bound \
                  + ",'" + format_row(s_row, column_names_list) + "', NULL ,'" + s_row_status + "')"
            execute_query_on_target(sql)

    for d_row in dest_rows:
        d_row_status = 'Extra Row'
        for s_row in source_rows:
            if target_table == target_schema + '.ligands':
                if d_row[0] == s_row[0] and d_row[1] == s_row[1] and d_row[2] == s_row[2] and d_row[3] == s_row[3]:
                    d_row_status = 'Mismatch'
            elif target_table != target_schema + '.ligands':
                if d_row[0] == s_row[0]:
                    d_row_status = 'Mismatch'

        if d_row_status == 'Extra Row':
            sql = "insert into dnatag_wip.invalid_records (table_name, lower_bound, upper_bound, " \
                  "source_row, dest_row, status) values('" + target_table + "'," + lower_bound + "," + upper_bound \
                  + ", NULL ,'" + format_row(d_row, column_names_list) + "','" + d_row_status + "')"
            execute_query_on_target(sql)


def insert_summary_records(target_table, lower_bound, upper_bound, count, status):
    sql = "insert into dnatag_wip.summary_records (table_name, lower_bound, upper_bound, count," \
          "status) values('" + target_table + "'," + lower_bound + "," + upper_bound + "," + count \
          + ",'" + status + "')"
    execute_query_on_target(sql)


def validate_data(target_table, source_df, temp_table, where_clause, lower_bound, upper_bound):
    sql = 'select *  FROM {} EXCEPT select *  FROM {} {}'.format(temp_table, target_table, where_clause)
    result_df = query_redshift(sql)
    print(source_df.count())

    if result_df.empty:
        insert_summary_records(target_table, lower_bound, upper_bound, str(int(source_df.count())), 'Success')
        return True
    else:
        insert_summary_records(target_table, lower_bound, upper_bound, str(int(source_df.count())), 'Failed')
        insert_invalid_rows(result_df, temp_table, target_table, lower_bound, upper_bound, where_clause)
        return False


def check_if_records_table_exist(temp_schema):
    sql = "select * from information_schema.tables where table_schema = '" + temp_schema + "' and table_name in (" \
                                                                                           "'invalid_records', " \
                                                                                           "'summary_records') "
    result = query_redshift(sql)

    if len(result) > 0:
        return True
    else:
        return False


def create_records_table(temp_schema):
    matched_record_sql = "CREATE TABLE " + temp_schema + ".summary_records ( \
    table_name varchar NULL, \
    lower_bound numeric NULL, \
    upper_bound numeric NULL, \
    count numeric(38) NULL, \
    status varchar NULL, \"timestamp\" timestamp NULL DEFAULT CURRENT_TIMESTAMP \
    );"

    invalid_record_sql = "CREATE TABLE " + temp_schema + ".invalid_records ( \
    table_name varchar NULL, \
    lower_bound numeric NULL, \
    upper_bound numeric NULL, \
    source_row varchar NULL, \
    dest_row varchar NULL, \
    status varchar NULL, \"timestamp\" timestamp NULL DEFAULT CURRENT_TIMESTAMP \
    );"

    execute_query_on_target(matched_record_sql)
    execute_query_on_target(invalid_record_sql)


def get_spark_session(app_name='spark_app'):
    #     .set('spark.executor.memory', '4g') \
    #     .set('spark.driver.cores', '4') \
    #     .set('spark.driver.memory', '6g') \
    conf = SparkConf() \
        .set('spark.scheduler.mode', 'FAIR') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.shuffle.service.enabled', 'true') \
        .set('spark.scheduler.pool', 'production') \
        .set('spark.dynamicAllocation.executorIdleTimeout', '300')
    return SparkSession.builder.appName(app_name) \
        .config(conf=conf).getOrCreate()


# .set('spark.scheduler.allocation.file', '//usr/lib/spark/conf/fairscheduler.xml.template')

def test_data_batches(batch, source_table, target_table, column, numPartition, temp_schema, custom_dtypes):
    print(batch)
    spark = get_spark_session()
    test_status = True
    batch = batch.split(',')
    lower_bound = batch[0]
    upper_bound = batch[1]
    predicate = generate_predicates_for_range(source_table, column, lower_bound, upper_bound, numPartition)
    source_df = read_data_from_source_table2(source_table, predicate, custom_dtypes, spark)
    #print(source_df.count())
    # source_df.printSchema()
    ## temp_table = target_table + '_temp' + lower_bound + '_' + upper_bound
    target_table_name = target_table.split('.')
    temp_table = temp_schema + '.' + target_table_name[1] + '_temp' + lower_bound + '_' + upper_bound

    write_data_to_target(source_df, temp_table, numPartition)
    where_clause = generate_where_clause(column, lower_bound, upper_bound)

    container_result = validate_data(target_table, source_df, temp_table, where_clause, lower_bound, upper_bound)
    delete_temp_table(temp_table)

    if not container_result:
        test_status = False

    if test_status == True:
        print('test passed')
    assert test_status == True, 'Different Data'


if __name__ == "__main__":
    #config_file = open('/home/hadoop/scripts/config.json')
    print(CONFIG_JSON)
    print('hellow')
    config_file = open(CONFIG_JSON)
    #config_file = open('./config1.json')
    config_file = json.load(config_file)
    tables_list = config_file['data']
    temp_schema = tables_list[0]['target_table']
    table_and_schema = temp_schema.split('.')
    # temp_schema = table_and_schema[0]
    temp_schema = 'dnatag_wip'

    records_table_exist = check_if_records_table_exist(temp_schema)
    if records_table_exist is False:
        create_records_table(temp_schema)

    for table in tables_list:
        source_table = table['source_table']
        print(source_table)
        target_table = table['target_table']
        column = table['column']
        lower_bound = table['lower_bound']
        upper_bound = table['upper_bound']
        spark_jobs = table['spark_job']
        numPartition = table['numPartition']
        custom_dtypes = table['custom_dtypes']
        batches = generate_batches_for_range(column, lower_bound, upper_bound, spark_jobs)
        print(batches)
        threads = []
        for batch in batches:
            the = threading.Thread(target=test_data_batches,
                                   args=(batch, source_table, target_table, column, numPartition, temp_schema, custom_dtypes,))
            the.start()
            threads.append(the)
        # x = threading.Thread(target=thread_function, args=(1,))

        for th in threads:
            th.join()
