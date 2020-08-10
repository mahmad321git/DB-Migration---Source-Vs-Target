import json

import psycopg2
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import findspark
import threading

findspark.init()


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


def read_data_from_source_table2(table, source_predicates, spark):
    df = spark.read \
        .option('fetchsize', 1000) \
        .jdbc(url="jdbc:oracle:thin:dnatag_qa/dnatag_qa@52.44.118.185:1521/ORA12C", table=table,
              predicates=source_predicates)
    return df


# .option('driver', "oracle.jdbc.OracleDriver") \

def write_data_to_target(source_df, temp_table, numPartition):
    source_df.write.option("numPartitions", numPartition) \
        .jdbc(url="jdbc:postgresql://nurix-aurora-instance.cgughnur3nr0.us-east-1.rds.amazonaws.com:5432/postgres",
              table=temp_table,
              mode='overwrite',
              properties={
                  'user': 'postgres',
                  'password': 'postgres',
              })


def generate_where_clause(column, lower_bound, upper_bound):
    where_clause = '''where {column} >= {lower} AND {column} <= {upper}'''
    where_clause = where_clause.format(column=column, lower=lower_bound, upper=upper_bound)
    return where_clause


def query_redshift(query):
    con = psycopg2.connect(dbname='postgres',
                           host='nurix-aurora-instance.cgughnur3nr0.us-east-1.rds.amazonaws.com',
                           port='5432',
                           user='postgres',
                           password='postgres')
    cur = con.cursor()
    cur.execute(query)
    result = cur.fetchall()
    return result


def delete_temp_table(temp_table):
    query = 'DROP TABLE {}'.format(temp_table)
    con = psycopg2.connect(dbname='postgres',
                           host='nurix-aurora-instance.cgughnur3nr0.us-east-1.rds.amazonaws.com',
                           port='5432',
                           user='postgres',
                           password='postgres')
    cur = con.cursor()
    cur.execute(query)


def validate_data(target_table, temp_table, where_clause):
    sql = 'select *  FROM {} EXCEPT select *  FROM {} {}'.format(temp_table, target_table, where_clause)
    print(sql)
    result = query_redshift(sql)
    print(result)
    if len(result) > 0:
        return False
    else:
        return True


def get_spark_session(app_name='spark_app'):
    #     .set('spark.executor.memory', '4g') \
    #     .set('spark.driver.cores', '4') \
    #     .set('spark.driver.memory', '6g') \
    conf = SparkConf() \
        .set('spark.scheduler.mode', 'FAIR') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.shuffle.service.enabled', 'true') \
        .set('spark.scheduler.pool', 'production') \
        .set('spark.scheduler.allocation.file', '//usr/lib/spark/conf/fairscheduler.xml.template')
    return SparkSession.builder.appName(app_name) \
        .config(conf=conf).getOrCreate()


# .set('spark.scheduler.allocation.file', '//usr/lib/spark/conf/fairscheduler.xml.template')

def test_data_batches(batch, source_table, target_table, column, numPartition):
    print(batch)
    spark = get_spark_session()
    test_status = True
    batch = batch.split(',')
    lower_bound = batch[0]
    upper_bound = batch[1]
    predicate = generate_predicates_for_range(source_table, column, lower_bound, upper_bound, numPartition)
    # print(predicate)
    source_df = read_data_from_source_table2(source_table, predicate, spark)
    # temp_table = source_table + '_temp'
    temp_table = target_table + '_temp' + lower_bound + '_' + upper_bound
    write_data_to_target(source_df, temp_table, numPartition)
    where_clause = generate_where_clause(column, lower_bound, upper_bound)
    # print(where_clause)
    container_result = validate_data(target_table, temp_table, where_clause)
    # delete_temp_table(temp_table)

    if not container_result:
        test_status = False

    if test_status == True:
        print('test passed')
    assert test_status == True, 'Different Data'


if __name__ == "__main__":
    config_file = open('/home/hadoop/data_validation/config.json')
    config_file = json.load(config_file)
    tables_list = config_file['data']

    for table in tables_list:
        source_table = table['source_table']
        target_table = table['target_table']
        column = table['column']
        lower_bound = table['lower_bound']
        upper_bound = table['upper_bound']
        spark_jobs = table['spark_job']
        numPartition = table['numPartition']
        batches = generate_batches_for_range(column, lower_bound, upper_bound, spark_jobs)
        print(batches)
        # list_spark = ['session1','session2', 'session3']
        threads = []
        for batch in batches:
            the = threading.Thread(target=test_data_batches,
                                   args=(batch, source_table, target_table, column, numPartition,))
            the.start()
            threads.append(the)
        # x = threading.Thread(target=thread_function, args=(1,))

        for th in threads:
            th.join()
