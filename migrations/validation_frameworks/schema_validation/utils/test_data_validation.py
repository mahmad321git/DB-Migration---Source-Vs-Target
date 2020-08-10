import sys
import threading
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from managers.sch_evo_architecture_base_manager.data_integrity_manager.data_integrity_manager_impl.data_validation_manager \
    import DataValidationManager
from helpers.pyspark_helper import SparkHelper

sys.path.append("..")


def test_data_validation(data_validation):
    test_status = True
    source_table = data_validation['source_table']
    target_table = data_validation['target_table']
    column = data_validation['column']
    lower_bound = data_validation['lower_bound']
    upper_bound = data_validation['upper_bound']
    numPartitions = data_validation['numPartitions']

    batches = SparkHelper().generate_batches_for_range(column, lower_bound, upper_bound, numPartitions)
    print(batches)
    threads = []
    for batch in batches:
        the = threading.Thread(target=spark_job, args=(batch, source_table, target_table, column,))
        the.start()
        threads.append(the)
    # x = threading.Thread(target=thread_function, args=(1,))

    for th in threads:
        th.join()


def spark_job(batch, source_table, target_table, column):
    print(batch)
    spark = get_spark_session()
    test_status = True
    source_table = 'DNATAG_QA.RESULTS1'
    target_table = 'dnatag_qa.results1'
    column = 'RESULT_ID'
    batch = batch.split(',')
    lower_bound = batch[0]
    upper_bound = batch[1]

    predicate = SparkHelper().generate_predicates_for_range(source_table, column, lower_bound, upper_bound, 4)
    source_df = SparkHelper().read_data_from_source_table2(source_table, predicate, spark)
    # temp_table = source_table + '_temp'
    temp_table = target_table + '_temp' + lower_bound + '_' + upper_bound
    SparkHelper().write_data_to_target2(source_df, temp_table)
    where_clause = SparkHelper().generate_where_clause(column, lower_bound, upper_bound)
    # print(where_clause)
    container_result = DataValidationManager().validate_data(target_table, temp_table, where_clause)

    if not container_result:
        test_status = False

    if test_status == True:
        print('test passed')
    assert test_status == True, 'Different Data'


def get_spark_session(app_name='spark_app'):
    conf = SparkConf() \
        .set('spark.scheduler.mode', 'FAIR') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.shuffle.service.enabled', 'true') \
        .set('spark.scheduler.pool', 'production') \
        .set('spark.scheduler.allocation.file',
             'C:/spark/conf/fairscheduler.xml.template')
    return SparkSession.builder.appName(app_name) \
        .config(conf=conf).getOrCreate()

#     .set('spark.executor.memory', '4g') \
#     .set('spark.driver.cores', '4') \
#     .set('spark.driver.memory', '6g')


