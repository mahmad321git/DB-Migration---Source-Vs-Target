from concurrent.futures._base import LOGGER

from helpers.redshift_helper import redshift_helper
import findspark

findspark.init()
import pyspark  # only run after findspark.init()
from pyspark.sql import SparkSession


class SparkHelper:
    def __init__(self):
        self.redshift_helper = redshift_helper()
        self.min_val = 0
        self.max_val = 0

    def generate_where_clause(self, column, lower_bound, upper_bound):
        return 'where ' + column + '>=' + lower_bound + 'and' + column + '<=' + upper_bound

    def generate_numeric_ranges(self, lower_bound, upper_bound, num_partitions):
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

    def generate_batches_from_range(self, lower_bound, upper_bound, num_partitions=1):
        return self.generate_numeric_ranges(lower_bound, upper_bound, num_partitions)

    def generate_predicates_for_range(self, column, lower_bound, upper_bound, num_partitions=1):
        predicates = []
        alias = ''
        where_clause = '''{alias_placeholder}{column} IS NOT NULL AND {alias_placeholder}{column} >= {min_val} AND {alias_placeholder}{column} <= {max_val}'''.strip()
        for min_val, max_val in self.generate_batches_from_range(lower_bound, upper_bound, num_partitions):
            predicate = where_clause.format(alias_placeholder=alias, column=column, min_val=min_val, max_val=max_val)
            predicates.append(predicate)

        return predicates

    def generate_batches_for_range(self, column, lower_bound, upper_bound, num_partitions=1):
        batches = []
        for min_val, max_val in self.generate_batches_from_range(lower_bound, upper_bound, num_partitions):
            batch = '''{min},{max}'''
            batch = batch.format(min=min_val, max=max_val).strip()
            batches.append(batch)
        return batches

    def write_data_to_target(self, source_df, temp_table):
        source_df.write.option('batchsize', 2000) \
            .option("numPartitions", 4) \
            .jdbc(url="jdbc:mysql://localhost:3306/sakila",
                  table=temp_table,
                  mode='overwrite',
                  properties={
                      'user': 'root',
                      'password': '1234',
                  })

    def read_data_from_source_table(self, data_dict):
        spark = SparkSession.builder.getOrCreate()
        table = data_dict['table']
        column = data_dict['column']
        lower_bound = data_dict['lower_bound']
        upper_bound = data_dict['upper_bound']
        numPartitions = data_dict['numPartitions']
        source_predicates = self.generate_predicates_for_range(column, lower_bound, upper_bound, numPartitions)
        df = spark.read \
            .option('fetchsize', 2000) \
            .jdbc(url="jdbc:mysql://localhost:3306/sakila",
                  table=table,
                  predicates=source_predicates,
                  properties={
                      'user': 'root',
                      'password': '1234',
                  })
        return df

    def read_data_from_source_table2(self, table, source_predicates):
        spark = SparkSession.builder.getOrCreate()
        df = spark.read \
            .option('fetchsize', 2000) \
            .jdbc(url="jdbc:mysql://localhost:3306/sakila",
                  table=table,
                  predicates=source_predicates,
                  properties={
                      'user': 'root',
                      'password': '1234',
                  })
        return df

    def write_data_to_target2(self, source_df, temp_table):
        source_df.write.option("numPartitions", 4) \
            .jdbc(url="jdbc:mysql://localhost:3306/sakila",
                  table=temp_table,
                  mode='append',
                  properties={
                      'user': 'root',
                      'password': '1234',
                  })

    def my_predicates(self, column, lower, upper, numPartitions):
        predicates = []
        batches = []
        alias = ''
        diff = int(upper / numPartitions)
        where_clause = '''{alias_placeholder}{column} IS NOT NULL AND {alias_placeholder}{column} >= {min_val} AND {alias_placeholder}{column} <= {max_val}'''.strip()
        i = 1
        self.min_val = lower
        self.max_val = diff
        while i <= numPartitions:
            predicate = where_clause.format(alias_placeholder=alias, column=column, min_val=self.min_val,
                                            max_val=self.max_val)
            batch = '''{min},{max}'''
            batch = batch.format(min=self.min_val, max=self.max_val).strip()
            batches.append(batch)
            predicates.append(predicate)

            i = i + 1
            self.min_val = self.max_val + 1
            if i < numPartitions:
                self.max_val = diff * i
            else:
                self.max_val = upper
        return predicates, batches
