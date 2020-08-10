import json
import sys

import allure

from managers.column_manager import ColumnValidationManager
from dependency_injection.column_validation_dependency_injection import get_schemas_list
from managers.source_manager import load_mapping_file

sys.path.append("..")


@allure.epic('Schema Validation')
@allure.feature('Sequences Validation')
@allure.story('Sequence Name, Start Value, Min Value, Increment Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Sequences Validation')
def test_sequences_validation(sequence, target_schemas, target_dbname, target_host, target_port, target_username,
                              target_password):
    allure.dynamic.title('Sequence Physical Existance Validation')
    source_tuple = sequence
    schema_name = source_tuple[3]
    target_sequences_list = ColumnValidationManager().get_sequences_from_target(get_schemas_list(target_schemas),
                                                                                target_dbname, target_host, target_port,
                                                                                target_username, target_password)
    sequence_found = False
    for target_tuple in target_sequences_list:
        if source_tuple[0] == target_tuple[0] and source_tuple[1] == target_tuple[1] and source_tuple[2] == \
                target_tuple[2] and source_tuple[3] == target_tuple[3]:
            sequence_found = True
    if not sequence_found:
        allure.dynamic.description('Sequence  "{}", not found on target'.format(source_tuple[0]))
        assert True == False
    else:
        allure.dynamic.description('Sequence  "{}" found on target'.format(source_tuple[0]))


@allure.epic('Schema Validation')
@allure.feature('Sequences Validation')
@allure.story('Sequence Next_Val & Associated Column Max_Val Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Sequences Validation')
def test_sequence_next_val_validation(target_sequences, target_schemas, target_dbname, target_host, target_port, target_username,
                                      target_password):
    allure.dynamic.title('Sequence Next_Val & Associated Column Max_Val Validation')
    sequence_mapping = load_mapping_file('sequence.json')
    sequence_name = target_sequences[0]
    sequence_schema = target_sequences[3]

    metadata = sequence_mapping[sequence_name]
    if len(metadata):
        table_name = metadata[0]
        column_name = metadata[1]
        increment_by = metadata[2]
        next_value = ColumnValidationManager().get_sequence_last_value(get_schemas_list(target_schemas),
                                                                       sequence_name, target_dbname, target_host,
                                                                       target_port, target_username,
                                                                       target_password) + increment_by
        if next_value == 1:
            next_value = ColumnValidationManager().get_sequence_start_val(get_schemas_list(target_schemas),
                                                                          sequence_name, target_dbname, target_host,
                                                                          target_port, target_username,
                                                                          target_password)

        max_value = ColumnValidationManager().get_max_val(sequence_schema, table_name, column_name, target_dbname,
                                                          target_host, target_port, target_username,
                                                          target_password)
        # print(next_value, max_value)
        if next_value > max_value:
            allure.dynamic.description(
                'Sequence "{}" Next_Val is greater than column "{}" Max_Val;  sequence Next_Val = {}, column Max_Val = {} on target side'.format(
                    sequence_name, column_name, next_value, max_value))
        else:
            allure.dynamic.description(
                'Sequence "{}" Next_Val is less than column "{}" Max_Val;  sequence Next_Val = {}, column Max_Val = {} on target side'.format(
                    sequence_name, column_name, next_value, max_value))
            assert True == False
