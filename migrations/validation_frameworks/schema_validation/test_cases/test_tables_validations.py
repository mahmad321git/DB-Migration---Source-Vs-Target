import sys

import allure

from managers.column_manager import ColumnValidationManager
from managers.source_manager import get_data_types_from_source, load_mapping_file
from dependency_injection.column_validation_dependency_injection import get_schemas_list

sys.path.append("..")


@allure.epic('Schema Validation')
@allure.feature('Tables Validation')
@allure.story('Physical Existance Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Tables Validation')
def test_table_physical_existance_validation(table_physical_existance, target_schemas, target_dbname, target_host, target_port,
                                             target_username, target_password):
    allure.dynamic.title('Table Physical Existance Validation')
    container_result = ColumnValidationManager().table_physical_existance_validation(
        table_physical_existance['table_name'], get_schemas_list(target_schemas), target_dbname, target_host,
        target_port, target_username, target_password)

    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False


@allure.epic('Schema Validation')
@allure.feature('Tables Validation')
@allure.story('Column Names Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Tables Validation')
def test_column_names_validation(column_names_match, target_schemas, target_dbname, target_host, target_port, target_username,
                                 target_password):
    allure.dynamic.title('Column Name Validation')
    container_result = ColumnValidationManager().column_names_match_validation(column_names_match['table_name'],
                                                                               column_names_match['column_name'],
                                                                               get_schemas_list(target_schemas),
                                                                               target_dbname, target_host, target_port,
                                                                               target_username, target_password)
    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False


@allure.epic('Schema Validation')
@allure.feature('Tables Validation')
@allure.story('Data Types Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Tables Validation')
def test_column_data_types_validation(get_tables_list, source_schemas, target_schemas, source_host, source_port, source_username, source_password,
                                      source_sid, target_dbname, target_host, target_port, target_username,
                                      target_password):
    source_cols_data_types = get_data_types_from_source(get_tables_list['table_name'], get_schemas_list(source_schemas),
                                                        source_host, source_port, source_username, source_password,
                                                        source_sid)
    target_cols_data_types = ColumnValidationManager().get_data_types_from_target(get_tables_list['table_name'],
                                                                                  get_schemas_list(target_schemas),
                                                                                  target_dbname, target_host,
                                                                                  target_port, target_username,
                                                                                  target_password)

    allure.dynamic.title('Column Data Type Validation')
    if len(target_cols_data_types) == 0:
        allure.dynamic.description('Table "{}" not found on target side'.format(get_tables_list['table_name']))
        assert True == False
    else:
        for source_item in source_cols_data_types:
            column_name = source_item[0]
            source_data_type = source_item[1]
            indices = [index for index, item in enumerate(target_cols_data_types) if column_name in item]
            if len(indices) > 0:
                target_item = target_cols_data_types[indices[0]]
                target_data_type = target_item[1]
                json_mappings = load_mapping_file('mapping.json')
                expected_target_data_types = json_mappings[source_data_type]

                if target_data_type not in expected_target_data_types:
                    allure.dynamic.description('Data types mismatched for column "{}"; source, target and expected data types are "{}", "{}", "{}" respectively'.format(column_name, source_data_type, target_data_type, expected_target_data_types))
                    assert True == False
                else:
                    allure.dynamic.description('Data types matched for column "{}"; source, target and expected data types are "{}", "{}", "{}" respectively'.format(column_name, source_data_type, target_data_type, expected_target_data_types))

            else:
                allure.dynamic.description(
                    'Column "{}", not found in table "{}" on target side'.format(column_name, get_tables_list['table_name']))
                assert True == False
