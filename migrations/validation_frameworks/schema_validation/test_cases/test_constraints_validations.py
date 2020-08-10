import sys

import allure

from managers.column_manager import ColumnValidationManager
from dependency_injection.column_validation_dependency_injection import get_schemas_list

sys.path.append("..")


@allure.epic('Schema Validation') # NN 19, 0 failed
@allure.feature('Constraints Validation')
@allure.story('Not Null Column Constraint')
@allure.suite('Schema Validation')
@allure.sub_suite('Constraints Validation')
def test_column_not_null_validation(not_null_column, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
    allure.dynamic.title('Not Null Constraint Validation')
    container_result = ColumnValidationManager().column_nullable_validation(get_schemas_list(target_schemas),
                                                                            not_null_column["table_name"],
                                                                            not_null_column["column_name"], target_dbname, target_host, target_port, target_username, target_password)
    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False


@allure.epic('Schema Validation') # PK 19, 0 failed
@allure.feature('Constraints Validation')
@allure.story('Primary Key Constraint')
@allure.suite('Schema Validation')
@allure.sub_suite('Constraints Validation')
def test_primary_key_validation(primary_key, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
    allure.dynamic.title('Primary Key Constraint Validation')
    container_result = ColumnValidationManager().primary_key_validation(get_schemas_list(target_schemas),
                                                                        primary_key['table_name'],
                                                                        primary_key['column_name'],
                                                                        primary_key['constraint_name'], target_dbname, target_host, target_port, target_username, target_password)
    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False


@allure.epic('Schema Validation') # 72, 6 failed
@allure.feature('Constraints Validation')
@allure.story('Check Constraint')
@allure.suite('Schema Validation')
@allure.sub_suite('Constraints Validation')
def test_check_constraints_validation(check_constraints, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
    allure.dynamic.title('Check Constraint Validation')
    container_result = ColumnValidationManager().check_constraint_validation(check_constraints['constraint_name'],
                                                                             check_constraints['search_condition'],
                                                                             get_schemas_list(target_schemas), target_dbname, target_host, target_port, target_username, target_password)
    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False


@allure.epic('Schema Validation') # UK 7, 0 failed
@allure.feature('Constraints Validation')
@allure.story('Unique Key Constraint')
@allure.suite('Schema Validation')
@allure.sub_suite('Constraints Validation')
def test_unique_key_validation(unique_key, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
    allure.dynamic.title('Unique Key Constraint Validation')
    container_result = ColumnValidationManager().unique_key_validation(get_schemas_list(target_schemas),
                                                                       unique_key['table_name'],
                                                                       unique_key['column_name'],
                                                                       unique_key['constraint_name'], target_dbname, target_host, target_port, target_username, target_password)
    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False


@allure.epic('Schema Validation') # FK 20, 1 failed
@allure.feature('Constraints Validation')
@allure.story('Foreign Key Constraint')
@allure.suite('Schema Validation')
@allure.sub_suite('Constraints Validation')
def test_foreign_key_validation(foreign_key, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
    allure.dynamic.title('Foreign Key Constraint Validation')
    container_result = ColumnValidationManager().foreign_key_validation(get_schemas_list(target_schemas),
                                                                        foreign_key['table_name'],
                                                                        foreign_key['column_name'],
                                                                        foreign_key['constraint_name'], target_dbname, target_host, target_port, target_username, target_password)
    allure.dynamic.description(container_result[1])
    if not container_result[0]:
        assert True == False