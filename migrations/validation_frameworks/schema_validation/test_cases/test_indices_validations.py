import sys

import allure

from managers.column_manager import ColumnValidationManager
from dependency_injection.column_validation_dependency_injection import get_schemas_list

sys.path.append("..")


def trim_index_name(index_name):
    index = len(index_name) - index_name.rfind('_')
    index_name = index_name[:-index]
    return index_name


@allure.epic('Schema Validation')
@allure.feature('Indexes Validation')
@allure.story('Indexes')
@allure.suite('Schema Validation')
@allure.sub_suite('Indexes Validation')
def test_indexes_validation(index, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
    global target_column_name, target_index_name
    index_found = False
    schema_name = index[3]
    target_indexes_list = ColumnValidationManager().get_indexes_from_target(get_schemas_list(target_schemas), target_dbname, target_host,
                                                                            target_port, target_username,
                                                                            target_password)
    source_index_name = trim_index_name(index[1])
    source_column_name = index[2]
    allure.dynamic.title('Index Column and Index Name Validation')

    for target_item in target_indexes_list:
        target_index_name = trim_index_name(target_item[1])
        target_column_name = target_item[2]
        if source_column_name in target_column_name and source_index_name == target_index_name:
            index_found = True
            print('Passed : {} \t {} \t {}'.format(source_column_name, source_index_name, target_column_name))

    if not index_found:
        allure.dynamic.description('Index not found for source index "{}",source table : {}'.format(source_index_name, index[0]))
        assert True == False
    else:
        allure.dynamic.description('Index name and index column name matched; '
                                   'source index column is "{}", source index name is "{}"'
                                   'target index column is "{}", target index name is "{}"'.format(source_column_name, source_index_name, target_column_name, target_index_name))


@allure.epic('Schema Validation')
@allure.feature('Indexes Validation')
# @allure.story('Foreign Key Indexes')
@allure.story('Entries in the Failed column means there is a missing FK on both source and destination. We recommend creating an index on every FK to prevent locking')
@allure.suite('Schema Validation')
@allure.sub_suite('Indexes Validation')
def test_all_foreign_keys_have_index_validation(foreign_key, target_schemas, target_dbname, target_host, target_port, target_username,
                                                target_password):
    allure.dynamic.title("Every Foreign Key has Index Validation")
    result = ColumnValidationManager().get_index_against_foreign_key_column(foreign_key['column_name'],
                                                                            get_schemas_list(target_schemas), target_dbname,
                                                                            target_host, target_port, target_username,
                                                                            target_password)
    if not result:
        allure.dynamic.description('Index not found against foreign key column: "{}", table: "{}"'.format(foreign_key['column_name'], foreign_key['table_name']))
        # allure.dynamic.story("Entries in the Failed column means there is a missing FK on both source and destination. We recommend creating an index on every FK to prevent locking")
        assert True == False
    else:
        allure.dynamic.description('Index found against foreign key column "{}"'.format(foreign_key['column_name']))

