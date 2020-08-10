import sys

import allure

from managers.column_manager import ColumnValidationManager
from dependency_injection.column_validation_dependency_injection import get_schemas_list
from managers.source_manager import get_source_views_data

sys.path.append("..")


# @allure.epic('Schema Validation')
# @allure.feature('Views Validation')
# # @allure.story('Physical Existance Validation')
# @allure.suite('Schema Validation')
# @allure.sub_suite('Views Validation')
# def test_views_physical_existance_validation(view_physical_existance, target_schemas, target_dbname, target_host, target_port, target_username, target_password):
#     allure.dynamic.title('View Physical Existance Validation')
#     allure.dynamic.story('Physical Existance Validation : {}'.format(str(get_schemas_list(target_schemas))))
#     container_result = ColumnValidationManager().view_physical_existance_validation(view_physical_existance['view_name'], get_schemas_list(target_schemas), target_dbname, target_host, target_port, target_username, target_password)
#     allure.dynamic.description(container_result[1])
#     if not container_result[0]:
#         allure.dynamic.description('View "{}" not found on target side'.format(view_physical_existance['view_name']))
#         assert True == False


@allure.epic('Schema Validation')
@allure.feature('Views Validation')
@allure.story('Views Data Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Views Validation')
def test_views_data_validation(view_physical_existance, target_schemas, target_dbname, target_host, target_port, target_username, target_password, source_schemas, source_host, source_port, source_username, source_password, source_sid, sample_rows):
    allure.dynamic.title('View Data Validation')
    source_view_data = get_source_views_data(view_physical_existance['view_name'].lower(), source_host, source_port, source_sid, source_schemas, source_username, source_password, sample_rows)
    target_view_data = ColumnValidationManager().get_target_views_data(view_physical_existance['view_name'].lower(), target_host, target_port, target_dbname, target_schemas, target_username, target_password, sample_rows)
    print(view_physical_existance['view_name'])
    print(source_view_data)
    print(target_view_data)
    # assert 1==11
    for item in source_view_data:
        if item in target_view_data:
            allure.dynamic.description('Source and Target data matched for View: {}'.format(view_physical_existance['view_name']))
        else:
            allure.dynamic.description('Source and Target data mismatched for View: {}'.format(view_physical_existance['view_name']))
            assert True == False