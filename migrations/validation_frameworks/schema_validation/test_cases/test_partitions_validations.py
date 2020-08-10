import sys

import allure

from managers.column_manager import ColumnValidationManager
from managers.source_manager import get_partitons_from_source, get_tables_that_has_partitions
from managers.source_manager import get_row_count_of_subpartition_from_source
from dependency_injection.column_validation_dependency_injection import get_schemas_list

sys.path.append("..")


@allure.epic('Schema Validation')
@allure.feature('Partitions Validation')
@allure.story('Partitons/Subpartition Name & Row Count Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Partitions Validation')
def test_partition_validation(source_partitions_list, source_schemas, source_host, source_port, source_username,
                              source_password, source_sid, target_dbname, target_host, target_port, target_username,
                              target_password, target_schemas):
    allure.dynamic.title('Partitions / Subpartitions / Row_Count Validation')

    table_name = source_partitions_list[0]
    target_partitions_list = ColumnValidationManager().get_partitons_from_target(table_name, target_dbname, target_host,
                                                                                 target_port, target_username,
                                                                                 target_password)

    if not source_partitions_list in target_partitions_list:
        allure.dynamic.description(
            'Partition / Subpartition is missing, source item: {}'.format(source_partitions_list))
        assert True == False
    else:
        subpartition = source_partitions_list[2]
        subpartition = subpartition.split("_")
        sel_id = subpartition[1]
        dl_libid = subpartition[2]
        source_partitions_row_count = get_row_count_of_subpartition_from_source(source_partitions_list[0].upper(),
                                                                                sel_id, dl_libid,
                                                                                source_schemas,
                                                                                source_host,
                                                                                source_port,
                                                                                source_username,
                                                                                source_password,
                                                                                source_sid)
        target_partitions_row_count = ColumnValidationManager().get_row_count_of_subpartition_from_target(
            target_schemas.lower(),
            source_partitions_list[2],
            target_dbname,
            target_host,
            target_port,
            target_username,
            target_password)
        if source_partitions_row_count == target_partitions_row_count:
            allure.dynamic.description('Partition name, subpartition name, anc their row count matched')
        else:
            allure.dynamic.description('subpartition row count mismatched; source row_count = {}, target row_count = {} for item {}'.format(source_partitions_row_count, target_partitions_row_count, source_partitions_list))
            assert True == False


# @allure.epic('Schema Validation')
# @allure.feature('Partitions Validation')
# @allure.suite('Schema Validation')
# @allure.sub_suite('Partitions Validation')
# def test_partition_validation(source_schemas, source_host, source_port, source_username, source_password, source_sid, target_dbname, target_host, target_port, target_username, target_password):
#     allure.dynamic.title('Partitions / Subpartitions / Row_Count Validation')
#     partitioned_tables = get_tables_that_has_partitions(get_schemas_list(source_schemas), source_host, source_port, source_username, source_password,
#                                                         source_sid)
#     for table in partitioned_tables:
#         table_name = table[0]
#         schema_name = table[1]
#         source_partitions_list = get_partitons_from_source(table_name, get_schemas_list(source_schemas), source_host, source_port,
#                                                            source_username, source_password, source_sid)
#         target_partitions_list = ColumnValidationManager().get_partitons_from_target(table_name, target_dbname, target_host, target_port, target_username, target_password)
#         # print(source_partitions_list[0])
#         # print(source_partitions_list[0] in target_partitions_list)
#         # print(target_partitions_list)
#         # assert 1==2
#         for source_item in source_partitions_list:
#             partition_found = False
#             for target_item in target_partitions_list:
#                 if source_item[1] == target_item[1] and source_item[2] == target_item[2] and source_item[3] == \
#                         target_item[3]:
#                     partition_found = True
#                     break
#
#             if not partition_found:
#                 allure.dynamic.description('Partition name, subpartition name, or their row count mismatched, source item: {}'.format(source_item))
#                 # print(source_item)
#                 assert True == False
#             else:
#                 allure.dynamic.description('Partition name, subpartition name, anc their row count matched')


# """PRE REQUISITE (There must be an entry of selection_id in SELECTIONS table
# and dl_libid in LIBRARIES TABLE)"""
#
# sel_id = "2"
# dl_libid = "1"
#
#
# def test_addpartsubpart_validation(target_dbname, target_host, target_port, target_username, target_password):
#     sp_response = ColumnValidationManager().call_part_subpart(sel_id, dl_libid, target_dbname, target_host, target_port,
#                                                               target_username, target_password)
#     response = sp_response[0][0]
#
#     print('DB Response: ', response)
#     subpart_check = ColumnValidationManager().validate_subpartition_added(sel_id, dl_libid, target_dbname, target_host,
#                                                                           target_port, target_username, target_password)
#     if subpart_check == 1:
#         print("SUBPARTITION VALIDATED")
#     else:
#         print("SUBPARTITION NOT VALIDATED")
#
#     part_check = ColumnValidationManager().validate_partition_added(sel_id, target_dbname, target_host, target_port,
#                                                                     target_username, target_password)
#     if part_check >= 1:
#         print("PARTITION VALIDATED")
#     else:
#         print("PARTITION NOT VALIDATED")


# def test_delpart_validation(target_dbname, target_host, target_port, target_username, target_password):
#     sp_response = ColumnValidationManager().call_delpart(sel_id, target_dbname, target_host, target_port,
#                                                               target_username, target_password)
#     response = sp_response[0][0]
#     print('DB Response: ', response)
#
#     subpart_check = ColumnValidationManager().validate_partition_deletion(sel_id, target_dbname, target_host, target_port, target_username, target_password)
#     if subpart_check == 0:
#         print("PARTITION DELETION: VALIDATED")
#     else:
#         print("PARTITION DELETION: VALIDATION FAILED")


# def test_delpartsubpart_validation(target_dbname, target_host, target_port, target_username, target_password):
#     sp_response = ColumnValidationManager().call_part_subpart(sel_id, dl_libid, target_dbname, target_host, target_port,
#                                                               target_username, target_password)
#     response = sp_response[0][0]
#     print('DB Response: ', response)
#
#     subpart_check = ColumnValidationManager().validate_partition_subpartition_deletion(sel_id, dl_libid, target_dbname, target_host, target_port, target_username, target_password)
#     if subpart_check == 0:
#         print("SUBPARTITION DELETION: VALIDATED")
#     else:
#         print("SUBPARTITION DELETION: VALIDATION FAILED")
