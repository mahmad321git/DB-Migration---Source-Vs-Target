import sys

import allure

from managers.column_manager import ColumnValidationManager
from dependency_injection.column_validation_dependency_injection import get_schemas_list

sys.path.append("..")


def convert_list_to_string(columns):
    for column in columns:
        columns[columns.index(column)] = ''.join(column)
    columns = ', '.join(columns)
    return columns


def convert_list_of_tuples_to_list_of_strings(data_types):
    for item in data_types:
        data_types[data_types.index(item)] = ''.join(item)
    return data_types


def convert_tuple_to_list_of_tuples(max_row):
    list_of_tuples = []
    i = 0
    while i < len(max_row):
        value = max_row[i]
        if i == 0:
            list_of_tuples.append(str(value))
        else:
            list_of_tuples.append(value)
        i = i + 1
    return list_of_tuples


def prepare_values(max_row, datatypes, columns, column_name, next_value, flag="not null"):
    values = ""
    values_list = []
    varchar = ["character varying", "varchar", "character", "char", "text"]
    number = ["numeric", "smallint", "bigint", "double precision", "integer", "decimal", "real", "serial",
              "smallserial", "bigserial"]
    date_time = ["timestamp without time zone", "timestamp with time zone", "date", "time without time zone",
                 "time with time zone"]
    i = 1
    for (value, dtype, column) in zip(max_row, datatypes, columns):
        if column == column_name and flag == "null":
            values = values + "null"
            values_list.append("null")
        elif column == column_name and flag == "not null":
            values = values + str(next_value)
            values_list.append(str(next_value))
        else:
            if dtype in number:
                values = values + "{}".format(value)
                values_list.append(value)
            elif dtype in varchar:
                values = values + "'{}'".format(value)
                values_list.append('' + value)

        if i < len(columns):
            values = values + ", "
            i = i + 1

    return values, values_list


@allure.epic('Schema Validation')
@allure.feature('Triggers Validation')
@allure.suite('Schema Validation')
@allure.sub_suite('Triggers Validation')
# def test_triggers_validation(triggers_list, target_dbname, target_schemas, target_host, target_port, target_username, target_password):
def triggers_validation(triggers_list, target_dbname, target_schemas, target_host, target_port, target_username, target_password):
    allure.dynamic.title('Triggers Functionality Validation')
    trigger_table = triggers_list[0]
    temp_table = trigger_table + "_temp"
    trigger_name = triggers_list[1]
    trigger_action = triggers_list[2]
    trigger_schema = triggers_list[3]
    metadata = ColumnValidationManager().load_json_file_containing_triggers_mappings(trigger_table, 'triggers.json')
    # dest_schema = metadata[2]
    dest_schema = target_schemas.lower()
    trigger_action = trigger_action.replace(trigger_schema, dest_schema)

    ColumnValidationManager().create_temp_table_at_target(dest_schema, temp_table, trigger_schema, trigger_table,
                                                          target_dbname, target_host, target_port, target_username,
                                                          target_password)
    ColumnValidationManager().create_trigger_at_target(trigger_name, dest_schema, temp_table, trigger_action,
                                                       target_dbname, target_host, target_port, target_username,
                                                       target_password)

    columns_list = ColumnValidationManager().get_table_columns_from_target(get_schemas_list(target_schemas),
                                                                           trigger_table, target_dbname,
                                                                           target_host, target_port,
                                                                           target_username, target_password)
    columns = convert_list_to_string(columns_list)
    columns_list = convert_list_of_tuples_to_list_of_strings(columns_list)
    data_types = ColumnValidationManager().get_data_types_of_columns_from_target(get_schemas_list(target_schemas),
                                                                                 trigger_table, target_dbname,
                                                                                 target_host, target_port,
                                                                                 target_username, target_password)
    data_types_list = convert_list_of_tuples_to_list_of_strings(data_types)

    column_name = ColumnValidationManager().load_json_file_containing_triggers_mappings(trigger_table,
                                                                                        'triggers.json')
    sequence_name = column_name[1]

    count = 1
    while count <= 2:
        min_row = ColumnValidationManager().load_json_file_containing_triggers_mappings(trigger_table,
                                                                                        'sample_data.json')

        sql = 'insert into ' + dest_schema + '.' + temp_table + ' (' + columns + ' ) values('
        data_list = convert_tuple_to_list_of_tuples(eval(min_row))

        if count % 2 == 0:
            next_value = ColumnValidationManager().get_sequence_last_value(sequence_name, dest_schema,
                                                                           target_dbname, target_host, target_port,
                                                                           target_username, target_password) + 1
            values, generated_values_list = prepare_values(data_list, data_types_list, columns_list,
                                                           column_name[0], next_value, "null")
        else:
            next_value = ColumnValidationManager().get_sequence_next_value(sequence_name, dest_schema,
                                                                           target_dbname, target_host, target_port,
                                                                           target_username, target_password)
            values, generated_values_list = prepare_values(data_list, data_types_list, columns_list,
                                                           column_name[0], next_value)

        insert_statement = sql + values + ')'
        ColumnValidationManager().insert_new_row_in_table(insert_statement, target_dbname, target_host, target_port,
                                                          target_username, target_password)
        inserted_values_list = ColumnValidationManager().get_max_row_from_target(dest_schema, temp_table,
                                                                                 column_name[0], target_dbname,
                                                                                 target_host, target_port,
                                                                                 target_username, target_password)
        inserted_values_list = convert_tuple_to_list_of_tuples(inserted_values_list[0])
        max_value = int(inserted_values_list.pop(0))

        if next_value == max_value:
            print('Passed')
            print(next_value)
            print(max_value)
            print(insert_statement)
            print(generated_values_list)
            print(inserted_values_list)
            allure.dynamic.description(
                'Sequence last value is equal to column max value; sequence last_value = {}, column max_value = {}'.format(
                    next_value, max_value))
        else:
            allure.dynamic.description(
                'Sequence last value is not equal to column max value; sequence last_value = {}, column max_value = {}'.format(
                    next_value, max_value))
            ColumnValidationManager().drop_table_at_target(dest_schema, temp_table, target_dbname, target_host,
                                                           target_port, target_username, target_password)
            assert True == False

        count = count + 1
        if count == 3:
            ColumnValidationManager().drop_table_at_target(dest_schema, temp_table, target_dbname, target_host, target_port, target_username, target_password)
#
#
# @allure.epic('Schema Validation')
# @allure.feature('Triggers Validations')
# @allure.story('Triggers Physical Existance Validation')
# @allure.suite('Schema Validation')
# @allure.sub_suite('Triggers Physical Existance Validation')
# def test_triggers_physical_existance_validation(triggers_physical_existance, target_dbname, target_schemas, target_host, target_port, target_username, target_password):
#     allure.dynamic.title('Triggers Physical Existance Validation')
#     source_trigger_name = triggers_physical_existance[0]
#     source_trigger_table = triggers_physical_existance[1]
#     target_triggers_list = ColumnValidationManager().get_triggers_from_target(get_schemas_list(target_schemas), target_dbname, target_host, target_port, target_username, target_password)
#     global trigger_exist
#     trigger_exist = False
#     for trigger in target_triggers_list:
#         target_trigger_table = trigger[0].upper()
#         target_trigger_name = trigger[1].upper()
#         if source_trigger_name == target_trigger_name and source_trigger_table == target_trigger_table:
#             allure.dynamic.description('Trigger found on target; source trigger name'
#                                        ': {}, source trigger table: {}, target trigger name'
#                                        ':{}, target trigger table:{}'.format(source_trigger_name
#                                                                              ,source_trigger_table,
#                                                                              target_trigger_name,
#                                                                              target_trigger_table))
#             trigger_exist = True
#             break
#     if not trigger_exist:
#         allure.dynamic.description('Trigger not found on target; source trigger name'
#                                    ': {}, source trigger table: {}'.format(source_trigger_name,
#                                                                            source_trigger_table))
#         assert True == False