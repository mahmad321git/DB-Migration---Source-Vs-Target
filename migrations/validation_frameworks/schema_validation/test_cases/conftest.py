import pytest

from dependency_injection import column_validation_dependency_injection as dp
from dependency_injection.column_validation_dependency_injection import get_schemas_list
from managers.column_manager import ColumnValidationManager


def pytest_addoption(parser):
    parser.addoption("--not_null_column", action="store", default=[])
    parser.addoption("--primary_key", action="store", default=[])
    parser.addoption("--unique_key", action="store", default=[])
    parser.addoption("--foreign_key", action="store", default=[])
    parser.addoption("--table_physical_existance", action ="store", default=[])
    parser.addoption("--view_physical_existance", action ="store", default=[])
    parser.addoption("--column_names_match", action ="store", default=[])
    parser.addoption("--data_validation", action ="store", default=[])
    parser.addoption("--get_tables_list", action ="store", default=[])
    parser.addoption("--check_constraints", action ="store", default=[])
    parser.addoption("--index", action ="store", default=[])
    parser.addoption("--sequence", action ="store", default=[])
    parser.addoption("--triggers_list", action ="store", default=[])
    parser.addoption("--triggers_physical_existance", action ="store", default=[])
    parser.addoption("--source_partitions_list", action="store", default=[])
    parser.addoption("--target_sequences", action="store", default=[])

    parser.addoption("--source_host", action="store", default="")
    parser.addoption("--source_port", action="store", default="")
    parser.addoption("--source_username", action="store", default="")
    parser.addoption("--source_password", action="store", default="")
    parser.addoption("--source_sid", action="store", default="")
    parser.addoption("--source_schemas", action="store", default="")
    parser.addoption("--sample_rows", action="store", default="")

    parser.addoption("--target_host", action="store", default="")
    parser.addoption("--target_port", action="store", default="")
    parser.addoption("--target_username", action="store", default="")
    parser.addoption("--target_password", action="store", default="")
    parser.addoption("--target_dbname", action="store", default="")
    parser.addoption("--target_schemas", action="store", default="")


def pytest_generate_tests(metafunc):

    source_host = metafunc.config.option.source_host
    source_port = metafunc.config.option.source_port
    source_username = metafunc.config.option.source_username
    source_password = metafunc.config.option.source_password
    source_sid = metafunc.config.option.source_sid
    source_schemas = metafunc.config.option.source_schemas
    target_host = metafunc.config.option.target_host
    target_port = metafunc.config.option.target_port
    target_username = metafunc.config.option.target_username
    target_password = metafunc.config.option.target_password
    target_dbname = metafunc.config.option.target_dbname
    target_schemas = metafunc.config.option.target_schemas
    sample_rows = metafunc.config.option.sample_rows

    if "not_null_column" in metafunc.fixturenames:
        metafunc.parametrize("not_null_column", dp.get_not_null_columns_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "primary_key" in metafunc.fixturenames:
        metafunc.parametrize("primary_key", dp.get_primary_keys_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "unique_key" in metafunc.fixturenames:
        metafunc.parametrize("unique_key", dp.get_unique_keys_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "foreign_key" in metafunc.fixturenames:
        metafunc.parametrize("foreign_key", dp.get_foreign_keys_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "table_physical_existance" in metafunc.fixturenames:
        metafunc.parametrize("table_physical_existance", dp.get_tables_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "view_physical_existance" in metafunc.fixturenames:
        metafunc.parametrize("view_physical_existance", dp.get_views_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "column_names_match" in metafunc.fixturenames:
        metafunc.parametrize("column_names_match", dp.get_columns_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "get_tables_list" in metafunc.fixturenames:
        metafunc.parametrize("get_tables_list", dp.get_tables_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "check_constraints" in metafunc.fixturenames:
        metafunc.parametrize("check_constraints", dp.get_check_constraints_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "index" in metafunc.fixturenames:
        metafunc.parametrize("index", dp.get_indexes_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "sequence" in metafunc.fixturenames:
        metafunc.parametrize("sequence", dp.get_sequences_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "triggers_list" in metafunc.fixturenames:
        metafunc.parametrize("triggers_list", ColumnValidationManager().get_triggers_from_target(get_schemas_list(target_schemas), target_dbname, target_host, target_port, target_username, target_password))
    if "triggers_physical_existance" in metafunc.fixturenames:
        metafunc.parametrize("triggers_physical_existance", dp.get_triggers_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "source_partitions_list" in metafunc.fixturenames:
        metafunc.parametrize("source_partitions_list", dp.get_partitions_list_from_source(source_schemas, source_host, source_port, source_username, source_password, source_sid))
    if "target_sequences" in metafunc.fixturenames:
        metafunc.parametrize("target_sequences", ColumnValidationManager().get_all_sequences_from_target(get_schemas_list(target_schemas), target_dbname, target_host, target_port, target_username, target_password))


@pytest.fixture(scope="session")
def source_host(request):
    return request.config.getoption("--source_host")


@pytest.fixture(scope="session")
def source_port(request):
    return request.config.getoption("--source_port")


@pytest.fixture(scope="session")
def source_username(request):
    return request.config.getoption("--source_username")


@pytest.fixture(scope="session")
def source_password(request):
    return request.config.getoption("--source_password")


@pytest.fixture(scope="session")
def source_sid(request):
    return request.config.getoption("--source_sid")


@pytest.fixture(scope="session")
def source_schemas(request):
    return request.config.getoption("--source_schemas")


@pytest.fixture(scope="session")
def target_host(request):
    return request.config.getoption("--target_host")


@pytest.fixture(scope="session")
def target_port(request):
    return request.config.getoption("--target_port")


@pytest.fixture(scope="session")
def target_username(request):
    return request.config.getoption("--target_username")


@pytest.fixture(scope="session")
def target_password(request):
    return request.config.getoption("--target_password")


@pytest.fixture(scope="session")
def target_dbname(request):
    return request.config.getoption("--target_dbname")


@pytest.fixture(scope="session")
def target_schemas(request):
    return request.config.getoption("--target_schemas")


@pytest.fixture(scope="session")
def sample_rows(request):
    return request.config.getoption("--sample_rows")