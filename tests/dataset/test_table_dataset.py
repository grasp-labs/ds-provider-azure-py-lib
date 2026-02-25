"""
**File:** ``test_table_dataset.py``
**Region:** ``tests/dataset/test_table_dataset``

Unit tests for AzureTable dataset class.

covers:
- Validation of linked service type.
- Content preparation and serialization.
- Deserialization of table entities to DataFrame.
- CRUD operations with error handling.
"""

import json
import uuid
from unittest.mock import MagicMock

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.data.tables import TableClient, TableServiceClient, TableTransactionError, UpdateMode
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DatasetException,
    DeleteError,
    ReadError,
    UpdateError,
)

from ds_provider_azure_py_lib.dataset.table import (
    AzureTable,
    AzureTableDatasetSettings,
    AzureTableDeserializer,
    PurgeSettings,
    ReadSettings,
)
from ds_provider_azure_py_lib.enums import ResourceType
from ds_provider_azure_py_lib.linked_service.storage_account import AzureLinkedService


def make_linked_service_with_table_client(table_client: TableClient | None = None) -> tuple[MagicMock, MagicMock]:
    """
    Helper to create a mocked AzureLinkedService that returns a TableServiceClient with an optional TableClient.

    Args:
        table_client (TableClient | None): Optional TableClient to be returned by get_table_client
            method. If None, the TableServiceClient will not have a TableClient set up.

    Returns:
        tuple: (AzureLinkedService mock, TableServiceClient mock)
    """
    svc = MagicMock(spec=TableServiceClient)
    if table_client:
        svc.get_table_client.return_value = table_client
    linked = MagicMock(spec=AzureLinkedService)
    connection_mock = MagicMock()
    connection_mock.table_service_client = svc
    linked.connection = connection_mock
    return linked, svc


def test_prepare_content_validations_and_serializer_json_conversion():
    """
    Test the _prepare_content method of AzureTable for various validations and serialization.
    """
    linked, _ = make_linked_service_with_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )

    # missing required columns -> NotImplementedError
    content_2 = pd.DataFrame([{"PartitionKey": "p"}])
    with pytest.raises(DatasetException):
        ds._prepare_content(content_2)

    # serializer None -> ValueError
    ds2 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        serializer=None,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    content_3 = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    with pytest.raises(DatasetException):
        ds2._prepare_content(content_3)

    # successful serialization: RowKey/PartitionKey to str, dict value to JSON
    ds3 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    content_4 = pd.DataFrame([{"PartitionKey": 10, "RowKey": 5, "Payload": {"a": 1}}])
    ent = ds3._prepare_content(content_4)
    assert ent["PartitionKey"] == "10" and ent["RowKey"] == "5"
    assert isinstance(ent["Payload"], str) and json.loads(ent["Payload"]) == {"a": 1}


def test_deserializer_builds_dataframe_and_uses_metadata_timestamp():
    """
    Test AzureTableDeserializer converts entities to DataFrame and handles Timestamp from metadata.
    """
    # entity without Timestamp -> take from metadata
    e1 = MagicMock()
    e1_keys = ["PartitionKey", "RowKey", "Value"]
    e1.__iter__.return_value = iter(e1_keys)
    e1.__getitem__.side_effect = {"PartitionKey": "p1", "RowKey": "r1", "Value": 1}.__getitem__
    e1.metadata = {"timestamp": "2024-01-01T00:00:00Z"}

    # entity with Timestamp present
    e2 = {"PartitionKey": "p2", "RowKey": "r2", "Value": 2, "Timestamp": "2024-01-02T00:00:00Z"}

    df = AzureTableDeserializer()([e1, e2])
    assert isinstance(df, pd.DataFrame)
    assert {"PartitionKey", "RowKey", "Value", "Timestamp"}.issubset(df.columns)
    assert len(df) == 2
    assert df.loc[df["RowKey"] == "r1", "Timestamp"].iloc[0] == "2024-01-01T00:00:00Z"


def test_prepare_content_raises_error_when_input_is_empty():
    """
    Test that _prepare_content raises ValueError when input DataFrame is empty.
    """
    linked, _ = make_linked_service_with_table_client(MagicMock(spec=TableClient))
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    with pytest.raises(DatasetException):
        ds._prepare_content(pd.DataFrame())


def test_prepare_content_raises_typeerror_for_non_dataframe_input():
    """
    Test that _prepare_content raises TypeError when input is not a DataFrame.
    """
    linked, _ = make_linked_service_with_table_client(MagicMock(spec=TableClient))
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    with pytest.raises(DatasetException):
        content = [{"PartitionKey": "p", "RowKey": "r"}]
        ds._prepare_content(content)


def test_rename_and_close_and_type():
    """
    Test rename raises NotImplementedError, close is no-op, and type property.
    """
    linked, _ = make_linked_service_with_table_client(MagicMock(spec=TableClient))
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    with pytest.raises(NotImplementedError):
        ds.rename()
    ds.close()  # no-op
    assert ds.type == ResourceType.TABLE


def _make_table_transaction_error():
    return TableTransactionError(message="boom", response=MagicMock())


def _make_linked_service_and_table_client():
    table_client = MagicMock(spec=TableClient)
    svc = MagicMock(spec=TableServiceClient)
    svc.get_table_client.return_value = table_client
    linked = MagicMock(spec=AzureLinkedService)
    connection_mock = MagicMock()
    connection_mock.table_service_client = svc
    linked.connection = connection_mock
    return linked, table_client, svc


def test_get_table_client_uses_service_client():
    linked, table_client, svc = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    assert ds._get_table_client() is table_client
    svc.get_table_client.assert_called_once_with(table_name="t")


def test_build_transaction_from_input_maps_errors():
    linked, _, _ = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.input = pd.DataFrame([{"PartitionKey": "p"}])
    with pytest.raises(CreateError):
        ds._build_transaction_from_input("create")
    with pytest.raises(UpdateError):
        ds._build_transaction_from_input("upsert")
    with pytest.raises(DeleteError):
        ds._build_transaction_from_input("delete")


def test_submit_transaction_noop_and_maps_errors():
    linked, table_client, _ = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds._submit_transaction([], CreateError)
    table_client.submit_transaction.assert_not_called()
    table_client.submit_transaction.side_effect = _make_table_transaction_error()
    with pytest.raises(CreateError):
        ds._submit_transaction([("create", {"PartitionKey": "p", "RowKey": "1"})], CreateError)


def test_create_table_handles_exists_and_http_error():
    linked, _, svc = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds._create_table()
    svc.create_table.assert_called_once()
    svc.create_table.side_effect = ResourceExistsError()
    ds._create_table()
    svc.create_table.side_effect = HttpResponseError(message="x")
    with pytest.raises(CreateError):
        ds._create_table()


def test_delete_table_success_and_error():
    linked, _, svc = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds._delete_table()
    svc.delete_table.assert_called_once_with(table_name="t")
    svc.delete_table.side_effect = HttpResponseError(message="x")
    with pytest.raises(DeleteError):
        ds._delete_table()


def test_read_uses_filter_and_handles_error():
    table_client = MagicMock()
    table_client.query_entities.return_value = [{"PartitionKey": "p", "RowKey": "1", "Timestamp": "2024-01-01T00:00:00Z"}]
    table_client.list_entities.return_value = [{"PartitionKey": "p", "RowKey": "2", "Timestamp": "2024-01-02T00:00:00Z"}]
    linked, _, svc = _make_linked_service_and_table_client()
    svc.get_table_client.return_value = table_client
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t", read=ReadSettings(query_filter="f")),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.read()
    table_client.query_entities.assert_called_once_with(query_filter="f")
    assert isinstance(ds.output, pd.DataFrame) and len(ds.output) == 1
    ds2 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds2.read()
    table_client.list_entities.assert_called_once()
    table_client.list_entities.side_effect = HttpResponseError(message="x")
    with pytest.raises(ReadError):
        ds2.read()


def test_create_success_table_transaction_and_http_error():
    linked, table_client, svc = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.input = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    ds.create()
    svc.create_table.assert_called()
    table_client.submit_transaction.assert_called_once()
    assert ds.output.equals(ds.input)
    ds._submit_transaction = MagicMock(side_effect=_make_table_transaction_error())
    with pytest.raises(CreateError):
        ds.create()
    ds._submit_transaction = MagicMock(side_effect=HttpResponseError(message="x"))
    with pytest.raises(CreateError):
        ds.create()


def test_update_calls_submit_with_replace_mode():
    linked, table_client, _ = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.input = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    ds.update()
    table_client.submit_transaction.assert_called_once()
    op, _, params = table_client.submit_transaction.call_args[0][0][0]
    assert op == "upsert" and params == {"mode": UpdateMode.REPLACE}


def test_delete_with_delete_table_flag_and_entity_path():
    linked, table_client, svc = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t", purge=PurgeSettings(delete_table=True)),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.purge()
    svc.delete_table.assert_called_once_with(table_name="t")
    ds2 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds2.input = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    ds2.delete()
    table_client.submit_transaction.assert_called()


def test_get_details_includes_read_and_delete_settings():
    ds = AzureTable(
        settings=AzureTableDatasetSettings(
            table_name="t",
            read=ReadSettings(query_filter="f"),
            purge=PurgeSettings(delete_table=True),
        ),
        linked_service=MagicMock(spec=AzureLinkedService),
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    details = ds.get_details()
    assert details["table_name"] == "t"
    assert details["query_filter"] == "f"
    assert details["delete_table"] == "True"


def test_table_deserializer_no_data_returns_empty_dataframe() -> None:
    deserializer = AzureTableDeserializer()
    result = deserializer([])
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_table_not_implemented_methods() -> None:
    """Test NotImplementedError methods for AzureTable."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    with pytest.raises(NotImplementedError, match="Rename operation is not supported"):
        table.rename()

    with pytest.raises(NotImplementedError, match=r"List operation is not supported.*Azure Table"):
        table.list()

    with pytest.raises(NotImplementedError, match=r"Upsert operation is not supported.*Azure Table"):
        table.upsert()


def test_table_create_with_empty_input() -> None:
    """Test create() with empty DataFrame and None input."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Test empty DataFrame
    table.input = pd.DataFrame()
    table.create()
    assert isinstance(table.output, pd.DataFrame)
    assert len(table.output) == 0

    # Test None input
    table.input = None
    table.create()
    assert isinstance(table.output, pd.DataFrame)
    assert len(table.output) == 0


def test_table_update_with_empty_input() -> None:
    """Test update() with empty DataFrame and None input."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Test empty DataFrame
    table.input = pd.DataFrame()
    table.update()
    assert isinstance(table.output, pd.DataFrame)
    assert len(table.output) == 0

    # Test None input
    table.input = None
    table.update()
    assert isinstance(table.output, pd.DataFrame)
    assert len(table.output) == 0


def test_table_delete_with_empty_input() -> None:
    """Test delete() with empty DataFrame and None input."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Test empty DataFrame
    table.input = pd.DataFrame()
    table.delete()
    assert isinstance(table.output, pd.DataFrame)
    assert len(table.output) == 0

    # Test None input
    table.input = None
    table.delete()
    assert isinstance(table.output, pd.DataFrame)
    assert len(table.output) == 0


def test_table_transaction_error_for_unknown_operation() -> None:
    """Test error handling in _build_transaction_from_input for unknown operation type."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    table.input = pd.DataFrame(
        {
            "PartitionKey": ["pk1"],
            "RowKey": ["rk1"],
            "Data": ["test"],
        }
    )
    table._prepare_content = MagicMock(side_effect=DatasetException("Test error", status_code=400))

    # Test unknown operation type goes to the else clause
    with pytest.raises(DatasetException):
        table._build_transaction_from_input("unknown_op")


def test_table_submit_transaction_error_without_status_code() -> None:
    """Test _submit_transaction handles errors without status_code."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Create a mock exception without status_code attribute
    mock_exc = TableTransactionError()
    mock_exc.message = "Test error"
    mock_exc.status_code = None

    table._get_table_client = MagicMock()
    table._get_table_client.return_value.submit_transaction = MagicMock(side_effect=mock_exc)

    # Pass a non-empty transaction list to trigger the submit
    with pytest.raises(DeleteError):
        table._submit_transaction([("delete", {"PartitionKey": "pk", "RowKey": "rk"})], DeleteError)


def test_table_read_without_filter() -> None:
    """Test read() without query filter calls list_entities()."""
    mock_table_client = MagicMock()
    mock_entities = [{"PartitionKey": "pk", "RowKey": "rk"}]
    mock_table_client.list_entities.return_value = mock_entities

    # Mock the connection to return a table_service_client
    mock_connection = MagicMock()
    mock_service_client = MagicMock()
    mock_service_client.get_table_client.return_value = mock_table_client
    mock_connection.table_service_client = mock_service_client

    linked_service = MagicMock()
    linked_service.connection = mock_connection

    table = AzureTable(
        deserializer=MagicMock(return_value=pd.DataFrame(mock_entities)),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Don't set query_filter to test the else clause
    table.settings.read.query_filter = None

    table.read()

    mock_table_client.list_entities.assert_called_once()
    table.deserializer.assert_called_once()


def test_table_read_without_deserializer_raises_error() -> None:
    """Test read() raises ReadError when deserializer is None."""
    mock_table_client = MagicMock()
    mock_entities = [{"PartitionKey": "pk", "RowKey": "rk"}]
    mock_table_client.list_entities.return_value = mock_entities

    # Mock the connection to return a table_service_client
    mock_connection = MagicMock()
    mock_service_client = MagicMock()
    mock_service_client.get_table_client.return_value = mock_table_client
    mock_connection.table_service_client = mock_service_client

    linked_service = MagicMock()
    linked_service.connection = mock_connection

    table = AzureTable(
        deserializer=None,
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test"),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )
    table.settings.read.query_filter = None

    with pytest.raises(ReadError, match="Deserializer is not initialized"):
        table.read()
