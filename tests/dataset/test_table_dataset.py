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
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient, TableTransactionError, UpdateMode
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DatasetException,
    DeleteError,
    ReadError,
    UpdateError,
    UpsertError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ValidationError

from ds_provider_azure_py_lib.dataset.table import (
    AzureTable,
    AzureTableDatasetSettings,
    AzureTableDeserializer,
    CreateSettings,
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

    # missing required columns -> NotSupportedError
    content_2 = pd.DataFrame([{"PartitionKey": "p"}])
    with pytest.raises(ValidationError):
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
    Test rename raises NotSupportedError, close is no-op, and type property.
    """
    linked, _ = make_linked_service_with_table_client(MagicMock(spec=TableClient))
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    with pytest.raises(NotSupportedError):
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
    with pytest.raises(ValidationError):
        ds._build_transaction_from_input("create")
    with pytest.raises(ValidationError):
        ds._build_transaction_from_input("update")
    with pytest.raises(ValidationError):
        ds._build_transaction_from_input("upsert")
    with pytest.raises(ValidationError):
        ds._build_transaction_from_input("delete")


@pytest.mark.parametrize(
    ("operation", "expected_error"),
    [
        ("create", CreateError),
        ("update", UpdateError),
        ("upsert", UpsertError),
        ("delete", DeleteError),
    ],
)
def test_build_transaction_from_input_maps_dataset_exception_by_operation(operation, expected_error):
    linked, _, _ = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.input = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    ds._prepare_content = MagicMock(side_effect=DatasetException("boom", status_code=400))

    with pytest.raises(expected_error):
        ds._build_transaction_from_input(operation)


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
    svc.create_table.side_effect = HttpResponseError(error_code="TableAlreadyExists", message="x")
    with pytest.raises(CreateError):
        ds._create_table()


def test_create_table_retries_when_table_is_being_deleted():
    linked, _, svc = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    err = HttpResponseError(message="x")
    err.error_code = "TableBeingDeleted"
    svc.create_table.side_effect = err
    ds._retry_create = MagicMock()

    ds._create_table()

    ds._retry_create.assert_called_once()


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


def test_update_calls_submit_with_merge_mode():
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
    assert op == "update" and params == {"mode": UpdateMode.MERGE}


def test_upsert_calls_submit_with_replace_mode():
    linked, table_client, _ = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.input = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    ds.upsert()
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


def test_delete_handles_404_and_re_raises_other_delete_errors():
    linked, _, _ = _make_linked_service_and_table_client()
    ds = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
    )
    ds.input = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])

    ds._submit_transaction = MagicMock(side_effect=DeleteError("not found", status_code=404))
    ds.delete()
    assert ds.output.equals(ds.input)

    ds._submit_transaction = MagicMock(side_effect=DeleteError("boom", status_code=500))
    with pytest.raises(DeleteError):
        ds.delete()


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
    """Test NotSupportedError methods for AzureTable."""
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

    with pytest.raises(NotSupportedError, match="Rename operation is not supported"):
        table.rename()

    with pytest.raises(NotSupportedError, match=r"List operation is not supported.*Azure Table"):
        table.list()


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


def test_table_purge_deletes_all_entities() -> None:
    """Test that purge() deletes all entities from the table."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test", purge=PurgeSettings(delete_table=False)),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Mock the table client with entities
    mock_table_client = MagicMock()
    mock_entities = [
        MagicMock(
            spec=dict,
            **{
                "__getitem__": MagicMock(side_effect=lambda k: "pk1" if k == "PartitionKey" else "rk1"),
                "__iter__": lambda: iter(["PartitionKey", "RowKey"]),
            },
        ),
        MagicMock(
            spec=dict,
            **{
                "__getitem__": MagicMock(side_effect=lambda k: "pk2" if k == "PartitionKey" else "rk2"),
                "__iter__": lambda: iter(["PartitionKey", "RowKey"]),
            },
        ),
    ]
    mock_table_client.list_entities.return_value = mock_entities

    table.linked_service.connection.table_service_client.get_table_client.return_value = mock_table_client

    table.purge()

    # Verify list_entities was called
    mock_table_client.list_entities.assert_called_once()
    # Verify submit_transaction was called
    mock_table_client.submit_transaction.assert_called_once()


def test_table_purge_with_empty_table() -> None:
    """Test that purge() handles empty table gracefully."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test", purge=PurgeSettings(delete_table=False)),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.1",
        description="test",
    )

    # Mock the table client with no entities
    mock_table_client = MagicMock()
    mock_table_client.list_entities.return_value = []

    table.linked_service.connection.table_service_client.get_table_client.return_value = mock_table_client

    table.purge()

    # Verify list_entities was called
    mock_table_client.list_entities.assert_called_once()
    # Verify submit_transaction was NOT called (no entities to delete)
    mock_table_client.submit_transaction.assert_not_called()


def test_table_purge_delete_table() -> None:
    """Test that purge() deletes the entire table when delete_table=True."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test", purge=PurgeSettings(delete_table=True)),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    table.purge()

    # Verify delete_table was called
    table.linked_service.connection.table_service_client.delete_table.assert_called_once_with(table_name="test")


def test_table_purge_http_error() -> None:
    """Test that purge() raises DeleteError on HTTP error."""
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test", purge=PurgeSettings(delete_table=False)),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    # Mock the table client to raise an error
    mock_table_client = MagicMock()
    mock_table_client.list_entities.side_effect = HttpResponseError(message="list failed")

    table.linked_service.connection.table_service_client.get_table_client.return_value = mock_table_client

    with pytest.raises(DeleteError, match="Failed to purge entities"):
        table.purge()


def test_table_purge_404_is_treated_as_already_purged() -> None:
    linked_service, _ = make_linked_service_with_table_client()
    table = AzureTable(
        deserializer=MagicMock(),
        serializer=MagicMock(),
        settings=AzureTableDatasetSettings(table_name="test", purge=PurgeSettings(delete_table=False)),
        linked_service=linked_service,
        id="test-id",
        name="test-table",
        version="1.0.0",
        description="test",
    )

    mock_table_client = MagicMock()
    not_found = HttpResponseError(message="not found")
    not_found.status_code = 404
    mock_table_client.list_entities.side_effect = not_found
    table.linked_service.connection.table_service_client.get_table_client.return_value = mock_table_client

    table.purge()


def test_upsert_with_empty_input_noop() -> None:
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

    table.input = pd.DataFrame()
    table.upsert()
    assert isinstance(table.output, pd.DataFrame)
    assert table.output.empty

    table.input = None
    table.upsert()
    assert isinstance(table.output, pd.DataFrame)
    assert table.output.empty


def _make_table_with_settings(retries=3, sleep=0):
    settings = AzureTableDatasetSettings(
        table_name=..., create=CreateSettings(retries_number=retries, sleep_seconds_between_retries=sleep)
    )
    table = AzureTable(
        id=...,
        name=...,
        version=...,
        linked_service=MagicMock(),
        settings=settings,
    )
    return table


def test_retry_create_success_on_first_try():
    table = _make_table_with_settings()
    table.linked_service.connection.table_service_client.create_table = MagicMock()
    table._retry_create()  # Should not raise


def test_retry_create_table_being_deleted_then_success():
    table = _make_table_with_settings(retries=2)
    svc = table.linked_service.connection.table_service_client
    err = HttpResponseError(message="TableBeingDeleted")
    err.error_code = "TableBeingDeleted"
    svc.create_table = MagicMock(side_effect=[err, None])
    with patch("time.sleep"):
        table.settings.table_name = "test"
        table._retry_create()  # Should not raise


def test_retry_create_gives_up_and_raises():
    table = _make_table_with_settings(retries=2)
    svc = table.linked_service.connection.table_service_client
    svc.create_table = MagicMock(side_effect=HttpResponseError(message="TableBeingDeleted", error_code="TableBeingDeleted"))
    with patch("time.sleep"), pytest.raises(CreateError):
        table._retry_create()


def test_retry_create_raises_when_table_already_exists_after_waiting():
    table = _make_table_with_settings(retries=1)
    svc = table.linked_service.connection.table_service_client
    svc.create_table = MagicMock(side_effect=ResourceExistsError(message="exists"))

    with pytest.raises(CreateError, match="already exists"):
        table._retry_create()


def test_retry_create_exhausts_retries_on_table_being_deleted():
    table = _make_table_with_settings(retries=2)
    svc = table.linked_service.connection.table_service_client
    err = HttpResponseError(message="TableBeingDeleted")
    err.error_code = "TableBeingDeleted"
    svc.create_table = MagicMock(side_effect=[err, err])

    with patch("time.sleep"), pytest.raises(CreateError, match="after 2 retries"):
        table._retry_create()


def test_wait_for_table_deletion_confirms_deletion(monkeypatch):
    table = _make_table_with_settings()

    # Mock TableClient and list_entities to raise ResourceNotFoundError with error_code "TableNotFound"
    class FakeResourceNotFoundError(ResourceNotFoundError):
        def __init__(self):
            self.error_code = "TableNotFound"

    fake_table_client = MagicMock()
    m = MagicMock()
    m.__iter__.side_effect = FakeResourceNotFoundError()
    fake_table_client.list_entities.return_value = m

    # Patch _get_table_client to return our fake client
    monkeypatch.setattr(table, "_get_table_client", lambda: fake_table_client)

    # Should not raise
    table._wait_for_table_deletion()


def test_wait_for_table_deletion_iterates_entities_before_confirming(monkeypatch):
    table = _make_table_with_settings()

    class FakeResourceNotFoundError(ResourceNotFoundError):
        def __init__(self):
            self.error_code = "TableNotFound"

    first_client = MagicMock()
    first_client.list_entities.return_value = [{"PartitionKey": "p", "RowKey": "1"}]

    second_client = MagicMock()

    class _NotFoundIter:
        def __iter__(self):
            raise FakeResourceNotFoundError()

    second_client.list_entities.return_value = _NotFoundIter()

    calls = iter([first_client, second_client])
    monkeypatch.setattr(table, "_get_table_client", lambda: next(calls))

    with patch("ds_provider_azure_py_lib.dataset.table.t.sleep") as sleep_mock:
        table._wait_for_table_deletion()

    sleep_mock.assert_called_once()
