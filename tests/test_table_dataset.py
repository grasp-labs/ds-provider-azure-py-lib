import json
from unittest.mock import MagicMock

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableClient, TableServiceClient, UpdateMode
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DeleteError,
    ReadError,
    UpdateError,
)
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import InvalidLinkedServiceTypeError

from ds_provider_azure_py_lib.dataset.table import (
    AzureTable,
    AzureTableDatasetSettings,
    AzureTableDeserializer,
)
from ds_provider_azure_py_lib.enums import ResourceType
from ds_provider_azure_py_lib.linked_service.storage_account import AzureLinkedService


def _make_linked_service_with_table_client(table_client: TableClient | None = None):
    svc = MagicMock(spec=TableServiceClient)
    if table_client:
        svc.get_table_client.return_value = table_client
    linked = MagicMock(spec=AzureLinkedService)
    # AzureTable expects connect() -> (something, TableServiceClient)
    linked.connect.return_value = (MagicMock(), svc)
    return linked, svc


def test_invalid_linked_service_type_and_wrong_client_type():
    # Not an AzureLinkedService instance
    with pytest.raises(InvalidLinkedServiceTypeError):
        AzureTable(
            settings=AzureTableDatasetSettings(table_name="t"),
            linked_service=object(),  # wrong type
        )
    # connect returns non-TableServiceClient
    linked = MagicMock(spec=AzureLinkedService)
    linked.connect.return_value = (MagicMock(), object())
    with pytest.raises(InvalidLinkedServiceTypeError):
        AzureTable(
            settings=AzureTableDatasetSettings(table_name="t"),
            linked_service=linked,
        )


def test_prepare_content_validations_and_serializer_json_conversion():
    linked, _ = _make_linked_service_with_table_client()
    ds = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)

    # len(content) != 1 -> NotImplementedError
    ds.content = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}, {"PartitionKey": "p", "RowKey": "2"}])
    with pytest.raises(NotImplementedError):
        ds._prepare_content()

    # missing required columns -> NotImplementedError
    ds.content = pd.DataFrame([{"PartitionKey": "p"}])
    with pytest.raises(NotImplementedError):
        ds._prepare_content()

    # serializer None -> ValueError
    ds2 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t"),
        linked_service=linked,
        serializer=None,
    )
    ds2.content = pd.DataFrame([{"PartitionKey": "p", "RowKey": "1"}])
    with pytest.raises(ValueError):
        ds2._prepare_content()

    # successful serialization: RowKey/PartitionKey to str, dict value to JSON
    ds3 = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    ds3.content = pd.DataFrame([{"PartitionKey": 10, "RowKey": 5, "Payload": {"a": 1}}])
    ent = ds3._prepare_content()
    assert ent["PartitionKey"] == "10" and ent["RowKey"] == "5"
    assert isinstance(ent["Payload"], str) and json.loads(ent["Payload"]) == {"a": 1}


def test_deserializer_builds_dataframe_and_uses_metadata_timestamp():
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


def test_read_uses_query_filter_or_list_and_handles_errors():
    table_client = MagicMock(spec=TableClient)
    linked, _ = _make_linked_service_with_table_client(table_client)

    # without filter -> list_entities
    ds1 = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    table_client.list_entities.return_value = [{"PartitionKey": "p", "RowKey": "r", "Timestamp": "t"}]
    ds1.read()
    assert isinstance(ds1.content, pd.DataFrame)
    table_client.list_entities.assert_called_once()

    # with filter -> query_entities
    ds2 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t", partition_key="pX"),
        linked_service=linked,
    )
    table_client.query_entities.return_value = [{"PartitionKey": "pX", "RowKey": "rX", "Timestamp": "tX"}]
    ds2.read()
    table_client.query_entities.assert_called_once()
    _, kwargs = table_client.query_entities.call_args
    assert "query_filter" in kwargs and "PartitionKey eq 'pX'" in kwargs["query_filter"]

    # HttpResponseError -> ReadError
    table_client.list_entities.side_effect = HttpResponseError("boom")
    ds3 = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    with pytest.raises(ReadError):
        ds3.read()


def test_create_success_and_errors():
    table_client = MagicMock(spec=TableClient)
    linked, svc = _make_linked_service_with_table_client(table_client)

    # success path
    ds = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    ds.content = pd.DataFrame([{"PartitionKey": "p", "RowKey": "r", "v": 1}])
    ds.create()
    svc.create_table.assert_called_once_with(table_name="t")
    table_client.create_entity.assert_called_once()
    entity_kwargs = table_client.create_entity.call_args.kwargs
    assert "entity" in entity_kwargs and entity_kwargs["entity"]["PartitionKey"] == "p"

    # table exists -> swallowed
    svc.create_table.reset_mock()
    table_client.create_entity.reset_mock()
    svc.create_table.side_effect = ResourceExistsError("exists")
    ds.create()
    table_client.create_entity.assert_called_once()

    # create table HttpResponseError -> CreateError
    svc.create_table.reset_mock()
    table_client.create_entity.reset_mock()
    svc.create_table.side_effect = HttpResponseError("fail")
    with pytest.raises(CreateError):
        ds.create()

    # create entity exists -> warning, not raising
    svc.create_table.side_effect = None
    table_client.create_entity.side_effect = ResourceExistsError("exists")
    ds.create()  # should not raise

    # create entity HttpResponseError -> CreateError
    table_client.create_entity.side_effect = HttpResponseError("fail")
    with pytest.raises(CreateError):
        ds.create()


def test_update_success_and_error():
    table_client = MagicMock(spec=TableClient)
    linked, _ = _make_linked_service_with_table_client(table_client)

    ds = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    ds.content = pd.DataFrame([{"PartitionKey": "p", "RowKey": "r", "v": 1}])

    ds.update()
    table_client.upsert_entity.assert_called_once()
    kwargs = table_client.upsert_entity.call_args.kwargs
    assert kwargs.get("mode") == UpdateMode.MERGE

    table_client.upsert_entity.side_effect = HttpResponseError("fail")
    with pytest.raises(UpdateError):
        ds.update()


def test_delete_entity_and_table_paths_and_errors():
    table_client = MagicMock(spec=TableClient)
    linked, svc = _make_linked_service_with_table_client(table_client)

    # delete entity success
    ds = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    ds.content = pd.DataFrame([{"PartitionKey": "p", "RowKey": "r"}])
    ds.delete()
    table_client.delete_entity.assert_called_once_with(row_key="r", partition_key="p")

    # delete entity errors -> DeleteError
    table_client.delete_entity.side_effect = ResourceNotFoundError("nf")
    with pytest.raises(DeleteError):
        ds.delete()
    table_client.delete_entity.side_effect = HttpResponseError("fail")
    with pytest.raises(DeleteError):
        ds.delete()

    # delete table success
    ds2 = AzureTable(
        settings=AzureTableDatasetSettings(table_name="t", delete_table=True),
        linked_service=linked,
    )
    svc.delete_table.return_value = None
    ds2.delete()
    svc.delete_table.assert_called_with(table_name="t")

    # delete table error -> DeleteError
    svc.delete_table.side_effect = HttpResponseError("fail")
    with pytest.raises(DeleteError):
        ds2.delete()


def test_rename_and_close_and_type():
    linked, _ = _make_linked_service_with_table_client(MagicMock(spec=TableClient))
    ds = AzureTable(settings=AzureTableDatasetSettings(table_name="t"), linked_service=linked)
    with pytest.raises(NotImplementedError):
        ds.rename()
    ds.close()  # no-op
    assert ds.type == ResourceType.BLOB
