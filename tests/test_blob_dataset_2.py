from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, DeleteError, ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import InvalidLinkedServiceTypeError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_provider_azure_py_lib.dataset.blob import AzureBlob, AzureBlobDatasetSettings
from ds_provider_azure_py_lib.linked_service.storage_account import AzureLinkedService


def _make_base_linked_service(blob_client_mock: MagicMock | None = None) -> MagicMock:
    """
    Returns a MagicMock(spec=AzureLinkedService) with .connect() -> (BlobServiceClient mock, None)
    and .blob_service_client set to the same client mock. If blob_client_mock is None a generic
    BlobServiceClient spec mock is returned.
    """
    bs_client = blob_client_mock or MagicMock(spec=BlobServiceClient)
    linked = MagicMock(spec=AzureLinkedService)
    linked.connect.return_value = (bs_client, None)
    linked.blob_service_client = bs_client
    return linked


def _make_linked_service_with_clients(container_client=None, blob_client=None):
    bs_client = MagicMock(spec=BlobServiceClient)
    if container_client:
        bs_client.get_container_client.return_value = container_client
    if blob_client:
        bs_client.get_blob_client.return_value = blob_client
    linked = MagicMock(spec=AzureLinkedService)
    linked.connect.return_value = (bs_client, None)
    linked.blob_service_client = bs_client
    return linked, bs_client


def test_invalid_linked_service_client_type_raises_invalid_linked_service_type_error():
    linked = MagicMock(spec=AzureLinkedService)
    linked.connect.return_value = (object(), None)  # not a BlobServiceClient

    with pytest.raises(InvalidLinkedServiceTypeError):
        AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked,
        )


def test_read_raises_when_no_blob_or_prefix_provided():
    linked_service = _make_base_linked_service()
    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked_service,
    )

    with pytest.raises(ReadError):
        ds.read()


def test_create_raises_when_no_blob_name_and_when_no_serializer():
    linked = _make_base_linked_service()

    ds1 = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )
    with pytest.raises(CreateError):
        ds1.create()

    ds2 = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=None,
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )
    with pytest.raises(CreateError):
        ds2.create()


def test__create_container_handles_resource_exists_and_http_error():
    container_client = MagicMock()
    container_client.create_container.return_value = None
    bs_client = MagicMock(spec=BlobServiceClient)
    bs_client.get_container_client.return_value = container_client
    linked = _make_base_linked_service(bs_client)

    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )
    ds._create_container()  # should not raise

    container_client2 = MagicMock()
    container_client2.create_container.side_effect = ResourceExistsError("exists")
    bs_client2 = MagicMock(spec=BlobServiceClient)
    bs_client2.get_container_client.return_value = container_client2
    linked2 = _make_base_linked_service(bs_client2)

    ds2 = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked2,
    )
    ds2._create_container()  # should not raise

    container_client3 = MagicMock()
    container_client3.create_container.side_effect = HttpResponseError("http err")
    bs_client3 = MagicMock(spec=BlobServiceClient)
    bs_client3.get_container_client.return_value = container_client3
    linked3 = _make_base_linked_service(bs_client3)

    ds3 = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked3,
    )
    with pytest.raises(CreateError):
        ds3._create_container()


def test__create_blob_raises_create_error_on_upload_failure():
    bs_client = MagicMock(spec=BlobServiceClient)
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = HttpResponseError("upload fail")
    bs_client.get_blob_client.return_value = blob_client
    linked = _make_base_linked_service(bs_client)

    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )

    with pytest.raises(CreateError):
        ds._create_blob(b"data", blob="b")


def test__read_blob_handles_download_errors_and_empty_stream(monkeypatch):
    bs_client = MagicMock(spec=BlobServiceClient)
    blob_client = MagicMock()
    blob_client.download_blob.side_effect = HttpResponseError("dl fail")
    bs_client.get_blob_client.return_value = blob_client
    linked = _make_base_linked_service(bs_client)

    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )

    with pytest.raises(ReadError):
        ds._read_blob("b")

    blob_client2 = MagicMock()
    dl = MagicMock()
    dl.readall.return_value = b""
    blob_client2.download_blob.return_value = dl
    bs_client2 = MagicMock(spec=BlobServiceClient)
    bs_client2.get_blob_client.return_value = blob_client2
    linked2 = _make_base_linked_service(bs_client2)

    bad_deserializer = MagicMock(side_effect=RuntimeError("should not be called"))
    ds2 = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=bad_deserializer,
        linked_service=linked2,
    )

    res = ds2._read_blob("b")
    assert isinstance(res, pd.DataFrame)
    assert res.empty
    bad_deserializer.assert_not_called()


def test__delete_blob_raises_delete_error_on_failure():
    bs_client = MagicMock(spec=BlobServiceClient)
    blob_client = MagicMock()
    blob_client.delete_blob.side_effect = HttpResponseError("del fail")
    bs_client.get_blob_client.return_value = blob_client
    linked = _make_base_linked_service(bs_client)

    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )

    with pytest.raises(DeleteError):
        ds._delete_blob("b")


def test__delete_blobs_continues_on_individual_failure_and_uses_listing():
    bs_client = MagicMock(spec=BlobServiceClient)
    container_client = MagicMock()
    container_client.list_blobs.return_value = [SimpleNamespace(name="a"), SimpleNamespace(name="b")]
    bs_client.get_container_client.return_value = container_client
    linked = _make_base_linked_service(bs_client)

    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", prefix="p"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )

    def fake_delete_blob(name):
        if name == "b":
            raise RuntimeError("boom")
        return pd.DataFrame([{"Name": "x", "HEX": "#000", "RGB": "rgb(0,0,0)"}])

    ds._delete_blob = fake_delete_blob

    res = ds._delete_blobs("p")
    bs_client.get_container_client.assert_called_once_with("c")
    container_client.list_blobs.assert_called_once_with(name_starts_with="p")

    assert isinstance(res, pd.DataFrame)


def test_update_rename_close_behavior():
    linked = _make_base_linked_service()
    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )

    with pytest.raises(NotImplementedError):
        ds.update()

    with pytest.raises(NotImplementedError):
        ds.rename()

    ds.close()  # no-op


def test_create_success_hits_stream_container_and_upload_and_log():
    container_client = MagicMock()
    container_client.create_container.return_value = None

    blob_client = MagicMock()
    blob_client.upload_blob.return_value = None

    linked, bs_client = _make_linked_service_with_clients(container_client=container_client, blob_client=blob_client)

    ds = AzureBlob(
        settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
        serializer=PandasSerializer(format="CSV"),
        deserializer=PandasDeserializer(format="CSV"),
        linked_service=linked,
    )
    ds.content = pd.DataFrame({"x": [1]})

    ds.create()

    bs_client.get_container_client.assert_called_once_with("c")
    container_client.create_container.assert_called_once()

    bs_client.get_blob_client.assert_called_once_with(container="c", blob="b")
    blob_client.upload_blob.assert_called_once()
    _, kwargs = blob_client.upload_blob.call_args
    assert kwargs.get("overwrite") is True
    assert "data" in kwargs and kwargs["data"] is not None


def test_concat_empty():
    result = AzureBlob.concat([])

    assert isinstance(result, pd.DataFrame)
    assert result.empty
