"""
**File:** ``test_blob_dataset.py``
**Region:** ``tests/dataset/test_blob_dataset``

Azure Blob Dataset tests.

Covers:
- Reading CSV files from Azure Blob Storage by blob name and prefix.
- Deleting blobs by name and prefix.
- Error handling during create, read, and delete operations.
"""

import io
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, DeleteError, ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import InvalidLinkedServiceTypeError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_provider_azure_py_lib.dataset.blob import AzureBlob, AzureBlobDatasetSettings
from ds_provider_azure_py_lib.enums import ResourceType
from ds_provider_azure_py_lib.linked_service import AzureLinkedService


class TestAzureBlobDataset(unittest.TestCase):
    CSV_TEST3 = (
        "Name,HEX,RGB\n"
        'Navy,#000080,"rgb(0,0,128)"\n'
        'Teal,#008080,"rgb(0,128,128)"\n'
        'Olive,#808000,"rgb(128,128,0)"\n'
        'Maroon,#800000,"rgb(128,0,0)"\n'
        'Lime,#00FF00,"rgb(0,255,0)"\n'
        'Blue,#0000FF,"rgb(0,0,255)"\n'
    )

    CSV_TEST2 = (
        "Name,HEX,RGB\n"
        'White,#FFFFFF,"rgb(100,100,100)"\n'
        'Silver,#C0C0C0,"rgb(75,75,75)"\n'
        'Gray,#808080,"rgb(50,50,50)"\n'
        'Black,#000000,"rgb(0,0,0)"\n'
        'Red,#FF0000,"rgb(100,0,0)"\n'
    )

    @staticmethod
    def _make_linked_service_mock(blob_bytes_map: dict) -> AzureLinkedService:
        """
        A helper to create a mocked AzureLinkedService with a BlobServiceClient that returns blobs
        with the provided byte content.

        Args:
            blob_bytes_map: A dict mapping blob names to their byte content.
        Returns:
            A MagicMock spec AzureLinkedService with the mocked BlobServiceClient.

        """
        mock_blob_service_client = MagicMock(spec=BlobServiceClient)

        mock_container_client = MagicMock()
        mock_container_client.list_blobs.return_value = [SimpleNamespace(name=n) for n in blob_bytes_map]
        mock_blob_service_client.get_container_client.return_value = mock_container_client

        mock_blob_map = {}
        for name, data in blob_bytes_map.items():
            m = MagicMock()
            dl = MagicMock()
            dl.readall.return_value = data
            m.download_blob.return_value = dl
            mock_blob_map[name] = m

        def _get_blob_client_side_effect(*args, **kwargs):
            blob_name = kwargs.get("blob") or kwargs.get("blob_name") or kwargs.get("name")
            if not blob_name:
                if len(args) >= 2:
                    blob_name = args[1]
                elif len(args) == 1:
                    blob_name = args[0]
            if blob_name in mock_blob_map:
                return mock_blob_map[blob_name]
            default = MagicMock()
            default.download_blob.return_value.readall.return_value = b""
            return default

        mock_blob_service_client.get_blob_client.side_effect = _get_blob_client_side_effect

        linked_service = MagicMock(spec=AzureLinkedService)
        linked_service.blob_service_client = mock_blob_service_client
        linked_service.service = "blob"
        linked_service.connect.return_value = (mock_blob_service_client, None)
        return linked_service

    @staticmethod
    def _assert_dataframe_matches(df: pd.DataFrame, expected):
        """
        expected: either a pandas.DataFrame or a list[dict]
        Compare only the Name, HEX, RGB columns and ignore the index.
        """
        expected_df = expected if isinstance(expected, pd.DataFrame) else pd.DataFrame(expected)
        pd.testing.assert_frame_equal(df.reset_index(drop=True)[["Name", "HEX", "RGB"]], expected_df[["Name", "HEX", "RGB"]])

    def test_read_test3_csv_from_blob(self):
        """
        Test reading a CSV file named 'test3.csv' from Azure Blob Storage.
        Validates that the output DataFrame matches the expected content.
        """
        linked_service = self._make_linked_service_mock({"test3.csv": self.CSV_TEST3.encode("utf-8")})

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", blob_name="test3.csv"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.read()
        # reference CSV_TEST3 instead of hardcoded rows
        expected_df = pd.read_csv(io.StringIO(self.CSV_TEST3))
        self.assertTrue(dataset.input.empty)  # assert input is empty
        self.assertIsNotNone(dataset.output)
        self._assert_dataframe_matches(dataset.output, expected_df)

    def test_read_test2_csv_from_blob(self):
        """
        Test reading a CSV file named 'test2.csv' from Azure Blob Storage.
        Validates that the output DataFrame matches the expected content.
        """
        linked_service = self._make_linked_service_mock({"test2.csv": self.CSV_TEST2.encode("utf-8")})

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", blob_name="test2.csv"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.read()

        # reference CSV_TEST2 instead of hardcoded rows
        expected_df = pd.read_csv(io.StringIO(self.CSV_TEST2))
        self._assert_dataframe_matches(dataset.output, expected_df)

    def test_get_by_prefix(self):
        """
        Test reading multiple CSV files from Azure Blob Storage using a prefix.
        Validates that the output DataFrame matches the concatenated expected content.
        """
        linked_service = self._make_linked_service_mock(
            {
                "test2.csv": self.CSV_TEST2.encode("utf-8"),
                "test3.csv": self.CSV_TEST3.encode("utf-8"),
            }
        )

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", prefix="test"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.read()

        # build expected by reading the CSV_TEST variables and concatenating
        expected_df = pd.concat(
            [pd.read_csv(io.StringIO(self.CSV_TEST2)), pd.read_csv(io.StringIO(self.CSV_TEST3))], ignore_index=True
        )
        self._assert_dataframe_matches(dataset.output, expected_df)

    def test_delete_blob_by_name(self):
        """
        Test deleting a single blob named 'test2.csv' from Azure Blob Storage.
        Validates that the delete_blob method was called on the correct blob.
        """
        linked_service = self._make_linked_service_mock({"test2.csv": self.CSV_TEST2.encode("utf-8")})

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", blob_name="test2.csv"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.delete()

        bs_client = linked_service.blob_service_client
        blob_client = bs_client.get_blob_client(container="test-blob", blob="test2.csv")
        blob_client.delete_blob.assert_called_once()

    def test_delete_blobs_by_prefix(self):
        """
        Test deleting multiple blobs from Azure Blob Storage using a prefix.
        Validates that the delete_blob method was called on each blob.
        """
        linked_service = self._make_linked_service_mock(
            {
                "test2.csv": self.CSV_TEST2.encode("utf-8"),
                "test3.csv": self.CSV_TEST3.encode("utf-8"),
            }
        )

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", prefix="test"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.delete()

        bs_client = linked_service.blob_service_client
        # ensure listing was used
        container_client = bs_client.get_container_client.return_value
        container_client.list_blobs.assert_called_once_with(name_starts_with="test")

        for name in ("test2.csv", "test3.csv"):
            blob_client = bs_client.get_blob_client(container="test-blob", blob=name)
            blob_client.delete_blob.assert_called_once()


class TestAzureBlobDataset2(unittest.TestCase):
    @staticmethod
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

    @staticmethod
    def _make_linked_service_with_clients(container_client=None, blob_client=None):
        """
        Returns a MagicMock(spec=AzureLinkedService) with .connect() -> (BlobServiceClient mock, None)
        and .blob_service_client set to the same client mock. The BlobServiceClient mock is set up to
        return the provided container_client and blob_client mocks when get_container_client and
        get_blob_client are called, respectively.
        Args:
            container_client: Optional MagicMock to be returned by get_container_client.
            blob_client: Optional MagicMock to be returned by get_blob_client.
        Returns:
            A tuple of (linked_service_mock, blob_service_client_mock).
        """
        bs_client = MagicMock(spec=BlobServiceClient)
        if container_client:
            bs_client.get_container_client.return_value = container_client
        if blob_client:
            bs_client.get_blob_client.return_value = blob_client
        linked = MagicMock(spec=AzureLinkedService)
        linked.connect.return_value = (bs_client, None)
        linked.blob_service_client = bs_client
        return linked, bs_client

    def test_invalid_linked_service_client_type_raises_invalid_linked_service_type_error(self):
        """
        Test that initializing AzureBlob with a linked service that does not return a BlobServiceClient
        from its connect() method raises InvalidLinkedServiceTypeError.
        """
        linked = MagicMock(spec=AzureLinkedService)
        linked.connect.return_value = (object(), None)  # not a BlobServiceClient

        with pytest.raises(InvalidLinkedServiceTypeError):
            AzureBlob(
                settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
                serializer=PandasSerializer(format="CSV"),
                deserializer=PandasDeserializer(format="CSV"),
                linked_service=linked,
            )

    def test_read_raises_when_no_blob_or_prefix_provided(self):
        """
        Test that calling read() on AzureBlob without blob_name or prefix in settings raises ReadError.
        """
        linked_service = self._make_base_linked_service()
        ds = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked_service,
        )

        with pytest.raises(ReadError):
            ds.read()

    def test_create_raises_when_no_blob_name_and_when_no_serializer(self):
        """
        Test that calling create() on AzureBlob without blob_name in settings or without a serializer
        raises CreateError.
        """
        linked = self._make_base_linked_service()

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

    def test__create_container_handles_resource_exists_and_http_error(self):
        """
        Test that _create_container() handles ResourceExistsError and HttpResponseError appropriately.
        """
        container_client = MagicMock()
        container_client.create_container.return_value = None
        bs_client = MagicMock(spec=BlobServiceClient)
        bs_client.get_container_client.return_value = container_client
        linked = self._make_base_linked_service(bs_client)

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
        linked2 = self._make_base_linked_service(bs_client2)

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
        linked3 = self._make_base_linked_service(bs_client3)

        ds3 = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked3,
        )
        with pytest.raises(CreateError):
            ds3._create_container()

    def test__create_blob_raises_create_error_on_upload_failure(self):
        """
        Test that _create_blob() raises CreateError when upload_blob fails.
        """
        bs_client = MagicMock(spec=BlobServiceClient)
        blob_client = MagicMock()
        blob_client.upload_blob.side_effect = HttpResponseError("upload fail")
        bs_client.get_blob_client.return_value = blob_client
        linked = self._make_base_linked_service(bs_client)

        ds = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked,
        )

        with pytest.raises(CreateError):
            ds._create_blob(b"data", blob="b")

    def test__read_blob_handles_download_errors_and_empty_stream(self):
        """
        Test that _read_blob() handles HttpResponseError during download and returns an empty DataFrame
        when the downloaded stream is empty.
        """
        bs_client = MagicMock(spec=BlobServiceClient)
        blob_client = MagicMock()
        blob_client.download_blob.side_effect = HttpResponseError("dl fail")
        bs_client.get_blob_client.return_value = blob_client
        linked = self._make_base_linked_service(bs_client)

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
        linked2 = self._make_base_linked_service(bs_client2)

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

    def test__delete_blob_raises_delete_error_on_failure(self):
        """
        Test that _delete_blob() raises DeleteError when delete_blob fails.
        """
        bs_client = MagicMock(spec=BlobServiceClient)
        blob_client = MagicMock()
        blob_client.delete_blob.side_effect = HttpResponseError("del fail")
        bs_client.get_blob_client.return_value = blob_client
        linked = self._make_base_linked_service(bs_client)

        ds = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked,
        )

        with pytest.raises(DeleteError):
            ds._delete_blob("b")

    def test__delete_blobs_continues_on_individual_failure_and_uses_listing(self):
        """
        Test that _delete_blobs() continues deleting other blobs even if one deletion fails,
        and that it uses blob listing to find blobs by prefix.
        """
        bs_client = MagicMock(spec=BlobServiceClient)
        container_client = MagicMock()
        container_client.list_blobs.return_value = [SimpleNamespace(name="a"), SimpleNamespace(name="b")]
        bs_client.get_container_client.return_value = container_client
        linked = self._make_base_linked_service(bs_client)

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

        self.assertRaises(DeleteError, ds._delete_blobs, "p")
        bs_client.get_container_client.assert_called_once_with("c")
        container_client.list_blobs.assert_called_once_with(name_starts_with="p")

    def test_update_rename_close_behavior(self):
        """
        Test rename raises NotImplementedError, close is no-op.
        """
        linked = self._make_base_linked_service()
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

    def test_create_success_hits_stream_container_and_upload_and_log(self):
        """
        Test that create() successfully creates the container and uploads the blob,
        and that the correct methods are called with expected parameters.
        """
        container_client = MagicMock()
        container_client.create_container.return_value = None

        blob_client = MagicMock()
        blob_client.upload_blob.return_value = None

        linked, bs_client = self._make_linked_service_with_clients(container_client=container_client, blob_client=blob_client)

        ds = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked,
        )
        ds.input = pd.DataFrame({"x": [1]})

        ds.create()

        bs_client.get_container_client.assert_called_once_with("c")
        container_client.create_container.assert_called_once()

        bs_client.get_blob_client.assert_called_once_with(container="c", blob="b")
        blob_client.upload_blob.assert_called_once()
        _, kwargs = blob_client.upload_blob.call_args
        assert kwargs.get("overwrite") is True
        assert "data" in kwargs and kwargs["data"] is not None

    def test_concat_empty(self):
        """
        Test that AzureBlob.concat with an empty list returns an empty DataFrame.
        """
        result = AzureBlob.concat([])

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_init_raises_type_error_for_non_azure_linked_service(self):
        with pytest.raises(TypeError):
            AzureBlob(
                settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
                serializer=PandasSerializer(format="CSV"),
                deserializer=PandasDeserializer(format="CSV"),
                linked_service=object(),  # not an AzureLinkedService
            )

    def test_type_property_returns_blob(self):
        """
        Test that the type property of AzureBlob returns ResourceType.BLOB.
        """
        linked = self._make_base_linked_service()
        ds = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c", blob_name="b"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked,
        )
        assert ds.type == ResourceType.BLOB

    def test_delete_raises_when_no_blob_name_or_prefix(self):
        """
        Test that calling delete() on AzureBlob without blob_name or prefix in settings raises DeleteError.
        """
        linked = self._make_base_linked_service()
        ds = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="c"),
            serializer=PandasSerializer(format="CSV"),
            deserializer=PandasDeserializer(format="CSV"),
            linked_service=linked,
        )
        with pytest.raises(DeleteError):
            ds.delete()
