# python
import io
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
from azure.storage.blob import BlobServiceClient
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_provider_azure_py_lib.dataset.blob import AzureBlob, AzureBlobDatasetSettings
from ds_provider_azure_py_lib.linked_service import AzureLinkedService


class TestAzureBlobDataset:
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
        linked_service = self._make_linked_service_mock({"test3.csv": self.CSV_TEST3.encode("utf-8")})

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", blob_name="test3.csv"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.read()
        df = dataset.content

        # reference CSV_TEST3 instead of hardcoded rows
        expected_df = pd.read_csv(io.StringIO(self.CSV_TEST3))
        self._assert_dataframe_matches(df, expected_df)

    def test_read_test2_csv_from_blob(self):
        linked_service = self._make_linked_service_mock({"test2.csv": self.CSV_TEST2.encode("utf-8")})

        dataset = AzureBlob(
            settings=AzureBlobDatasetSettings(container_name="test-blob", blob_name="test2.csv"),
            serializer=PandasSerializer(format=DatasetStorageFormatType.CSV),
            deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
            linked_service=linked_service,
        )

        dataset.read()
        df = dataset.content

        # reference CSV_TEST2 instead of hardcoded rows
        expected_df = pd.read_csv(io.StringIO(self.CSV_TEST2))
        self._assert_dataframe_matches(df, expected_df)

    def test_get_by_prefix(self):
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
        df = dataset.content

        # build expected by reading the CSV_TEST variables and concatenating
        expected_df = pd.concat(
            [pd.read_csv(io.StringIO(self.CSV_TEST2)), pd.read_csv(io.StringIO(self.CSV_TEST3))], ignore_index=True
        )
        self._assert_dataframe_matches(df, expected_df)

    def test_delete_blob_by_name(self):
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
