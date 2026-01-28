"""
**File:** ``02_blob_dataset_connect_read_and_delete.py``
**Region:** ``examples/02_blob_dataset_connect_read_and_delete``

Example 02: Connect to Azure Blob Storage using AzureBlob dataset, read data, and delete blobs.

This example demonstrates how to:
- Create an instance of `AzureBlob` dataset with the necessary settings, serializer, deserializer, and linked service.
- Read data from blobs in the specified container with a given prefix.
- Print the content read from the blobs.
- Delete the blobs after reading.

Prerequisites:
- An Azure Storage account with a container named "test-blob".
- Blobs in the container with names starting with "test" in CSV format.
"""

from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_provider_azure_py_lib.dataset.blob import AzureBlob, AzureBlobDatasetSettings
from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings


def main():
    dataset = AzureBlob(
        settings=AzureBlobDatasetSettings(
            container_name="test-blob",
            # blob_name="test3.csv",
            prefix="test", # to read all blobs with this prefix (must be in .csv format for deserializer to work)
        ),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
        linked_service=AzureLinkedService(
            settings=AzureLinkedServiceSettings(
                account_name="...",
                access_key="..."
            )
        )
    )

    dataset.read()
    print(dataset.content)
    dataset.delete()
    dataset.read()
    print(dataset.content)



if __name__ == "__main__":
    main()
