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
- An Azure Storage account with a container named "test-container".
- Blobs in the container with names starting with "test" and in CSV format.
"""
import os
import uuid

from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_provider_azure_py_lib.dataset.blob import AzureBlob, AzureBlobDatasetSettings, CreateSettings, DeleteSettings
from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings


def main():
    account_name = os.environ.get("ACCOUNT_NAME")
    account_key = os.environ.get("ACCOUNT_KEY")

    dataset = AzureBlob(
        settings=AzureBlobDatasetSettings(
            container_name="test-container",
            blob_name="test.csv",
            # prefix="test",  # to read all blobs with this prefix (all of them must be in the format specified in deserializer), remove blob_name when using prefix
            create=CreateSettings(
                overite_blob_if_exists=True, # if True, it will overwrite the blob if it already exists. If False, it will raise an error if the blob already exists. default is True.
                new_container=True # if True, it will create a new container if the specified container does not exist. If False, it will raise an error if the container does not exist. default is True.
            ),
            delete=DeleteSettings(
                delete_container=True # if True, it will delete the container, If False, it will only delete the blob. default is False.
            ),
        ),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.CSV),
        linked_service=AzureLinkedService(
            settings=AzureLinkedServiceSettings(
                account_name=account_name,
                access_key=account_key
            ),
            id=uuid.uuid4(),
            name="testazurepackage",
            version="0.0.1",
            description="testazurepackage",
        ),
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
        description="testazurepackage"
    )
    dataset.linked_service.connect()
    dataset.linked_service.test_connection()

    dataset.read()
    print(dataset.output)

    dataset.delete()
    dataset.read()
    print(dataset.output)


if __name__ == "__main__":
    main()
