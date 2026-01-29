"""
**File:** ``__init__.py``
**Region:** ``ds_provider_azure_py_lib/dataset``

Azure Datasets: Table and Blob

This module implements a datasets for Azure.

Example (AzureTable):
    >>> azure_table = AzureTable(
    ...     deserializer=AzureTableDeserializer(),
    ...     serializer=AzureTableSerializer(),
    ...     settings=AzureTableDatasetSettings(
    ...         table_name="users",
    ...         partition_key="partition_key",
    ...         row_key="row_key",
    ...         query_filter="additional query filter",
    ...         delete_table=False,
    ...     ),
    ...     linked_service=AzureLinkedService(
    ...         settings=AzureLinkedServiceSettings(
    ...             account_name="account name",
    ...             access_key="access key"
    ...         ),
    ...     ),
    ... )
    >>> azure_table.read()
    >>> table_data = azure_table.output

Example (AzureBlob):
    >>> azure_blob = AzureBlob(
    ...     deserializer=AzureBlobDeserializer(format=DatasetStorageFormatType.CSV),
    ...     serializer=AzureBlobSerializer(format=DatasetStorageFormatType.CSV),
    ...     settings=AzureBlobDatasetSettings(
    ...         container_name="my-container",
    ...         blob_name="path/to/example_file.csv",
    ...         prefix=None, # for multiple blobs, provide a prefix instead of blob_name
    ...     ),
    ...     linked_service=AzureLinkedService(
    ...         settings=AzureLinkedServiceSettings(
    ...             account_name="account name",
    ...             access_key="access key"
    ...         ),
    ...     ),
    ... )
    >>> azure_blob.read()
    >>> blob_data = azure_blob.output
"""

from .blob import AzureBlob, AzureBlobDatasetSettings
from .table import AzureTable, AzureTableDatasetSettings

__all__ = [
    "AzureBlob",
    "AzureBlobDatasetSettings",
    "AzureTable",
    "AzureTableDatasetSettings",
]
