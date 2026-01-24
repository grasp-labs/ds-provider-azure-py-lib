"""
**File:** ``__init__.py``
**Region:** ``ds_provider_azure_py_lib/dataset``

Azure Datasets: Table and Blob

This module implements a datasets for Azure.

Example:
    table_name: str
    partition_key: str | None = None
    row_key: str | None = None
    query_filter: str | None = None
    delete_table: bool = False
    >>> from ds_provider_azure_py_lib.dataset.table import AzureTableDeserializer, AzureTableSerializer
    >>> from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings
    >>> from ds_provider_azure_py_lib.dataset import AzureTable, AzureTableDatasetSettings
    >>> azure_table = AzureTable(
    ...     deserializer=AzureTableDeserializer(),
    ...     serializer=AzureTableSerializer(),
    ...     settings=AzureTableDatasetSettings(
    ...         table_name="users",
    ...         partition_key="partition_key",
    ...         row_key="row_key",
    ...         query_filter="query_filter",
    ...         delete_table=False,
    ...     ),
    ...     linked_service=AzureLinkedService(
    ...         settings=AzureLinkedServiceSettings(
    ...             account_name=...,
    ...             uri=...
    ...         ),
    ...     ),
    ... )
    >>> azure_table.read()
    >>> data = azure_table.content
"""  # todo add example for table and blob to docstring above

from .table import AzureTable, AzureTableDatasetSettings

# todo from .blob import AzureBlob, AzureBlobDatasetSettings


__all__ = [
    "AzureTable",
    "AzureTableDatasetSettings",
    # "AzureBlob",
    # "AzureBlobDatasetSettings",
]
