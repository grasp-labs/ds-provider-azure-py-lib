"""
**File:** ``enums.py``
**Region:** ``ds_provider_azure_py_lib/enums``

Constants for Azure provider.

Example:
    >>> ResourceType.STORAGE_ACCOUNT
    'ds.resource.linked-service.azure-storage-account'
    >>> ResourceType.BLOB
    'ds.resource.dataset.azure-blob'
    >>> ResourceType.TABLE
    'ds.resource.dataset.azure-table'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants definitions for Azure provider.
    """

    BLOB = "ds.resource.dataset.azure-blob"
    TABLE = "ds.resource.dataset.azure-table"
    STORAGE_ACCOUNT = "ds.resource.linked-service.azure-storage-account"
