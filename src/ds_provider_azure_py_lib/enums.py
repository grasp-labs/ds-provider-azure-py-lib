"""
**File:** ``enums.py``
**Region:** ``ds_provider_azure_py_lib/enums``

Constants for Azure provider.

Example:
    >>> ResourceType.STORAGE_ACCOUNT
    'ds.resource.linked_service.azure_storage_account'
    >>> ResourceType.BLOB
    'ds.resource.dataset.azure_blob'
    >>> ResourceType.TABLE
    'ds.resource.dataset.azure_table'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants definitions for Azure provider.
    """

    BLOB = "ds.resource.dataset.azure_blob"
    TABLE = "ds.resource.dataset.azure_table"
    STORAGE_ACCOUNT = "ds.resource.linked_service.azure_storage_account"
