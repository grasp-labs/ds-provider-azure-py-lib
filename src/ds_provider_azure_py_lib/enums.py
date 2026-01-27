"""
**File:** ``enums.py``
**Region:** ``ds_provider_azure_py_lib/enums``

Constants for Azure provider.

Example:
    >>> ResourceType.LINKED_SERVICE
    'DS.RESOURCE.LINKED_SERVICE.AZURE'
    >>> ResourceType.BLOB
    'DS.RESOURCE.DATASET.AZURE_BLOB'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants definitions for Azure provider.
    """

    BLOB = "DS.RESOURCE.DATASET.AZURE_BLOB"
    TABLE = "DS.RESOURCE.DATASET.AZURE_TABLE"
    STORAGE_ACCOUNT = "DS.RESOURCE.LINKED_SERVICE.AZURE_STORAGE_ACCOUNT"
