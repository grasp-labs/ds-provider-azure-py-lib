"""
**File:** ``enums.py``
**Region:** ``ds_provider_azure_py_lib/enums``

Constants for Azure provider.

Example:
    >>> ResourceType.LINKED_SERVICE
    'DS.RESOURCE.LINKED_SERVICE.AZURE'
    >>> ResourceType.DATASET
    'DS.RESOURCE.DATASET.AZURE'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants definitions for Azure provider.
    """

    LINKED_SERVICE = "DS.RESOURCE.LINKED_SERVICE.AZURE"
    DATASET = "DS.RESOURCE.DATASET.AZURE"
    STORAGE_ACCOUNT = "DS.RESOURCE.LINKED_SERVICE.AZURE_STORAGE_ACCOUNT"
