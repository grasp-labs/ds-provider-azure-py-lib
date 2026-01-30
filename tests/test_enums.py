"""
**File:** ``test_enums.py``
**Region:** ``tests/test_enums``

ResourceType enum tests.

Covers:
- Enum value definitions and string representations.
- Enum membership and comparison operations.
"""

from __future__ import annotations

from ds_provider_azure_py_lib.enums import ResourceType


def test_resource_type_storage_account_value() -> None:
    """
    It exposes the correct linked service type value.
    """
    assert ResourceType.STORAGE_ACCOUNT == "DS.RESOURCE.LINKED_SERVICE.AZURE_STORAGE_ACCOUNT"
    assert isinstance(ResourceType.STORAGE_ACCOUNT, str)


def test_resource_type_blob_value() -> None:
    """
    It exposes the correct blob dataset type value.
    """
    assert ResourceType.BLOB == "DS.RESOURCE.DATASET.AZURE_BLOB"
    assert isinstance(ResourceType.BLOB, str)


def test_resource_type_table_value() -> None:
    """
    It exposes the correct table dataset type value.
    """
    assert ResourceType.TABLE == "DS.RESOURCE.DATASET.AZURE_TABLE"
    assert isinstance(ResourceType.TABLE, str)


def test_resource_type_enum_membership() -> None:
    """
    It allows checking enum membership.
    """
    assert ResourceType.STORAGE_ACCOUNT in ResourceType
    assert ResourceType.BLOB in ResourceType
    assert ResourceType.TABLE in ResourceType


def test_resource_type_enum_comparison() -> None:
    """
    It supports equality comparison with strings.
    """
    assert ResourceType.STORAGE_ACCOUNT == "DS.RESOURCE.LINKED_SERVICE.AZURE_STORAGE_ACCOUNT"
    assert ResourceType.BLOB == "DS.RESOURCE.DATASET.AZURE_BLOB"
    assert ResourceType.TABLE == "DS.RESOURCE.DATASET.AZURE_TABLE"
    assert ResourceType.BLOB != ResourceType.TABLE
    assert ResourceType.STORAGE_ACCOUNT != ResourceType.TABLE
