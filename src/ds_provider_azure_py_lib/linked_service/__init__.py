"""
**File:** ``__init__.py``
**Region:** ``ds_provider_azure_py_lib/linked_service``

Azure Linked Service

This module implements a linked service for Azure databases.

Example:
    >>> azure_linked_service = AzureLinkedService(settings=AzureLinkedServiceSettings(
    ...    account_name="your_account_name",
    ...    auth=AzureAuth(
    ...        tenant_id="tenant_id",
    ...        client_id="client_id",
    ...        client_secret="client_secret",
    ...        access_key="access_key",
    ... )))
    >>> blob_service_client, table_service_client = aws_linked_service.connect()
"""

from .storage_account import AzureLinkedService, AzureLinkedServiceSettings

__all__ = [
    "AzureLinkedService",
    "AzureLinkedServiceSettings",
]
