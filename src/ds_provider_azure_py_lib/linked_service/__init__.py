"""
**File:** ``__init__.py``
**Region:** ``ds_provider_azure_py_lib/linked_service``

Azure Linked Service

This module implements a linked service for Azure storage.
Authentication supports either an account access key (shared key) or an Azure AD
service principal (client credentials).

Example (access key):
    >>> azure_linked_service = AzureLinkedService(settings=AzureLinkedServiceSettings(
    ...    account_name="your_account_name",
    ...    access_key="access key"))
    >>> azure_linked_service.connect()

Example (client credentials):
    >>> azure_linked_service = AzureLinkedService(settings=AzureLinkedServiceSettings(
    ...    account_name="your_account_name",
    ...    tenant_id="tenant id",
    ...    client_id="client id",
    ...    client_secret="client secret"))
    >>> azure_linked_service.connect()
"""

from .storage_account import AzureLinkedService, AzureLinkedServiceSettings

__all__ = [
    "AzureLinkedService",
    "AzureLinkedServiceSettings",
]
