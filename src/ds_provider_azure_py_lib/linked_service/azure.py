"""
**File:** ``azure.py``
**Region:** ``ds_provider_azure_py_lib/linked_service/azure``

Azure Linked Service

This module implements a linked service for Azure databases.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Generic, TypeVar

from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings

from ..enums import ResourceType
from .auth import AzureAuth


@dataclass(kw_only=True)
class AzureLinkedServiceSettings(LinkedServiceSettings):
    """
    The object containing the Azure linked service settings.
    """

    account_name: str
    auth: AzureAuth = field(default_factory=AzureAuth)


AzureLinkedServiceSettingsType = TypeVar(
    "AzureLinkedServiceSettingsType",
    bound=AzureLinkedServiceSettings,
)


@dataclass(kw_only=True)
class AzureLinkedService(LinkedService[AzureLinkedServiceSettingsType], Generic[AzureLinkedServiceSettingsType]):
    """
    Linked service for connecting to AzureLinkedService.
    """

    settings: AzureLinkedServiceSettingsType
    blob_service_client: BlobServiceClient | None = None
    table_service_client: TableServiceClient | None = None
    credential: Any = None

    def __post_init__(self) -> None:
        """
        Post-initialization to set up the credential.
        Returns:
            None
        """
        self.check_settings_is_set()
        self.credential = self.settings.auth.get_credential(self.settings.account_name)

    def check_settings_is_set(self) -> None:
        """
        Check if settings are set correctly.
        Returns:
            None
        Raises:
            AttributeError: If settings are not set correctly.
        """
        if not isinstance(self.settings, AzureLinkedServiceSettings):
            raise AttributeError("settings not set.")

    @property
    def type(self) -> StrEnum:
        """
        Get the type of the linked service.
        Returns:
             str
        """
        return ResourceType.STORAGE_ACCOUNT

    def connect_blob_service(self) -> None:
        """
        Connect to Azure Blob StorageAccount.
        Returns:
            BlobServiceClient
        """
        self.log.info("Connecting to Azure Blob StorageAccount...")
        account_url = f"https://{self.settings.account_name}.blob.core.windows.net/"

        self.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=self.credential,
        )

    def connect_table_service(self) -> None:
        """
        Connect to Azure Table StorageAccount.
        Returns:
             TableServiceClient
        """
        self.log.info("Connecting to Azure Table StorageAccount...")
        account_url = f"https://{self.settings.account_name}.table.core.windows.net/"
        self.table_service_client = TableServiceClient(
            endpoint=account_url,
            credential=self.credential,
        )

    def connect(self) -> tuple[BlobServiceClient, TableServiceClient]:
        """
        Connect to Azure Storage (Blob or Table).
        Returns:
            Union[BlobServiceClient, TableServiceClient]
        """
        if self.blob_service_client is None:
            self.connect_blob_service()
        if self.table_service_client is None:
            self.connect_table_service()

        return self.blob_service_client, self.table_service_client  # type: ignore

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection to Azure Storage (Blob or Table).
        Returns:
            tuple[bool, str]
        """
        try:
            blob_client, table_client = self.connect()
            return (
                True,
                f"Connection successfully tested for ({blob_client.account_name} | {table_client.account_name}) StorageAccount.",
            )
        except Exception as exc:
            self.log.error(f"Failed to test connection: {exc}", exc_info=True)
            return False, str(exc)

    def close(self) -> None:
        """
        No need to close the linked service. Just to comply with the interface.
        Returns:
            None
        """
        pass
