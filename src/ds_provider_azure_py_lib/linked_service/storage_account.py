"""
**File:** ``storage_account.py``
**Region:** ``ds_provider_azure_py_lib/linked_service/storage_account``

Azure Linked Service

This module implements a linked service for Azure Storage services (Blob and Table).
"""

from dataclasses import dataclass
from typing import Generic, TypeVar

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError

from ..enums import ResourceType


@dataclass(kw_only=True)
class AzureLinkedServiceSettings(LinkedServiceSettings):
    """
    The object containing the Azure linked service settings.
    """

    account_name: str
    access_key: str


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
    credential: AzureNamedKeyCredential | None = None

    def __post_init__(self) -> None:
        """
        Post-initialization to set up the credential.

        Returns:
            None
        Raises:
            AuthenticationError: If access_key is not provided.
        """
        self.check_settings_is_set()

        if not self.settings.access_key:
            raise AuthenticationError("Access Key is required for Azure Named Key authentication.")

        self.credential = AzureNamedKeyCredential(
            name=self.settings.account_name,
            key=self.settings.access_key,
        )

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
    def type(self) -> ResourceType:
        """
        Get the type of the linked service.

        Returns:
             ResourceType
        """
        return ResourceType.STORAGE_ACCOUNT

    def get_blob_service(self) -> BlobServiceClient:
        """
        Connect to Azure Blob StorageAccount.

        Returns:
            BlobServiceClient
        Raises:
            ConnectionError: If blob service client was not created successfully.
        """
        self.log.info("Connecting to Azure Blob StorageAccount...")
        account_url = f"https://{self.settings.account_name}.blob.core.windows.net/"

        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=self.credential,
        )
        if blob_service_client is None:
            raise ConnectionError("Failed to create BlobServiceClient.")
        return blob_service_client

    def get_table_service(self) -> TableServiceClient:
        """
        Connect to Azure Table StorageAccount.

        Returns:
             TableServiceClient
        Raises:
             ConnectionError: If table service client was not created successfully.
        """
        self.log.info("Connecting to Azure Table StorageAccount...")
        account_url = f"https://{self.settings.account_name}.table.core.windows.net/"
        table_service_client = TableServiceClient(
            endpoint=account_url,
            credential=self.credential,
        )
        if table_service_client is None:
            raise ConnectionError("Failed to create BlobServiceClient.")
        return table_service_client

    def connect(self) -> tuple[BlobServiceClient, TableServiceClient]:
        """
        Connect to Azure Storage (Blob and Table), ensuring both service clients are initialized.

        Returns:
            tuple[BlobServiceClient, TableServiceClient]: A tuple containing the blob and table service clients.
        """
        if self.blob_service_client is None:
            self.blob_service_client = self.get_blob_service()
        if self.table_service_client is None:
            self.table_service_client = self.get_table_service()

        return self.blob_service_client, self.table_service_client

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
