"""
**File:** ``storage_account.py``
**Region:** ``ds_provider_azure_py_lib/linked_service/storage_account``

Azure Linked Service

This module implements a linked service for Azure Storage services (Blob and Table).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


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
    _blob_service_client: BlobServiceClient | None = None
    _table_service_client: TableServiceClient | None = None
    _credential: AzureNamedKeyCredential | None = None

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

        if not self.settings.access_key:
            raise AuthenticationError("Access Key is required for Azure Named Key authentication.")
        if not self.settings.account_name:
            raise AuthenticationError("Account Name is required for Azure Named Key authentication.")

    @property
    def type(self) -> ResourceType:
        """
        Get the type of the linked service.

        Returns:
             ResourceType
        """
        return ResourceType.STORAGE_ACCOUNT

    @property
    def blob_service_client(self) -> BlobServiceClient:
        """
        Get the BlobServiceClient instance.

        Returns:
            BlobServiceClient
        Raises:
            ConnectionError: If blob service client is not connected.
        """
        if not self._blob_service_client:
            raise ConnectionError("Blob service client is not connected. Call connect() first.")
        return self._blob_service_client

    @property
    def table_service_client(self) -> TableServiceClient:
        """
        Get the TableServiceClient instance.

        Returns:
            TableServiceClient
        Raises:
            ConnectionError: If table service client is not connected.
        """
        if not self._table_service_client:
            raise ConnectionError("Table service client is not connected. Call connect() first.")
        return self._table_service_client

    def get_blob_service(self) -> BlobServiceClient:
        """
        Connect to Azure Blob StorageAccount.

        Returns:
            BlobServiceClient
        Raises:
            ConnectionError: If blob service client was not created successfully.
        """
        logger.debug("Connecting to Azure Blob StorageAccount...")
        account_url = f"https://{self.settings.account_name}.blob.core.windows.net/"

        return BlobServiceClient(
            account_url=account_url,
            credential=self._credential,
        )

    def get_table_service(self) -> TableServiceClient:
        """
        Connect to Azure Table StorageAccount.

        Returns:
             TableServiceClient
        Raises:
             ConnectionError: If table service client was not created successfully.
        """
        logger.debug("Connecting to Azure Table StorageAccount...")
        account_url = f"https://{self.settings.account_name}.table.core.windows.net/"

        return TableServiceClient(
            endpoint=account_url,
            credential=self._credential,
        )

    def connect(self) -> None:
        """
        Connect to Azure Storage (Blob and Table), ensuring both service clients are initialized.

        Returns:
            None
        """
        self.check_settings_is_set()
        self._credential = AzureNamedKeyCredential(
            name=self.settings.account_name,
            key=self.settings.access_key,
        )
        self._blob_service_client = self.get_blob_service()
        self._table_service_client = self.get_table_service()
        logger.debug("Connected to Azure StorageAccount.")

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection to Azure Storage (Blob or Table).

        Returns:
            tuple[bool, str]
        """
        try:
            if not self._blob_service_client or not self._table_service_client:
                self.connect()
            _ = self.blob_service_client.list_containers()
            _ = self.table_service_client.list_tables()
            logger.debug("Tested connection to Azure StorageAccount successfully.")
            return True, "Connection successful."
        except Exception as exc:
            logger.error(f"Failed to test connection: {exc}", exc_info=True)
            return False, str(exc)

    def close(self) -> None:
        """
        No need to close the linked service. Just to comply with the interface.

        Returns:
            None
        """
        pass
