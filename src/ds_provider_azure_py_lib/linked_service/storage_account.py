"""
**File:** ``storage_account.py``
**Region:** ``ds_provider_azure_py_lib/linked_service/storage_account``

Azure Linked Service

This module implements a linked service for Azure Storage services (Blob and Table).
Example:
>>>linked_service = AzureLinkedService(
...        settings=AzureLinkedServiceSettings(
...            account_name="account name",
...            access_key="account key",
...        ),
...        id=uuid.uuid4(),
...        name="testazurepackage",
...        version="0.0.1",
...        description="testazurepackage"
...    )
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    ConnectionError,
)

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class AzureLinkedServiceSettings(LinkedServiceSettings):
    """
    The object containing the Azure linked service settings.
    """

    account_name: str
    access_key: str = field(metadata={"mask": True})


AzureLinkedServiceSettingsType = TypeVar(
    "AzureLinkedServiceSettingsType",
    bound=AzureLinkedServiceSettings,
)


@dataclass(kw_only=True)
class AzureLinkedServiceConnection:
    """
    The object containing the Azure linked service connection clients.
    """

    blob_service_client: BlobServiceClient
    table_service_client: TableServiceClient


@dataclass(kw_only=True)
class AzureLinkedService(LinkedService[AzureLinkedServiceSettingsType], Generic[AzureLinkedServiceSettingsType]):
    """
    Linked service for connecting to AzureLinkedService.
    """

    settings: AzureLinkedServiceSettingsType
    _blob_service_client: BlobServiceClient | None = None
    _table_service_client: TableServiceClient | None = None
    _credential: AzureNamedKeyCredential | None = field(default=None, metadata={"serialize": False})

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
    def connection(self) -> AzureLinkedServiceConnection:
        """
        Get the connection object for Azure StorageAccount.

        Returns:
            AzureLinkedServiceConnection
        """
        if self._table_service_client is None or self._blob_service_client is None:
            raise ConnectionError(
                message="Connection has not been established. Call connect() first.",
                details={"provider": self.type.value},
            )
        return AzureLinkedServiceConnection(
            blob_service_client=self._blob_service_client, table_service_client=self._table_service_client
        )

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
        """
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
        """
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

    def __enter__(self) -> "AzureLinkedService[AzureLinkedServiceSettingsType]":
        """
        Enter context manager.

        Returns:
            AzureLinkedService: Returns self for use in with statement.
        """
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """
        Exit context manager and close the connection.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.

        Returns:
            None
        """
        self.close()
