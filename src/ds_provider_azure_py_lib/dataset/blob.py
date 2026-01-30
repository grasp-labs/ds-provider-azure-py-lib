"""
**File:** ``blob.py``
**Region:** ``ds_provider_azure_py_lib/dataset/blob``

Azure Blob Dataset

This module implements a blob ataset for azure.

Example:
    >>> azure_blob = AzureBlob(
    ...     deserializer=AzureBlobDeserializer(format=DatasetStorageFormatType.CSV),
    ...     serializer=AzureBlobSerializer(format=DatasetStorageFormatType.CSV),
    ...     settings=AzureBlobDatasetSettings(
    ...         container_name="my-container",
    ...         blob_name="path/to/example_file.csv",
    ...         prefix=None, # for multiple blobs, provide a prefix instead of blob_name
    ...     ),
    ...     linked_service=AzureLinkedService(
    ...         settings=AzureLinkedServiceSettings(
    ...             account_name="account name",
    ...             access_key="access key"
    ...         ),
    ...     ),
    ... )
    >>> azure_blob.read()
    >>> blob_data = azure_blob.output
"""

from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
)
from azure.core.paging import ItemPaged
from azure.storage.blob import (
    BlobClient,
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
)
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
)
from ds_resource_plugin_py_lib.common.resource.dataset.base import TabularDataset
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, DeleteError, ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import InvalidLinkedServiceTypeError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType
from ..linked_service.storage_account import AzureLinkedService

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class AzureBlobDatasetSettings(DatasetSettings):
    """
    Settings for Azure Blob Storage dataset operations.

    Exactly one of `blob_name` or `prefix` must be provided for read()/delete();
    if specifying both, only blob_name will be considered.

    `prefix` is not used for create(); create() can be used only with `blob_name`.
    """

    container_name: str
    blob_name: str | None = None
    prefix: str | None = None


AzureBlobDatasetSettingsType = TypeVar(
    "AzureBlobDatasetSettingsType",
    bound=AzureBlobDatasetSettings,
)
AzureLinkedServiceType = TypeVar(
    "AzureLinkedServiceType",
    bound=AzureLinkedService[Any],
)


@dataclass(kw_only=True)
class AzureBlob(
    TabularDataset[
        AzureLinkedServiceType,
        AzureBlobDatasetSettingsType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[AzureLinkedServiceType, AzureBlobDatasetSettingsType],
):
    linked_service: AzureLinkedServiceType
    settings: AzureBlobDatasetSettingsType
    client: BlobServiceClient = field(init=False)

    serializer: PandasSerializer
    deserializer: PandasDeserializer

    def __post_init__(self) -> None:
        if not isinstance(self.linked_service, AzureLinkedService):
            raise TypeError(f"Expected linked_service to be of type AzureLinkedService, got {type(self.linked_service)}")

        client, _ = self.linked_service.connect()
        if not isinstance(client, BlobServiceClient):
            raise InvalidLinkedServiceTypeError("Linked Service must use service 'blob' to be used in Azure Blob Dataset.")
        self.client = client

    @property
    def type(self) -> ResourceType:
        """
        Get the type of the dataset.

        Returns:
             ResourceType
        """
        return ResourceType.BLOB

    def _list_blobs(self, prefix: str) -> ItemPaged[BlobProperties]:
        """
        List all blobs in the container with a specific prefix.

        Args:
            prefix: a string prefix to match one or multiple blobs.
        Returns:
            ItemPaged[BlobProperties]: An iterable of BlobProperties matching the prefix.
        """
        container_client: ContainerClient = self.client.get_container_client(self.settings.container_name)
        return container_client.list_blobs(name_starts_with=prefix)

    def _read_blob(self, blob: str) -> pd.DataFrame:
        """
        Read a specific blob in the container.

        Args:
            blob: name of the blob to read.
        Returns:
            pd.DataFrame: content of the blob as a DataFrame.
        """
        logger.info(f"Reading blob: {self.settings.blob_name}")
        content = pd.DataFrame()

        blob_client: BlobClient = self.client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            stream = blob_client.download_blob().readall()
        except HttpResponseError as exc:
            logger.error(f"Failed to read blob {blob}: {exc!s}")
            raise ReadError(f"Failed to read blob {blob}: {exc!s}") from exc

        if stream and self.deserializer:
            content = self.deserializer(stream)

        logger.info(f"Blob {blob} read successfully.")
        return content

    def _read_blobs(self, prefix: str) -> pd.DataFrame:
        """
        Read all blobs in the container with a specific prefix.

        Args:
            prefix: a string prefix to match one or multiple blobs.
        Returns:
             pd.DataFrame: Content of all blobs concatenated as a DataFrame.
        """

        logger.info(f"Listing blobs in with prefix: {prefix}")

        content = self.concat([self._read_blob(blob.name) for blob in self._list_blobs(prefix)])
        return content

    def _create_container(self) -> None:
        """
        Create a container in the Azure Blob Storage.

        Raises:
            CreateError: If the container creation fails.
        Returns:
            None
        """
        container_client: ContainerClient = self.client.get_container_client(self.settings.container_name)
        try:
            container_client.create_container()
            logger.info(f"Container {self.settings.container_name} created successfully)")
        except ResourceExistsError:
            logger.warning(f"Container {self.settings.container_name} already exists")
        except HttpResponseError as exc:
            logger.error(f"Failed to create container: {exc!s}")
            raise CreateError(f"Failed to create container in Azure Blob Storage: {exc!s}") from exc

    def _create_blob(self, stream: str, blob: str) -> None:
        """
        Create a specific blob in the container.

        Args:
            blob: name of the blob to create.
            stream: data stream to upload to the blob.
        Raises:
            CreateError: If the blob creation fails.
        Returns:
            None
        """
        blob_client = self.client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            blob_client.upload_blob(
                data=stream,
                overwrite=True,
            )
        except HttpResponseError as exc:
            logger.error(f"Failed to create blob {blob_client.blob_name}: {exc!s}")
            raise CreateError(f"Failed to create blob {blob_client.blob_name}: {exc!s}") from exc

    def _delete_blob(self, blob: str) -> pd.DataFrame:
        """
        Delete a specific blob in the container.

        Args:
            blob: name of the blob to delete.
        Returns:
            pd.DataFrame: Empty DataFrame upon successful deletion.
        Raises:
            DeleteError: If the blob deletion fails.
        """
        logger.info(f"Deleting blob: {blob}")
        blob_client = self.client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            blob_client.delete_blob()
            # todo check if we can remove many blobs at once
        except HttpResponseError as exc:
            logger.error(f"Failed to delete blob {blob}: {exc!s}")
            raise DeleteError(f"Failed to delete blob {blob}: {exc!s}") from exc
        logger.info(f"Blob {blob} deleted successfully.")
        return pd.DataFrame()

    def _delete_blobs(self, prefix: str) -> pd.DataFrame:
        """
        Delete all blobs in the container with a specific prefix.

        Args:
            prefix: a string prefix to match one or multiple blobs.
        Returns:
             pd.DataFrame: Empty DataFrame upon successful deletion of all blobs.
        Raises:
            DeleteError: If one or more blob deletions fail.
        """
        logger.info(f"Listing blobs in with prefix: {prefix}")
        all_deleted = True
        results = []
        for blob in self._list_blobs(prefix):
            try:
                results.append(self._delete_blob(blob.name))
            except Exception as exc:
                all_deleted = False
                logger.error(f"Failed to delete blob {blob.name}: {exc!s}")

        if not all_deleted:
            raise DeleteError("One or more blobs failed to delete.")

        logger.info("Data deleted successfully.")
        content = self.concat(results)
        return content

    def read(self, **_kwargs: Any) -> None:
        """
        Read Azure Blob Storage dataset.

        Args:
            _kwargs: Additional keyword arguments to pass to the request.
        Returns:
            None
        Raises:
            ReadError: If reading the blob(s) fails.
        """
        if self.settings.blob_name:
            self.output = self._read_blob(self.settings.blob_name)
        elif self.settings.prefix:
            self.output = self._read_blobs(self.settings.prefix)
        else:
            raise ReadError("Either blob name or prefix must be provided for reading.")
        logger.info(f"Read data ({len(self.output)}) items from Blob Storage ({self.settings.container_name})")

    def create(self, **_kwargs: Any) -> None:
        """
        Create a blob in the container

        Args:
            _kwargs: Additional keyword arguments to pass to the request. (not used)
        Returns:
            None
        Raises:
            CreateError: If the blob creation fails.
        """

        if not self.settings.blob_name:
            raise CreateError("Blob name must be provided for creation.")

        if not self.serializer:
            raise CreateError("Data serializer must be provided for creation.")

        stream = self.serializer(self.input)

        # Create Container if not exist
        self._create_container()

        self._create_blob(stream, blob=self.settings.blob_name)

        logger.info(f"Blob {self.settings.blob_name} created successfully.")

    def update(self, **_kwargs: Any) -> NoReturn:
        raise NotImplementedError("Update operation is not supported for Azure Blob datasets")

    def delete(self, **_kwargs: Any) -> None:
        """
        Deletes a specific blob in the container.

        Args:
            _kwargs: Additional keyword arguments to pass to the request. (not used)
        Returns:
            None
        Raises:
            DeleteError: If the blob deletion fails.
        """
        if self.settings.blob_name:
            blob_name = self.settings.blob_name
            self._delete_blob(blob_name)
            logger.info(f"Blob {blob_name} deleted successfully.")
        elif self.settings.prefix:
            prefix = self.settings.prefix
            self._delete_blobs(prefix)
            logger.info(
                f"Blobs with prefix '{prefix}' deleted successfully from container "
                f"'{self.settings.container_name}'."
            )
        else:
            raise DeleteError("Either blob name or prefix must be provided for deletion.")
    def rename(self, **_kwargs: Any) -> NoReturn:
        raise NotImplementedError("Rename operation is not supported for Azure Blob datasets")

    def close(self) -> None:
        """
        No need to close the linked service. Just to comply with the interface.

        Returns:
            None
        """
        pass

    @staticmethod
    def concat(dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """
        concatenate a list of dataframes into a single dataframe.

        Args:
            dfs: DataFrames to concatenate.
        Returns:
             DataFrame: Concatenated DataFrame or empty DataFrame if input list is empty.
        """
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)
