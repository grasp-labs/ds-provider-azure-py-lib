"""
**File:** ``blob.py``
**Region:** ``ds_provider_azure_py_lib/dataset/blob``

Azure Blob Dataset

This module implements a blob dataset for azure.

Example:
    >>> azure_blob = AzureBlob(
    ...     deserializer=AzureBlobDeserializer(format=DatasetStorageFormatType.CSV),
    ...     serializer=AzureBlobSerializer(format=DatasetStorageFormatType.CSV),
    ...     settings=AzureBlobDatasetSettings(
    ...         container_name="my-container",
    ...         blob_name="path/to/example_file.csv",
    ...         prefix=None, # for multiple blobs, provide a prefix instead of blob_name
    ...         create=CreateSettings(
    ...            overwrite_blob_if_exists=True, # overwrite the blob that already exists or raise an error
    ...            new_container=True # create a new container or raise an error
    ...         ),
        ...         purge=PurgeSettings(
        ...            delete_container=True # confirm deletion of the container via purge()
        ...         ),
    ...     ),
    ...     linked_service=AzureLinkedService(
    ...         settings=AzureLinkedServiceSettings(
    ...             account_name="account name",
    ...             access_key="access key"
    ...         ),
    ...        id=uuid.uuid4(),
    ...        name="testazurepackage",
    ...        version="0.0.1",
    ...        description="testazurepackage",
    ...     ),
    ... id=uuid.uuid4(),
    ... name="testazurepackage",
    ... version="0.0.1",
    ... description="testazurepackage"
    ... )
    >>> azure_blob.read()
    >>> blob_data = azure_blob.output
"""

import builtins
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.core.paging import ItemPaged
from azure.storage.blob import (
    BlobClient,
    BlobProperties,
    ContainerClient,
)
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
)
from ds_resource_plugin_py_lib.common.resource.dataset.base import TabularDataset
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DeleteError,
    ReadError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType
from ..linked_service.storage_account import AzureLinkedService

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class CreateSettings:
    """
    Settings for create operations.
    """

    overwrite_blob_if_exists: bool = True
    """
    controls whether to overwrite an existing blob in case of name conflict.
    If True, the create operation will overwrite the existing blob with the new content.
    If False, the create operation will raise an error if a blob with the same name already exists.
    """
    new_container: bool = True
    """
    confirm creation of a new container if it does not exist already.
    If True, the create operation will attempt to create the container if it does not exist.
    If False, the create operation will raise an error if the container does not exist.
    """


@dataclass(kw_only=True)
class PurgeSettings:
    """
    Settings for purge operations
    """

    delete_container: bool = False
    """
    Confirm deletion of the entire container when purge() is called.
    If True, purge() will delete the container before considering blob_name.
    If False, purge() will purge the configured blob if blob_name is set.
    """


@dataclass(kw_only=True)
class AzureBlobDatasetSettings(DatasetSettings):
    """
    Settings for Azure Blob Storage dataset operations.

    Exactly one of `blob_name` or `prefix` must be provided for read();
    if specifying both, only `blob_name` will be considered.
    `prefix` is not used for create(); it can be called only with `blob_name`.
    `create` by default (if not passed) will attempt to create the container if it does not exist.
    """

    container_name: str
    blob_name: str | None = None
    prefix: str | None = None

    create: CreateSettings = field(default_factory=CreateSettings)
    purge: PurgeSettings = field(default_factory=PurgeSettings)


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

    serializer: PandasSerializer | None = field(default_factory=lambda: PandasSerializer(format=DatasetStorageFormatType.JSON))
    deserializer: PandasDeserializer | None = field(
        default_factory=lambda: PandasDeserializer(format=DatasetStorageFormatType.JSON)
    )

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
        container_client: ContainerClient = self.linked_service.connection.blob_service_client.get_container_client(
            self.settings.container_name
        )
        return container_client.list_blobs(name_starts_with=prefix)

    def _read_blob(self, blob: str) -> pd.DataFrame:
        """
        Read a specific blob in the container.

        Args:
            blob: name of the blob to read.

        Returns:
            pd.DataFrame: content of the blob as a DataFrame.
        """
        logger.debug(f"Reading blob: {blob}")
        content = pd.DataFrame()

        blob_client: BlobClient = self.linked_service.connection.blob_service_client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            stream = blob_client.download_blob().readall()
        except HttpResponseError as exc:
            logger.error(f"Failed to read blob {blob}: {exc!s}")

            raise ReadError(
                f"Failed to read blob {blob}: {exc!s}", details=self.get_details(), status_code=getattr(exc, "status_code", 500)
            ) from exc

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
        logger.debug(f"Listing blobs in with prefix: {prefix}")

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
        container_client: ContainerClient = self.linked_service.connection.blob_service_client.get_container_client(
            self.settings.container_name
        )
        try:
            container_client.create_container()
            logger.info(f"Container {self.settings.container_name} created successfully")
        except ResourceExistsError:
            logger.warning(f"Container {self.settings.container_name} already exists, but 'new_container' is set to True.")
        except HttpResponseError as exc:
            logger.error(f"Failed to create container: {exc!s}")
            raise CreateError(f"Failed to create container in Azure Blob Storage: {exc!s}", details=self.get_details()) from exc

    def _create_blob(self, stream: bytes, blob: str) -> None:
        """
        Create a specific blob in the container.

        Args:
            stream: data stream to upload to the blob.
            blob: name of the blob to create.

        Raises:
            CreateError: If the blob creation fails.

        Returns:
            None
        """
        blob_client = self.linked_service.connection.blob_service_client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            blob_client.upload_blob(
                data=stream,
                overwrite=self.settings.create.overwrite_blob_if_exists,
            )
        except HttpResponseError as exc:
            logger.error(f"Failed to create blob {blob_client.blob_name}: {exc!s}")
            raise CreateError(f"Failed to create blob {blob_client.blob_name}: {exc!s}", details=self.get_details()) from exc

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
        logger.debug(f"Deleting blob: {blob}")
        blob_client = self.linked_service.connection.blob_service_client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            blob_client.delete_blob()
        except HttpResponseError as exc:
            logger.error(f"Failed to delete blob {blob}: {exc!s}")
            raise DeleteError(f"Failed to delete blob {blob}: {exc!s}", details=self.get_details()) from exc
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
        logger.debug(f"Listing blobs in with prefix: {prefix}")
        all_deleted = True
        results = []
        for blob in self._list_blobs(prefix):
            try:
                results.append(self._delete_blob(blob.name))
            except Exception as exc:
                all_deleted = False
                logger.error(f"Failed to delete blob {blob.name}: {exc!s}")

        if not all_deleted:
            raise DeleteError("One or more blobs failed to delete.", details=self.get_details())

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
        self.output = pd.DataFrame()

        if self.settings.blob_name:
            self.output = self._read_blob(self.settings.blob_name)
        elif self.settings.prefix:
            self.output = self._read_blobs(self.settings.prefix)
        else:
            raise ReadError("Either blob name or prefix must be provided for reading.", details=self.get_details())
        logger.debug(f"Read data ({len(self.output)}) items from Blob Storage ({self.settings.container_name})")

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
            raise CreateError("Blob name must be provided for creation.", details=self.get_details(), status_code=400)

        if not self.serializer:
            raise CreateError("Data serializer must be provided for creation.", details=self.get_details(), status_code=400)

        # Empty input is a no-op per contract
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        if self.settings.create.new_container:
            self._create_container()

        stream = self.serializer(self.input)
        self._create_blob(stream, blob=self.settings.blob_name)

        logger.debug(f"Blob {self.settings.blob_name} created successfully.")
        self.output = self.input.copy()

    def update(self) -> NoReturn:
        raise NotSupportedError("Update operation is not supported for Azure Blob datasets")

    def list(self) -> NoReturn:
        raise NotSupportedError("List operation is not supported for Azure Blob datasets")

    def purge(self, **_kwargs: Any) -> None:
        """
        Purge the configured blob target.

        If `delete_container=True`, the container is deleted first.
        elif `blob_name` is configured, only that blob is deleted.
        Missing blobs or containers are treated as already purged.

        Args:
            _kwargs: Additional keyword arguments to pass to the request. (not used)

        Returns:
            None

        Raises:
            DeleteError: If the purge operation fails.
        """
        if self.settings.purge.delete_container:
            container_client: ContainerClient = self.linked_service.connection.blob_service_client.get_container_client(
                self.settings.container_name
            )
            try:
                container_client.delete_container()
                logger.info(f"Container {self.settings.container_name} deleted successfully.")
            except (HttpResponseError, ResourceNotFoundError) as exc:
                if getattr(exc, "status_code", None) == 404 or isinstance(exc, ResourceNotFoundError):
                    logger.info(f"Container {self.settings.container_name} already deleted.")
                    return
                logger.error(f"Failed to delete container {self.settings.container_name}: {exc!s}")
                raise DeleteError(
                    f"Failed to delete container {self.settings.container_name}: {exc!s}", details=self.get_details()
                ) from exc
            return
        elif self.settings.blob_name:
            self._delete_blob_if_exists(self.settings.blob_name)
            logger.info(f"Blob {self.settings.blob_name} purged successfully.")
            return

        logger.warning(f"No blob_name configured for purge on container {self.settings.container_name}; no action taken.")

    def upsert(self) -> NoReturn:
        raise NotSupportedError("Upsert operation is not supported for Azure Blob datasets")

    def delete(self, **_kwargs: Any) -> None:
        """
        Delete is not supported for Azure Blob datasets.

        Args:
            _kwargs: Additional keyword arguments to pass to the request. (not used)

        Returns:
            None

        Raises:
            NotSupportedError: Always.
        """
        raise NotSupportedError("Delete operation is not supported for Azure Blob datasets")

    def _delete_blob_if_exists(self, blob: str) -> None:
        """
        Delete a blob during purge and treat missing blobs as already purged.

        Args:
            blob: name of the blob to delete.
        """
        logger.debug(f"Purging blob: {blob}")
        blob_client = self.linked_service.connection.blob_service_client.get_blob_client(
            container=self.settings.container_name,
            blob=blob,
        )
        try:
            blob_client.delete_blob()
        except (HttpResponseError, ResourceNotFoundError) as exc:
            if getattr(exc, "status_code", None) == 404 or isinstance(exc, ResourceNotFoundError):
                logger.info(f"Blob {blob} already purged.")
                return
            logger.error(f"Failed to purge blob {blob}: {exc!s}")
            raise DeleteError(f"Failed to purge blob {blob}: {exc!s}", details=self.get_details()) from exc

    def rename(self) -> NoReturn:
        raise NotSupportedError("Rename operation is not supported for Azure Blob datasets")

    def close(self) -> None:
        """
        No need to close the linked service. Just to comply with the interface.

        Returns:
            None
        """
        pass

    @staticmethod
    def concat(dfs: builtins.list[pd.DataFrame]) -> pd.DataFrame:
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

    def get_details(self) -> dict[str, Any]:
        """
        Get details of the dataset.

        Returns:
            Dict[str, Any]: Details of the dataset.
        """
        return {
            "type": self.type.value,
            "container_name": self.settings.container_name,
            "blob_name": self.settings.blob_name,
            "prefix": self.settings.prefix,
        }
