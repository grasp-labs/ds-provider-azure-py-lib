"""
**File:** ``table.py``
**Region:** ``ds_provider_azure_py_lib/dataset/table``

Azure Dataset - Table Storage

This module implements a dataset for Azure Table Storage, allowing for CRUD operations
on table entities using pandas DataFrames for data representation.

Example:
    >>> azure_table = AzureTable(
    ...     deserializer=AzureTableDeserializer(),
    ...     serializer=AzureTableSerializer(),
    ...     settings=AzureTableDatasetSettings(
    ...         table_name="users",
    ...         partition_key="partition_key",
    ...         row_key="row_key",
    ...         query_filter="additional query filter",
    ...         delete_table=False,
    ...     ),
    ...     linked_service=AzureLinkedService(
    ...         settings=AzureLinkedServiceSettings(
    ...             account_name="account name",
    ...             access_key="access key"
    ...         ),
    ...     id=uuid.uuid4(),
    ...     name="testazurepackage",
    ...     version="0.0.1",
    ...     description="testazurepackage",
    ... ),
    ... id=uuid.uuid4(),
    ... name="testazurepackage",
    ... version="0.0.1",
    ... description="testazurepackage"
    ... )
    >>> azure_table.read()
    >>> table_data = azure_table.output
"""

import builtins
import time as t
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.data.tables import TableClient, TableTransactionError, UpdateMode
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DatasetException,
    DeleteError,
    ReadError,
    UpdateError,
    UpsertError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ValidationError

from ..enums import ResourceType
from ..linked_service.storage_account import AzureLinkedService
from ..serde import AzureTableDeserializer, AzureTableSerializer

logger = Logger.get_logger(__name__, package=True)

TransactionEntry = tuple[str, dict[str, Any]] | tuple[str, dict[str, Any], Mapping[str, Any]]


@dataclass(kw_only=True)
class ReadSettings:
    """
    Settings specific to the read() operation.

    These settings only apply when reading data from the database
    and do not affect other operations like: create(), delete(), update(), or rename().
    """

    query_filter: str | None = None
    """
    An OData-compliant string to filter the entities returned by the read() operation.
    If None, no filter is applied and all entities are returned.

    Example: "PartitionKey eq '{self.partition_key}' and RowKey eq '{self.row_key}'"
    """


@dataclass(kw_only=True)
class PurgeSettings:
    """
    Settings specific to the purge() operation.

    These settings only apply when deleting data from the database
    and do not affect other operations like:  create(), read(), update(), or rename().
    """

    delete_table: bool = False
    """
    If True, the entire table will be deleted when purge() is called.
    If False, only the table content will be deleted.
    """
    wait_after_table_deletion: bool = False
    """
    If True, waits for confirmation that the table has been deleted after the delete_table operation.
    If False, returns immediately after initiating the deletion.
    """
    delete_table_wait_retries: int = 10
    """
    Maximum number of retries to wait for table deletion confirmation before giving up.
    """
    delete_table_sleep_seconds_between_retries: int = 3
    """
    Number of seconds to sleep between retry attempts when waiting for table deletion confirmation.
    """


@dataclass(kw_only=True)
class CreateSettings:
    sleep_seconds_between_retries: int = 30
    """
    Number of seconds to sleep between retry attempts when creating a table.
    """
    retry_on_table_being_deleted: bool = True
    """
    If True, retries table creation when the table is being deleted.
    If False, raises an error immediately.
    """
    retries_number: int = 10
    """
    Maximum number of retries to attempt when creating a table.
    """


@dataclass(kw_only=True)
class AzureTableDatasetSettings(DatasetSettings):
    """
    Settings for Azure Table Storage dataset operations.

    The `read` settings contains read-specific configuration that only
    applies to the read() operation, not to create(), delete(), update(), etc.
    """

    table_name: str

    purge: PurgeSettings = field(default_factory=lambda: PurgeSettings())
    """
    Purge-specific settings. Only applies to the purge() operation.
    """
    read: ReadSettings = field(default_factory=lambda: ReadSettings())
    """
    Read-specific settings. Only applies to the read() operation.

    By default, read() will use read without filter.
    """
    create: CreateSettings = field(default_factory=lambda: CreateSettings())


AzureTableDatasetSettingsType = TypeVar(
    "AzureTableDatasetSettingsType",
    bound=AzureTableDatasetSettings,
)
AzureLinkedServiceType = TypeVar(
    "AzureLinkedServiceType",
    bound=AzureLinkedService[Any],
)


@dataclass(kw_only=True)
class AzureTable(
    TabularDataset[
        AzureLinkedServiceType,
        AzureTableDatasetSettingsType,
        AzureTableSerializer,
        AzureTableDeserializer,
    ],
    Generic[AzureLinkedServiceType, AzureTableDatasetSettingsType],
):
    linked_service: AzureLinkedServiceType
    settings: AzureTableDatasetSettingsType

    serializer: AzureTableSerializer = field(
        default_factory=AzureTableSerializer,
    )
    deserializer: AzureTableDeserializer = field(
        default_factory=AzureTableDeserializer,
    )

    @property
    def type(self) -> ResourceType:
        """
        Get the type of the Dataset.

        Returns:
            ResourceType
        """
        return ResourceType.TABLE

    def _prepare_content(self, content: pd.DataFrame) -> dict[str, Any]:
        """
        Ensure that the content is provided and is in the correct format.

        Args:
            content (pd.DataFrame): The content to prepare.

        Returns:
            dict: The prepared content.

        Raises:
            DatasetException: If the content is not a DataFrame, is empty, or does not contain required columns.
        """
        if not isinstance(content, pd.DataFrame):
            raise DatasetException(
                f"The content must be a pandas DataFrame, got {type(content)} instead.",
                status_code=400,
                details=self.get_details(),
            )

        if len(content) == 0:
            raise DatasetException(
                "The DataFrame is empty. Cannot prepare content for Azure Table Storage.",
                status_code=400,
                details=self.get_details(),
            )

        required_columns = {"PartitionKey", "RowKey"}
        if not required_columns.issubset(content.columns):
            raise ValidationError(
                f"The DataFrame must contain the columns: {', '.join(required_columns)}",
                status_code=400,
                details=self.get_details(),
            )

        if self.serializer is None:
            raise DatasetException("Serializer is not initialized.", status_code=400, details=self.get_details())
        return self.serializer(content)

    def _get_table_client(self) -> TableClient:
        """
        Return a TableClient for the currently configured table.

        Returns:
            TableClient
        """
        return self.linked_service.connection.table_service_client.get_table_client(table_name=self.settings.table_name)

    def _build_transaction_from_input(self, operation: str, params: Mapping[str, Any] | None = None) -> list[TransactionEntry]:
        """
        Build a list of transaction entries from self.input.
        operation: operation name as expected by TableClient.submit_transaction, e.g. "create", "upsert", "delete"

        Args:
            operation (str): The operation to perform.
            params: optional params dict passed as third item in tuple (when required) e.g. {"mode": UpdateMode.REPLACE}

        Returns:
            list[TransactionEntry]

        Raises:
            CreateError: If there is an error preparing content for creation.
            UpdateError: If there is an error preparing content for update.
            DeleteError: If there is an error preparing content for deletion.
            DatasetException: If there is a general error preparing content.
        """
        transaction: list[TransactionEntry] = []
        for _, row in self.input.iterrows():
            entity_df = pd.DataFrame([row])
            try:
                entity: dict[str, Any] = self._prepare_content(entity_df)
            except ValidationError as exc:
                raise exc
            except DatasetException as exc:
                if operation == "create":
                    raise CreateError(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
                elif operation == "update":
                    raise UpdateError(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
                elif operation == "upsert":
                    raise UpsertError(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
                elif operation == "delete":
                    raise DeleteError(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
                else:
                    raise DatasetException(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
            if params is not None:
                transaction.append((operation, entity, params))
            else:
                transaction.append((operation, entity))
        return transaction

    def _submit_transaction(self, transaction: Iterable[TransactionEntry], error_cls: builtins.type[DatasetException]) -> None:
        """
        Submit transaction and map TableTransactionError to provided error_type.

        Args:
            transaction (Iterable[TransactionEntry]): The transaction to submit.
            error_cls (builtins.type[DatasetException]): The exception class to raise on error.

        Raises:
            error_cls: An error submitting the transaction.
        """
        table_client = self._get_table_client()
        try:
            if not transaction:
                return
            table_client.submit_transaction(transaction)
        except (TableTransactionError, HttpResponseError) as exc:
            logger.error(f"{error_cls.__class__.__name__}: {exc.message}")
            if exc.status_code:
                raise error_cls(message=exc.message, status_code=exc.status_code, details=self.get_details()) from exc
            else:
                raise error_cls(message=exc.message, details=self.get_details()) from exc

    def _delete_table(self) -> None:
        """
        Deletes the entire table from Azure Table Storage.

        Returns:
            None

        Raises:
            DeleteError: If the table could not be deleted.
        """
        logger.debug(f"Deleting table: {self.settings.table_name}.")
        try:
            self.linked_service.connection.table_service_client.delete_table(table_name=self.settings.table_name)
            logger.info(f"Successfully deleted table: {self.settings.table_name}.")
        except HttpResponseError as exc:
            logger.error(f"Failed to delete table ({self.settings.table_name})")
            raise DeleteError(f"Failed to delete table in Azure Table Storage: {exc!s}", details=self.get_details()) from exc

    def _create_table(self) -> None:
        """
        Creates a table in Azure Table Storage if it does not exist.

        Returns:
            None

        Raises:
            CreateError: If the table could not be created due to an error other than it already existing.
        """
        try:
            self.linked_service.connection.table_service_client.create_table(
                table_name=self.settings.table_name,
            )
            logger.info(f"Table ({self.settings.table_name}) successfully created.")
        except (ResourceExistsError, HttpResponseError) as exc:
            if self.settings.create.retry_on_table_being_deleted and getattr(exc, "error_code", None) == "TableBeingDeleted":
                self._retry_create()
            else:
                raise CreateError(f"Failed to create table in Azure Table Storage: {exc!s}", details=self.get_details()) from exc

    def read(self, **_kwargs: Any) -> None:
        """
        Read Azure Table Storage dataset.

        Args:
            _kwargs: Additional keyword arguments

        Returns:
            None

        Raises:
            ReadError: If there is an error reading from Azure Table Storage.
        """
        table_client: TableClient = self.linked_service.connection.table_service_client.get_table_client(
            table_name=self.settings.table_name
        )
        try:
            if self.settings.read.query_filter:
                entities = table_client.query_entities(
                    query_filter=self.settings.read.query_filter,
                )
            else:
                entities = table_client.list_entities()
            if self.deserializer is None:
                raise ReadError("Deserializer is not initialized.", status_code=400, details=self.get_details())
            self.output = self.deserializer(entities)
        except HttpResponseError as exc:
            raise ReadError(f"Failed to read from Table Storage: {exc!s}", details=self.get_details(), status_code=500) from exc

        logger.debug(f"Read data from Table Storage: {len(self.output)} items")

    def create(self, **_kwargs: Any) -> None:
        """
        Create an entity in Azure Table Storage.

        Returns:
            None

        Raises:
            CreateError: If the entity could not be created.
        """
        # Empty input is a no-op per contract
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        self._create_table()
        transaction = self._build_transaction_from_input("create")
        try:
            self._submit_transaction(transaction, CreateError)
        except TableTransactionError as exc:
            raise CreateError(
                message=exc.message, status_code=getattr(exc, "status_code", 500), details=self.get_details()
            ) from exc

        except HttpResponseError as exc:
            raise CreateError("Failed to create entity in Azure Table Storage.", details=self.get_details()) from exc
        self.output = self.input.copy()

    def update(self, **_kwargs: Any) -> None:
        """
        Update an entity in Azure Table Storage.

        Returns:
            None
        """
        # Empty input is a no-op per contract
        if self.input is None or len(self.input) == 0:
            logger.debug("Input DataFrame is empty. No entities to update in Azure Table Storage.")
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        transaction = self._build_transaction_from_input("update", {"mode": UpdateMode.MERGE})
        self._submit_transaction(transaction, UpdateError)
        logger.info("Successfully updated entities.")
        self.output = self.input.copy()

    def delete(self, **_kwargs: Any) -> None:
        """
        Delete specific entities from Azure Table Storage.

        Only entities specified in `self.input` are deleted, matched by PartitionKey and RowKey.

        Args:
            _kwargs: Additional keyword arguments

        Returns:
            None

        Raises:
            DeleteError: If there is an error deleting from Azure Table Storage.
        """
        # For entity deletion, empty input is a no-op per contract
        if self.input is None or len(self.input) == 0:
            logger.debug("Input DataFrame is empty. No entities to delete in Azure Table.")
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        transaction = self._build_transaction_from_input("delete")
        logger.debug(f"Deleting entities: {len(transaction)} items")
        try:
            self._submit_transaction(transaction, DeleteError)
        except DeleteError as exc:
            if exc.status_code == 404:
                logger.warning("Entity not found during delete operation.")
            else:
                raise DeleteError(
                    message=exc.message, status_code=getattr(exc, "status_code", 500), details=self.get_details()
                ) from exc
        logger.info("Successfully deleted entities.")
        self.output = self.input.copy()

    def rename(self) -> NoReturn:
        raise NotSupportedError("Rename operation is not supported for Azure Table datasets")

    def close(self) -> None:
        """
        No need to close the linked service. Just to comply with the interface.

        Returns:
            None
        """
        pass

    def list(self) -> NoReturn:
        raise NotSupportedError("List operation is not supported for Azure Table datasets")

    def purge(self, **_kwargs: Any) -> None:
        """
        Purge all entities from the table or drop the entire table.

        If `delete_table=True` in settings, deletes the entire table.
        Otherwise, deletes all entities from the table, leaving it empty.

        Returns:
            None

        Raises:
            DeleteError: If there is an error purging from Azure Table Storage.
        """
        if self.settings.purge.delete_table:
            # Delete the entire table
            self._delete_table()
            self._wait_for_table_deletion()
        else:
            # Delete all entities from the table
            try:
                table_client = self._get_table_client()
                # Fetch all entities and delete them
                entities_to_delete = list(table_client.list_entities())
                if entities_to_delete:
                    # Build and submit delete transaction for all entities
                    transaction: list[TransactionEntry] = []
                    for entity in entities_to_delete:
                        # Entity must have PartitionKey and RowKey
                        entity_dict = dict(entity)
                        transaction.append(("delete", entity_dict))

                    self._submit_transaction(transaction, DeleteError)
                logger.info(f"Successfully purged all entities from table {self.settings.table_name}.")
            except HttpResponseError as exc:
                if exc.status_code == 404:
                    logger.warning(f"Table {self.settings.table_name} not found during purge. Treating as already purged.")
                else:
                    raise DeleteError(
                        f"Failed to purge entities from table {self.settings.table_name}: {exc!s}",
                        details=self.get_details(),
                    ) from exc

    def upsert(self, **_kwargs: Any) -> None:
        # Empty input is a no-op per contract
        if self.input is None or len(self.input) == 0:
            logger.debug("Input DataFrame is empty. No entities to update in Azure Table Storage.")
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        transaction = self._build_transaction_from_input("upsert", {"mode": UpdateMode.REPLACE})
        self._submit_transaction(transaction, UpsertError)
        logger.info("Successfully updated entities.")
        self.output = self.input.copy()

    def get_details(self) -> dict[str, Any]:
        """
        Get details about the dataset.

        Returns:
            dict[str, Any]
        """
        details: dict[str, Any] = {
            "table_name": self.settings.table_name,
            "dataset_type": self.type.value,
        }

        read_settings = getattr(self.settings, "read", None)
        if read_settings is not None and read_settings.query_filter is not None:
            details["query_filter"] = read_settings.query_filter

        purge_settings = getattr(self.settings, "purge", None)
        if purge_settings is not None:
            details["delete_table"] = str(purge_settings.delete_table)

        return details

    def _wait_for_table_deletion(self) -> None:
        if not self.settings.purge.wait_after_table_deletion:
            return

        for _ in range(self.settings.purge.delete_table_wait_retries):
            table_client = self._get_table_client()
            entities = table_client.list_entities()
            try:
                for entity in entities:
                    _ = entity
            except ResourceNotFoundError as exc:
                if getattr(exc, "error_code", None) == "TableNotFound":
                    logger.info("Confirmed table deletion.")
                    return
            t.sleep(self.settings.purge.delete_table_sleep_seconds_between_retries)
        logger.debug("Table deletion not confirmed after waiting.")

    def _retry_create(self) -> None:
        for _ in range(self.settings.create.retries_number):
            try:
                self.linked_service.connection.table_service_client.create_table(table_name=self.settings.table_name)
                logger.info(f"Table ({self.settings.table_name}) successfully created after waiting for deletion.")
                return
            except (ResourceExistsError, HttpResponseError) as inner_exc:
                if getattr(inner_exc, "error_code", None) == "TableBeingDeleted":
                    logger.debug(f"Table ({self.settings.table_name}) is still being deleted. Waiting before retrying...")
                    t.sleep(self.settings.create.sleep_seconds_between_retries)
                elif isinstance(inner_exc, ResourceExistsError):
                    raise CreateError(
                        f"Failed to create table in Azure Table Storage. Table ({self.settings.table_name}) already exists.",
                        details=self.get_details(),
                    ) from inner_exc
                else:
                    logger.error(f"Failed to create table ({self.settings.table_name}) after deletion: {inner_exc.message}")
                    raise CreateError(
                        f"Failed to create table in Azure Table Storage after waiting for deletion: {inner_exc!s}",
                        details=self.get_details(),
                    ) from inner_exc

        raise CreateError(
            f"Failed to create table in Azure Table Storage after {self.settings.create.retries_number} retries"
            f" waiting for deletion.",
            details=self.get_details(),
        )
