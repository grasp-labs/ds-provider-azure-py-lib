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
    ...     ),
    ... )
    >>> azure_table.read()
    >>> table_data = azure_table.output
"""

import builtins
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
)
from azure.core.paging import ItemPaged
from azure.data.tables import TableClient, TableEntity, TableTransactionError, UpdateMode
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
)
from ds_resource_plugin_py_lib.common.serde.deserialize import DataDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import DataSerializer

from ..enums import ResourceType
from ..linked_service.storage_account import AzureLinkedService

logger = Logger.get_logger(__name__, package=True)

TransactionEntry = tuple[str, dict[str, Any]] | tuple[str, dict[str, Any], Mapping[str, Any]]


class AzureTableSerializer(DataSerializer):
    """
    Serialize Azure Table Storage data.
    The serializer is responsible for converting the data from
    a DataFrame into a format that can be sent to the Azure Table Storage API.
    """

    def __call__(self, obj: pd.DataFrame, **_kwargs: Any) -> Any | dict[str, Any]:
        """
        Serialize the data from a DataFrame into a dict.
        :param df: pd.DataFrame
        :param _kwargs: Additional keyword arguments
        :return: Dict
        """
        df = obj.assign(
            RowKey=obj["RowKey"].astype(str),
            PartitionKey=obj["PartitionKey"].astype(str),
        )
        entity = df.iloc[0].to_dict()
        for key, value in entity.items():
            if isinstance(value, dict):
                entity[key] = json.dumps(value)
        return entity


class AzureTableDeserializer(DataDeserializer):
    """
    Deserialize Azure Table Storage data.
    The deserializer is responsible for converting the data from
    a dict into a format that can be sent to the Azure Table Storage API.
    """

    def __call__(self, value: ItemPaged[TableEntity], **_kwargs: Any) -> Any:
        """
        Deserialize the data from a DataFrame into a dict.
        :param df: pd.DataFrame
        :param _kwargs: Additional keyword arguments
        :return: Dict
        """
        data = []
        for entity in value:
            entity_data = {key: entity[key] for key in entity}
            if "Timestamp" not in entity_data:
                entity_data["Timestamp"] = entity.metadata["timestamp"]
            data.append(entity_data)

        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)


@dataclass(kw_only=True)
class ReadSettings:
    """
    Settings specific to the read() operation.

    These settings only apply when reading data from the database
    and do not affect create(), delete(), update(), or rename() operations.
    """

    query_filter: str | None = None
    """
    An OData filter string to filter the entities returned by the read() operation.
    If None, no filter is applied and all entities are returned.

    Example: "PartitionKey eq '{self.partition_key}' and RowKey eq '{self.row_key}'"
    """


@dataclass(kw_only=True)
class DeleteSettings:
    """
    Settings specific to the delete() operation.

    These settings only apply when deleting data from the database
    and do not affect create(), read(), update(), or rename() operations.
    """

    delete_table: bool = False
    """
    If True, the entire table will be deleted when delete() is called.
    If False, only the entities specified in the input will be deleted.
    """


@dataclass(kw_only=True)
class AzureTableDatasetSettings(DatasetSettings):
    """
    Settings for Azure Table Storage dataset operations.

    The `read` settings contains read-specific configuration that only
    applies to the read() operation, not to create(), delete(), update(), etc.
    """

    table_name: str

    delete: DeleteSettings | None = None
    """
    Delete-specific settings. Only applies to the read() operation.

    If None, read() will use default behavior (No table removed on delete, just entity).
    """
    read: ReadSettings | None = None
    """
    Read-specific settings. Only applies to the read() operation.

    If None, read() will use read without filter.
    """


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

    serializer: AzureTableSerializer | None = field(
        default_factory=lambda: AzureTableSerializer(),
    )
    deserializer: AzureTableDeserializer | None = field(
        default_factory=lambda: AzureTableDeserializer(),
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
            TypeError: If the content is not a pandas DataFrame.
            ValueError: If the DataFrame is empty.
            NotImplementedError: If required columns are missing.
        """
        if not isinstance(content, pd.DataFrame):
            raise DatasetException(f"The content must be a pandas DataFrame, got {type(content)} instead.", status_code=400)

        if len(content) == 0:
            raise DatasetException("The DataFrame is empty. Cannot prepare content for Azure Table Storage.", status_code=400)

        if len(content) > 1:
            logger.warning("Are you sure you want to process multiple rows?")

        required_columns = {"PartitionKey", "RowKey"}
        if not required_columns.issubset(content.columns):
            raise DatasetException(f"The DataFrame must contain the columns: {', '.join(required_columns)}", status_code=400)

        if self.serializer is None:
            raise DatasetException("Serializer is not initialized.", status_code=400)
        return self.serializer(content)

    def _get_table_client(self) -> TableClient:
        """
        Return a TableClient for the currently configured table.
        Returns:
            TableClient
        """
        return self.linked_service.table_service_client.get_table_client(table_name=self.settings.table_name)

    def _build_transaction_from_input(self, operation: str, params: Mapping[str, Any] | None = None) -> list[TransactionEntry]:
        """
        Build a list of transaction entries from self.input.
        operation: operation name as expected by TableClient.submit_transaction, e.g. "create", "upsert", "delete"
        params: optional params dict passed as third item in tuple (when required) e.g. {"mode": UpdateMode.REPLACE}
        Returns:
        """
        transaction: list[TransactionEntry] = []
        for _, row in self.input.iterrows():
            entity_df = pd.DataFrame([row])
            try:
                entity: dict[str, Any] = self._prepare_content(entity_df)
            except DatasetException as exc:
                if operation == "create":
                    raise CreateError(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
                elif operation == "upsert":
                    raise UpdateError(message=str(exc), status_code=exc.status_code, details=self.get_details()) from exc
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

    def _create_table(self) -> None:
        """
        Creates a table in Azure Table Storage if it does not exist.

        Returns:
            None
        Raises:
            CreateError: If the table could not be created.
        """
        try:
            self.linked_service.table_service_client.create_table(
                table_name=self.settings.table_name,
            )
            logger.info(f"Table ({self.settings.table_name}) successfully created.")
        except ResourceExistsError:
            logger.debug(f"Table ({self.settings.table_name}) already exists.")
        except HttpResponseError as exc:
            raise CreateError(f"Failed to create table in Azure Table Storage: {exc!s}") from exc

    def _delete_table(self) -> None:
        """
        Deletes the table from Azure Table Storage.

        Returns:
            None
        Raises:
            DeleteError: If the table could not be deleted.
        """
        logger.debug(f"Deleting table: {self.settings.table_name}.")
        try:
            self.linked_service.table_service_client.delete_table(table_name=self.settings.table_name)
        except HttpResponseError as exc:
            logger.error(f"Failed to delete Table ({self.settings.table_name})")
            raise DeleteError(
                f"Failed to delete table in Azure Table Storage: {exc!s}", details=self.get_details()
            ) from exc  # todo change status_code, add details
        logger.info(f"Successfully deleted table:{self.settings.table_name}.")

    def read(self, **__kwargs: Any) -> None:
        """
        Read Azure Table Storage dataset.

        Args:
            __kwargs: Additional keyword arguments
        Returns:
             List[Dict]
        """
        table_client: TableClient = self.linked_service.table_service_client.get_table_client(table_name=self.settings.table_name)
        try:
            if self.settings.read and self.settings.read.query_filter:
                entities = table_client.query_entities(
                    query_filter=self.settings.read.query_filter,
                )
            else:
                entities = table_client.list_entities()
            if self.deserializer is None:
                raise ValueError("Deserializer is not initialized.")  # todo different error
            self.output = self.deserializer(entities)
        except HttpResponseError as exc:
            raise ReadError(f"Failed to read from Table Storage: {exc!s}") from exc  # todo change status_code, add details

        logger.debug(f"Read data from Table Storage: {len(self.output)} items")

    def create(self, **_kwargs: Any) -> None:
        """
        Create an entity in Azure Table Storage.

        Returns:
            None
        Raises:
            CreateError: If the entity could not be created.
        """
        if len(self.input) < 0:
            raise CreateError(
                "Input DataFrame is empty. Cannot create entity in Azure Table Storage.",
                status_code=400,
                details=self.get_details(),
            )

        self._create_table()
        transaction = self._build_transaction_from_input("create")
        try:
            self._submit_transaction(transaction, CreateError)
        except TableTransactionError as exc:
            if exc.status_code:
                raise CreateError(message=exc.message, status_code=exc.status_code, details=self.get_details()) from exc
            else:
                raise CreateError(message=exc.message) from exc
            pass
        except HttpResponseError:
            pass
        self.output = self.input

    def update(self, **_kwargs: Any) -> None:
        """
        Update an entity in Azure Table Storage.

        Returns:
            None
        Raises:
            UpdateError: If the entity could not be updated.
        """
        if len(self.input) == 0:
            raise UpdateError(
                "Input DataFrame is empty. Cannot update entity in Azure Table Storage."
            )  # todo change status_code, add details

        transaction = self._build_transaction_from_input("upsert", {"mode": UpdateMode.REPLACE})
        self._submit_transaction(transaction, UpdateError)
        self.output = self.input
        logger.info("Successfully updated entities.")

    def delete(self, **_kwargs: Any) -> None:
        """
        Delete an entity or table in Azure Table Storage.
        """
        if self.settings.delete and self.settings.delete.delete_table:
            self._delete_table()
        else:
            if len(self.input) == 0:
                raise DeleteError(
                    "Input DataFrame is empty. Cannot delete entity in Azure Table Storage.",
                    status_code=400,
                    details=self.get_details(),
                )  # todo change status_code, add details
            transaction = self._build_transaction_from_input("delete")
            logger.debug(f"Deleting entities: {len(transaction)} items")
            self._submit_transaction(transaction, DeleteError)
            logger.info("Successfully deleted entities.")

    def rename(self, **_kwargs: Any) -> NoReturn:
        raise NotImplementedError("Rename operation is not supported for Azure Table datasets")

    def close(self) -> None:
        """No need to close the linked service. Just to comply with the interface."""
        pass

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

        delete_settings = getattr(self.settings, "delete", None)
        if delete_settings is not None:
            details["delete_table"] = str(delete_settings.delete_table)

        return details
