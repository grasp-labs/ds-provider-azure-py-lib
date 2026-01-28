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
    >>> table_data = azure_table.content
"""

import json
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.core.paging import ItemPaged
from azure.data.tables import TableClient, TableEntity, TableServiceClient, UpdateMode
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, DeleteError, ReadError, UpdateError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import InvalidLinkedServiceTypeError
from ds_resource_plugin_py_lib.common.serde.deserialize import DataDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import DataSerializer

from ..enums import ResourceType
from ..linked_service.storage_account import AzureLinkedService


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
class AzureTableDatasetSettings(DatasetSettings):
    """
    Settings for Azure Table Storage dataset operations.

    The `read` settings contains read-specific configuration that only
    applies to the read() operation, not to create(), delete(), update(), etc.
    """

    table_name: str
    partition_key: str | None = None
    row_key: str | None = None
    query_filter: str | None = None
    delete_table: bool = False

    def __post_init__(self) -> None:
        filters = []
        if self.partition_key:
            filters.append(f"PartitionKey eq '{self.partition_key}'")
        if self.row_key:
            filters.append(f"RowKey eq '{self.row_key}'")
        if self.query_filter:
            filters.append(self.query_filter)

        self.query_filter = " and ".join(filters) if filters else None


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
    client: TableServiceClient = field(init=False)

    serializer: AzureTableSerializer | None = field(
        default_factory=lambda: AzureTableSerializer(),
    )
    deserializer: AzureTableDeserializer | None = field(
        default_factory=lambda: AzureTableDeserializer(),
    )

    def __post_init__(self) -> None:
        if not isinstance(self.linked_service, AzureLinkedService):
            raise InvalidLinkedServiceTypeError("Linked service must be of type Azure Storage to be used in Table Dataset.")
        _, self.client = self.linked_service.connect()
        if not isinstance(self.client, TableServiceClient):
            raise InvalidLinkedServiceTypeError("Linked Service must use service 'table' to be used in Table Dataset.")

    @property
    def type(self) -> ResourceType:
        """
        Get the type of the Dataset.
        Returns:
            ResourceType
        """
        return ResourceType.BLOB

    def _prepare_content(self) -> dict[str, Any]:
        """
        Ensure that the content is provided and is in the correct format.
        """
        if len(self.content) != 1:
            raise NotImplementedError("Azure Table Storage does not support batching.")

        required_columns = {"PartitionKey", "RowKey"}
        if not required_columns.issubset(self.content.columns):
            raise NotImplementedError(f"The DataFrame must contain the columns: {', '.join(required_columns)}")

        if self.serializer is None:
            raise ValueError("Serializer is not initialized.")
        return self.serializer(self.content)

    def _create_table(self) -> None:
        """
        Creates a table in Azure Table Storage if it does not exist.
        :return: bool
        """
        try:
            self.client.create_table(
                table_name=self.settings.table_name,
            )
            self.log.info(f"Table ({self.settings.table_name}) successfully created.")
        except ResourceExistsError:
            return
        except HttpResponseError as exc:
            raise CreateError(f"Failed to create table in Azure Table Storage: {exc!s}") from exc

    def _delete_table(self) -> None:
        """
        Deletes the table from Azure Table Storage.
        """
        self.log.info(f"Deleting table: {self.settings.table_name}.")
        try:
            self.client.delete_table(table_name=self.settings.table_name)
        except HttpResponseError as exc:
            self.log.error(f"Failed to delete Table ({self.settings.table_name})")
            raise DeleteError(f"Failed to delete table in Azure Table Storage: {exc!s}") from exc
        self.log.info(f"Successfully deleted table:{self.settings.table_name}.")

    def _delete_entity(self) -> None:
        """
        Deletes entities from Azure Table Storage.
        """
        entity = self._prepare_content()

        # Delete entity.
        table_client: TableClient = self.client.get_table_client(table_name=self.settings.table_name)
        try:
            self.log.info(f"Deleting entity: {entity}")
            table_client.delete_entity(
                row_key=entity["RowKey"],
                partition_key=entity["PartitionKey"],
            )
        except (ResourceNotFoundError, HttpResponseError) as exc:
            self.log.error(f"Failed to delete entity: {exc!s}")
            raise DeleteError(f"Failed to delete entity in Azure Table Storage: {exc!s}") from exc
        self.log.info("Successfully deleted entity.")

    def read(self, **__kwargs: Any) -> None:
        """
        Read Azure Table Storage dataset.
        :param __kwargs: dict
        :return: List[Dict]
        """
        # Read entities.
        table_client: TableClient = self.client.get_table_client(table_name=self.settings.table_name)
        try:
            if self.settings.query_filter:
                entities = table_client.query_entities(
                    query_filter=self.settings.query_filter,
                )
            else:
                entities = table_client.list_entities()
            if self.deserializer is None:
                raise ValueError("Deserializer is not initialized.")
            self.content = self.deserializer(entities)
        except HttpResponseError as exc:
            raise ReadError(f"Failed to read from Table Storage: {exc!s}") from exc

        self.log.info(f"Read data from Table Storage: {len(self.content)} items")

    def create(self, **_kwargs: Any) -> None:
        """
        Create an entity in Azure Table Storage.
        TODO: Support batching.
        """
        # Ensure correct content is provided.
        entity = self._prepare_content()

        # Create Table if not exist.
        self._create_table()

        # Create entity.
        table_client: TableClient = self.client.get_table_client(table_name=self.settings.table_name)
        try:
            self.log.info(f"Creating entity: {entity}")
            table_client.create_entity(entity=entity)
        except ResourceExistsError as exc:
            self.log.warning(f"Entity already exists: {exc!s}")
        except HttpResponseError as exc:
            self.log.error(f"Failed to create entity: {exc!s}")
            raise CreateError(f"Failed to create entity in Azure Table Storage '{self.settings.table_name}': {exc!s}") from exc
        self.log.info("Successfully created entity.")

    def update(self, **_kwargs: Any) -> None:
        """
        Update an entity in Azure Table Storage.
        """
        entity = self._prepare_content()

        # Update entity.
        table_client: TableClient = self.client.get_table_client(table_name=self.settings.table_name)
        try:
            self.log.info(f"Updating entity: {entity}")
            table_client.upsert_entity(
                entity=entity,
                mode=UpdateMode.MERGE,
            )
        except HttpResponseError as exc:
            self.log.error(f"Failed to update entity: {exc!s}")
            raise UpdateError(f"Failed to update entity in Azure Table Storage '{self.settings.table_name}': {exc!s}") from exc
        self.log.info("Successfully updated entity.")

    def delete(self, **_kwargs: Any) -> None:
        """
        Delete an entity or table in Azure Table Storage.
        """
        if self.settings.delete_table:
            self._delete_table()
        else:
            self._delete_entity()

    def rename(self, **_kwargs: Any) -> NoReturn:
        raise NotImplementedError("Rename operation is not supported for Azure Table datasets")

    def close(self) -> None:
        pass
