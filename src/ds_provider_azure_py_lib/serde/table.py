import json
from typing import Any

import pandas as pd
from azure.core.paging import ItemPaged
from azure.data.tables import TableEntity
from ds_resource_plugin_py_lib.common.serde.deserialize import DataDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import DataSerializer


class AzureTableSerializer(DataSerializer):
    """
    Serialize Azure Table Storage data.
    The serializer is responsible for converting the data from
    a DataFrame into a format that can be sent to the Azure Table Storage API.
    """

    def __call__(self, obj: pd.DataFrame, **_kwargs: Any) -> Any | dict[str, Any]:
        """
        Serialize the data from a DataFrame into a dict.

        Args:
          obj: Input DataFrame.
          _kwargs: Additional keyword arguments.

        Returns:
          A dict representation of the first row.
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
        Deserialize the data from an item-paged result into a DataFrame.

        Args:
            value: The paged table entities.
            _kwargs: Additional keyword arguments.

        Returns:
            A DataFrame with the entities.
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
