"""
**File:** ``table.py``
**Region:** ``ds_provider_azure_py_lib/serde/table``

Serde for Azure Table Storage data.
"""

from typing import Any

import pandas as pd
from azure.core.paging import ItemPaged
from azure.data.tables import TableEntity
from ds_resource_plugin_py_lib.common.serde.deserialize import DataDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import DataSerializer

from ds_provider_azure_py_lib.serde.coercion import _coerce_value


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
        return {k: _coerce_value(v) for k, v in df.iloc[0].to_dict().items()}


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
            if "Timestamp" not in entity_data and hasattr(entity, "metadata") and "timestamp" in entity.metadata:
                entity_data["Timestamp"] = entity.metadata["timestamp"]
            data.append(entity_data)

        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
