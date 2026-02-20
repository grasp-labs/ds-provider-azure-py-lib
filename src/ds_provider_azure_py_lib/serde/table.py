import base64
import json
from datetime import date, datetime, time
from typing import Any
from uuid import UUID

import pandas as pd
from azure.core.paging import ItemPaged
from azure.data.tables import EdmType, TableEntity
from ds_resource_plugin_py_lib.common.serde.deserialize import DataDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import DataSerializer

_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1


def _coerce_value(value: Any) -> Any:  # noqa: PLR0912
    """
    Convert a pandas / numpy / pyarrow scalar to a type the Azure Table SDK accepts.

    Args:
        value: The value to coerce.

    Returns:
        A value that the Azure Table SDK can serialize.
    """
    # NA-like values (NaT, NaN, pd.NA) → None (property omitted from entity)
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        pass

    # Sequences (lists, tuples) → JSON string
    if isinstance(value, (list, tuple)):
        return json.dumps(value)

    # Nested dicts → JSON string
    if isinstance(value, dict):
        return json.dumps(value)

    # pd.Timestamp → native datetime.datetime (tz-naive gets localized to UTC)
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.to_pydatetime()

    # pd.Timedelta → ISO 8601 duration string (e.g., "PT86400S" for 1 day)
    if isinstance(value, pd.Timedelta):
        return f"PT{value.total_seconds()}S"

    # UUID → string representation
    if isinstance(value, UUID):
        return str(value)

    # date (but not datetime) → ISO 8601 date string
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()

    # time → ISO 8601 time string
    if isinstance(value, time):
        return value.isoformat()

    # bytes → base64 string (standard for binary data in APIs)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("utf-8")

    # numpy / pyarrow scalar → native Python type via .item()
    if hasattr(value, "item"):
        value = value.item()

    # Large ints that overflow Azure Table's default Int32 → explicit Int64
    if isinstance(value, int) and not isinstance(value, bool) and (value < _INT32_MIN or value > _INT32_MAX):
        return (value, EdmType.INT64)

    return value


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
            if "Timestamp" not in entity_data:
                entity_data["Timestamp"] = entity.metadata["timestamp"]
            data.append(entity_data)

        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
