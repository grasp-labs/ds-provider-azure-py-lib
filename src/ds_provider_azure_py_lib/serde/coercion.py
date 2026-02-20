"""
**File:** coercion.py**
**Region:** ``ds_provider_azure_py_lib/serde/coercion``

Coercion functions to convert between pandas/numpy/pyarrow types and Azure Table Storage-compatible types.
"""

import base64
import contextlib
import json
from datetime import date, datetime, time, timezone
from typing import Any
from uuid import UUID

import pandas as pd
from azure.data.tables import EdmType

_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1


def _coerce_for_json(value: Any) -> Any:  # noqa: PLR0912
    """
    Recursively coerce a value to be JSON-serializable.

    Handles nested structures (lists, dicts) by coercing their contents.
    """
    if value is None:
        return None

    # Handle lists
    if isinstance(value, list):
        return [_coerce_for_json(item) for item in value]

    # Handle tuples (convert to list for JSON)
    if isinstance(value, tuple):
        return [_coerce_for_json(item) for item in value]

    # Handle dicts
    if isinstance(value, dict):
        return {k: _coerce_for_json(v) for k, v in value.items()}

    # For other types, apply the standard coercion logic
    # (but skip the top-level list/tuple/dict checks to avoid recursion)

    # PyArrow scalar → native Python type via .as_py()
    if hasattr(value, "as_py") and hasattr(value, "type"):
        with contextlib.suppress(TypeError, ValueError, AttributeError):
            return _coerce_for_json(value.as_py())

    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        pass

    # pd.Timestamp → ISO format string (tz-naive gets localized to UTC)
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.isoformat()

    # pd.Timedelta
    if isinstance(value, pd.Timedelta):
        seconds = value.total_seconds()
        sign = "-" if seconds < 0 else ""
        seconds = abs(seconds)
        seconds_str = str(int(seconds)) if seconds.is_integer() else str(seconds)
        return f"{sign}PT{seconds_str}S"

    # UUID
    if isinstance(value, UUID):
        return str(value)

    # date (but not datetime)
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()

    # datetime → ISO format string
    if isinstance(value, datetime):
        return value.isoformat()

    # time
    if isinstance(value, time):
        return value.isoformat()

    # bytes
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("utf-8")

    # numpy/pyarrow scalar
    if hasattr(value, "item") and not isinstance(value, (bytes, memoryview)):
        return _coerce_for_json(value.item())

    # Large ints
    if isinstance(value, int) and not isinstance(value, bool) and (value < _INT32_MIN or value > _INT32_MAX):
        # Return as list for JSON serialization (will be wrapped as tuple at top-level)
        return [value, "EdmType.INT64"]

    return value


def _coerce_value(value: Any) -> Any:  # noqa: PLR0912
    """
    Convert a pandas / numpy / pyarrow scalar to a type the Azure Table SDK accepts.

    Args:
        value: The value to coerce.

    Returns:
        A value that the Azure Table SDK can serialize.
    """
    # PyArrow scalar → native Python type via .as_py()
    # Check for PyArrow scalars first (they have type and as_py attributes)
    if hasattr(value, "as_py") and hasattr(value, "type"):
        with contextlib.suppress(TypeError, ValueError, AttributeError):
            value = value.as_py()

    # numpy / pyarrow scalar → native Python type via .item()
    # Do this FIRST before any other checks to ensure proper type handling
    if hasattr(value, "item") and not isinstance(value, (bytes, memoryview)):
        value = value.item()

    # NA-like values (NaT, NaN, pd.NA) → None (property omitted from entity)
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        pass

    # Sequences (lists, tuples) → JSON string
    if isinstance(value, (list, tuple)):
        coerced = _coerce_for_json(value)
        return json.dumps(coerced)

    # Nested dicts → JSON string
    if isinstance(value, dict):
        coerced = _coerce_for_json(value)
        return json.dumps(coerced)

    # pd.Timestamp → native datetime.datetime (tz-naive gets localized to UTC)
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.to_pydatetime()

    # pd.Timedelta → ISO 8601 duration string (e.g., "PT86400S" for 1 day)
    if isinstance(value, pd.Timedelta):
        seconds = value.total_seconds()
        sign = "-" if seconds < 0 else ""
        seconds = abs(seconds)
        seconds_str = str(int(seconds)) if seconds.is_integer() else str(seconds)
        return f"{sign}PT{seconds_str}S"

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

    # After scalar unboxing, handle datetime objects (e.g., from numpy.datetime64)
    if isinstance(value, datetime) and value.tzinfo is None:
        # Localize naive datetime to UTC
        return value.replace(tzinfo=timezone.utc)

    # Large ints that overflow Azure Table's default Int32 → explicit Int64
    if isinstance(value, int) and not isinstance(value, bool) and (value < _INT32_MIN or value > _INT32_MAX):
        return (value, EdmType.INT64)

    return value
