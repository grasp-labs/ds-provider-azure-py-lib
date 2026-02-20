"""
**File:** ``coercion.py``
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
    Recursively coerce a value into a JSON-serializable form.

    This helper is intended for preparing values to be passed to ``json.dumps``.
    It preserves the overall structure of the input while converting unsupported
    types into JSON-friendly representations. Containers (lists, tuples, and
    dicts) are processed recursively.

    Args:
        value: Any Python, pandas, NumPy, or PyArrow value to coerce. May be a
            scalar (e.g. ``int``, ``str``, ``pd.Timestamp``), a container
            (``list``, ``tuple``, ``dict``), or a library-specific scalar
            (e.g. ``numpy.scalar``, ``pyarrow.Scalar``, ``pd.Timedelta``).

    Returns:
        A JSON-serializable value with the same logical content as ``value``,
        where:

        * ``None`` and pandas/NumPy/pyarrow missing values are returned as
          ``None``.
        * ``list`` and ``tuple`` inputs become lists whose elements have been
          recursively coerced.
        * ``dict`` inputs become dicts whose values have been recursively
          coerced.
        * ``pyarrow`` scalars are converted via ``.as_py()`` and then coerced
          again.
        * ``numpy`` scalars are converted via ``.item()`` and then coerced
          again.
        * ``pd.Timestamp`` values are converted to ISO 8601 strings, with
          timezone-naive timestamps localized to UTC before conversion.
        * ``pd.Timedelta`` values are converted to ISO 8601 duration strings
          of the form ``"PT<seconds>S"`` (including a leading ``-`` for
          negative values).
        * ``uuid.UUID``, :class:`datetime.date`, :class:`datetime.datetime`,
          and :class:`datetime.time` values are converted to their ISO format
          string representations.
        * ``bytes`` values are base64-encoded and returned as UTF-8 strings.
        * Integer values outside the 32-bit signed range are returned as
          ``[value, "EdmType.INT64"]`` to preserve precision when encoded as
          JSON.
        * All other values are returned unchanged.

    Examples:
        Basic scalar coercion:

        >>> _coerce_for_json(42)
        42
        >>> _coerce_for_json(None)
        None

        Large integer (outside 32-bit range) is wrapped for INT64 handling:

        >>> _coerce_for_json(2**40)
        [1099511627776, "EdmType.INT64"]

        Timestamp and date/time coercion:

        >>> _coerce_for_json(pd.Timestamp("2024-01-01T00:00:00Z"))
        '2024-01-01T00:00:00+00:00'
        >>> _coerce_for_json(datetime(2024, 1, 1, 12, 0, 0))
        '2024-01-01T12:00:00'

        Bytes and nested structures:

        >>> _coerce_for_json({"data": b"hello", "ids": (1, 2, 3)})
        {'data': 'aGVsbG8=', 'ids': [1, 2, 3]}
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
        na_result = pd.isna(value)
        # Handle both scalar and array-like results
        # For array-like, check size to avoid ambiguity warning
        if hasattr(na_result, "size") and na_result.size > 0:
            # For single-element arrays, check the value
            if na_result.size == 1 and na_result.item():
                return None
        elif isinstance(na_result, bool) and na_result:
            # For scalar bool results
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
        na_result = pd.isna(value)
        # Handle both scalar and array-like results
        # For array-like, check size to avoid ambiguity warning
        if hasattr(na_result, "size") and na_result.size > 0:
            # For single-element arrays, check the value
            if na_result.size == 1 and na_result.item():
                return None
        elif isinstance(na_result, bool) and na_result:
            # For scalar bool results
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
