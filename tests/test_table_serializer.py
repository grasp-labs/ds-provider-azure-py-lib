"""
**File:** ``test_table_serializer.py``
**Region:** ``tests/test_table_serializer``

Unit tests for AzureTableSerializer and _coerce_value function.

Covers:
- Handling of pandas missing values (NaT, NaN, pd.NA)
- Conversion of numpy scalars to native Python types
- Handling of large integers that overflow Int32
- Handling of tz-naive and tz-aware timestamps
- Handling of nested dicts
"""

import base64
import json
import uuid
from datetime import date, datetime, time, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
from azure.data.tables import EdmType

from ds_provider_azure_py_lib.serde.coercion import _coerce_value
from ds_provider_azure_py_lib.serde.table import AzureTableSerializer


class TestCoerceValue:
    """Test the _coerce_value helper function."""

    def test_coerce_none_returns_none(self):
        """None should pass through unchanged."""
        assert _coerce_value(None) is None

    def test_coerce_nat_returns_none(self):
        """pd.NaT should be converted to None."""
        assert _coerce_value(pd.NaT) is None

    def test_coerce_nan_returns_none(self):
        """float('nan') should be converted to None."""
        assert _coerce_value(float("nan")) is None

    def test_coerce_pd_na_returns_none(self):
        """pd.NA should be converted to None."""
        assert _coerce_value(pd.NA) is None

    def test_coerce_numpy_int64_to_native_int(self):
        """numpy.int64 should be converted to native int."""
        value = np.int64(42)
        result = _coerce_value(value)
        assert result == 42
        assert isinstance(result, int)
        assert not isinstance(result, np.integer)

    def test_coerce_numpy_float64_to_native_float(self):
        """numpy.float64 should be converted to native float."""
        value = np.float64(3.14)
        result = _coerce_value(value)
        assert result == 3.14
        assert isinstance(result, float)
        assert not isinstance(result, np.floating)

    def test_coerce_numpy_bool_to_native_bool(self):
        """numpy.bool_ should be converted to native bool."""
        value = np.bool_(True)
        result = _coerce_value(value)
        assert result is True
        assert isinstance(result, bool)
        assert not isinstance(result, np.bool_)

    def test_coerce_numpy_bool_false_to_native_bool(self):
        """numpy.bool_(False) should be converted to native bool False."""
        value = np.bool_(False)
        result = _coerce_value(value)
        assert result is False
        assert isinstance(result, bool)

    def test_coerce_pandas_timestamp_tz_aware_to_datetime(self):
        """pd.Timestamp with timezone should be converted to native datetime."""
        value = pd.Timestamp("2024-01-15 10:30:00", tz="UTC")
        result = _coerce_value(value)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None
        assert result.tzinfo == timezone.utc

    def test_coerce_pandas_timestamp_tz_naive_to_utc_datetime(self):
        """pd.Timestamp without timezone should be localized to UTC and converted."""
        value = pd.Timestamp("2024-01-15 10:30:00")
        result = _coerce_value(value)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None
        assert result.tzinfo == timezone.utc
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_coerce_native_datetime_unchanged(self):
        """Native datetime should pass through unchanged."""
        value = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _coerce_value(value)
        assert result is value

    def test_coerce_small_int_unchanged(self):
        """Small integers (within Int32 range) should pass through unchanged."""
        value = 100
        result = _coerce_value(value)
        assert result == 100
        assert isinstance(result, int)

    def test_coerce_large_positive_int_to_int64_tuple(self):
        """Large positive integers (>= 2^31) should be wrapped as (value, EdmType.INT64)."""
        value = 3_000_000_000  # > 2^31 - 1
        result = _coerce_value(value)
        assert isinstance(result, tuple)
        assert result[0] == 3_000_000_000
        assert result[1] == EdmType.INT64

    def test_coerce_large_negative_int_to_int64_tuple(self):
        """Large negative integers (< -2^31) should be wrapped as (value, EdmType.INT64)."""
        value = -3_000_000_000  # < -2^31
        result = _coerce_value(value)
        assert isinstance(result, tuple)
        assert result[0] == -3_000_000_000
        assert result[1] == EdmType.INT64

    def test_coerce_int32_max_boundary(self):
        """Integer at 2^31 - 1 boundary should not be wrapped."""
        value = 2**31 - 1
        result = _coerce_value(value)
        assert result == 2**31 - 1
        assert isinstance(result, int)
        assert not isinstance(result, tuple)

    def test_coerce_int32_max_plus_one(self):
        """Integer at 2^31 boundary should be wrapped as Int64."""
        value = 2**31
        result = _coerce_value(value)
        assert isinstance(result, tuple)
        assert result == (2**31, EdmType.INT64)

    def test_coerce_int32_min_boundary(self):
        """Integer at -2^31 boundary should not be wrapped."""
        value = -(2**31)
        result = _coerce_value(value)
        assert result == -(2**31)
        assert isinstance(result, int)
        assert not isinstance(result, tuple)

    def test_coerce_int32_min_minus_one(self):
        """Integer at -2^31 - 1 boundary should be wrapped as Int64."""
        value = -(2**31) - 1
        result = _coerce_value(value)
        assert isinstance(result, tuple)
        assert result == (-(2**31) - 1, EdmType.INT64)

    def test_coerce_bool_not_treated_as_large_int(self):
        """bool should not trigger Int64 wrapping (bool is subclass of int)."""
        value = True
        result = _coerce_value(value)
        assert result is True
        assert isinstance(result, bool)
        assert not isinstance(result, tuple)

    def test_coerce_nested_dict_to_json_string(self):
        """Nested dicts should be converted to JSON strings."""
        value = {"a": 1, "b": "test"}
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == {"a": 1, "b": "test"}

    def test_coerce_nested_dict_with_complex_values(self):
        """Nested dicts with lists and nested objects should be JSON-serialized."""
        value = {"items": [1, 2, 3], "nested": {"x": "y"}}
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == {"items": [1, 2, 3], "nested": {"x": "y"}}

    def test_coerce_string_unchanged(self):
        """Strings should pass through unchanged."""
        value = "hello"
        result = _coerce_value(value)
        assert result == "hello"

    def test_coerce_numpy_datetime64_to_datetime(self):
        """numpy.datetime64 should be converted to native datetime via .item()."""
        value = np.datetime64("2024-01-15T10:30:00")
        result = _coerce_value(value)
        # numpy.datetime64 with time component converts to datetime.datetime
        assert isinstance(result, datetime)
        # Ensure the resulting datetime is timezone-aware for Azure Table Storage
        assert result.tzinfo is not None

    def test_coerce_timedelta_to_iso8601_string(self):
        """pd.Timedelta should be converted to ISO 8601 duration string."""
        value = pd.Timedelta("1 days")
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "PT86400S"  # 1 day = 86400 seconds

    def test_coerce_timedelta_zero(self):
        """pd.Timedelta of zero should be converted to PTS."""
        value = pd.Timedelta("0 days")
        result = _coerce_value(value)
        assert result == "PT0S"

    def test_coerce_timedelta_hours(self):
        """pd.Timedelta with hours should be converted correctly."""
        value = pd.Timedelta(hours=2)
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "PT7200S"  # 2 hours = 7200 seconds

    def test_coerce_negative_timedelta_to_iso8601_string(self):
        """Negative pd.Timedelta should be converted to ISO 8601 duration with leading '-'."""
        value = pd.Timedelta("-1 days")
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "-PT86400S"  # -1 day = -86400 seconds

    def test_coerce_bytes_to_base64_string(self):
        """bytes should be converted to base64 string."""
        value = b"hello"
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == base64.b64encode(b"hello").decode("utf-8")
        assert result == "aGVsbG8="

    def test_coerce_empty_bytes_to_base64(self):
        """Empty bytes should be converted to empty base64 string."""
        value = b""
        result = _coerce_value(value)
        assert result == ""

    def test_coerce_date_to_isoformat_string(self):
        """date should be converted to ISO 8601 date string."""
        value = date(2024, 1, 15)
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "2024-01-15"

    def test_coerce_time_to_isoformat_string(self):
        """time should be converted to ISO 8601 time string."""
        value = time(10, 30, 45)
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "10:30:45"

    def test_coerce_time_with_microseconds(self):
        """time with microseconds should be converted to ISO 8601 time string."""
        value = time(10, 30, 45, 123456)
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "10:30:45.123456"

    def test_coerce_uuid_to_string(self):
        """UUID should be converted to string."""
        test_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        result = _coerce_value(test_uuid)
        assert isinstance(result, str)
        assert result == "550e8400-e29b-41d4-a716-446655440000"

    def test_coerce_uuid_new(self):
        """UUID.uuid4() should be converted to string."""
        test_uuid = uuid.uuid4()
        result = _coerce_value(test_uuid)
        assert isinstance(result, str)
        assert result == str(test_uuid)

    def test_coerce_list_to_json_string(self):
        """list should be converted to JSON string."""
        value = [1, 2, 3]
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == [1, 2, 3]

    def test_coerce_list_with_mixed_types(self):
        """list with mixed types should be converted to JSON string."""
        value = [1, "text", 3.14, True]
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == [1, "text", 3.14, True]

    def test_coerce_empty_list(self):
        """empty list should be converted to empty JSON array."""
        value = []
        result = _coerce_value(value)
        assert result == "[]"

    def test_coerce_tuple_to_json_string(self):
        """tuple should be converted to JSON string."""
        value = (1, 2, 3)
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == [1, 2, 3]  # JSON converts tuples to arrays

    def test_coerce_empty_tuple(self):
        """empty tuple should be converted to empty JSON array."""
        value = ()
        result = _coerce_value(value)
        assert result == "[]"

    def test_coerce_nested_list_with_dict(self):
        """nested list with dict should be converted to JSON string."""
        value = [1, {"key": "value"}, 3]
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == [1, {"key": "value"}, 3]

    def test_coerce_list_with_numpy_scalars(self):
        """list containing numpy scalars should be recursively coerced and JSON-serialized."""
        value = [np.int64(1), np.float64(2.5), np.bool_(True)]
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert json.loads(result) == [1, 2.5, True]

    def test_coerce_list_with_timestamps(self):
        """list containing pd.Timestamp should be recursively coerced to ISO datetime strings."""
        value = [pd.Timestamp("2024-01-15 10:30:00"), "text", 42]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert len(parsed) == 3
        # Timestamp converted to ISO format string
        assert isinstance(parsed[0], str)
        assert "2024-01-15" in parsed[0]
        assert parsed[1] == "text"
        assert parsed[2] == 42

    def test_coerce_list_with_uuid(self):
        """list containing UUID should be recursively coerced to string."""
        test_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        value = [test_uuid, "data"]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed[0] == "550e8400-e29b-41d4-a716-446655440000"
        assert parsed[1] == "data"

    def test_coerce_list_with_bytes(self):
        """list containing bytes should be recursively coerced to base64 strings."""
        value = [b"hello", b"world"]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == ["aGVsbG8=", "d29ybGQ="]

    def test_coerce_list_with_timedeltas(self):
        """list containing pd.Timedelta should be recursively coerced to ISO duration strings."""
        value = [pd.Timedelta("1 days"), pd.Timedelta(hours=2)]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == ["PT86400S", "PT7200S"]

    def test_coerce_list_with_dates_and_times(self):
        """list containing date and time objects should be recursively coerced to ISO strings."""
        value = [date(2024, 1, 15), time(10, 30, 45)]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == ["2024-01-15", "10:30:45"]

    def test_coerce_dict_with_numpy_scalars(self):
        """dict containing numpy scalars should be recursively coerced and JSON-serialized."""
        value = {"count": np.int64(100), "ratio": np.float64(0.95), "active": np.bool_(True)}
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == {"count": 100, "ratio": 0.95, "active": True}

    def test_coerce_dict_with_timestamp(self):
        """dict containing pd.Timestamp should be recursively coerced."""
        value = {"created": pd.Timestamp("2024-01-15 10:30:00"), "name": "test"}
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "2024-01-15" in parsed["created"]
        assert parsed["name"] == "test"

    def test_coerce_dict_with_uuid(self):
        """dict containing UUID should be recursively coerced to string."""
        test_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        value = {"id": test_uuid, "status": "active"}
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed["id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert parsed["status"] == "active"

    def test_coerce_dict_with_bytes(self):
        """dict containing bytes should be recursively coerced to base64 strings."""
        value = {"data": b"hello", "image": b"image_bytes"}
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == {"data": "aGVsbG8=", "image": "aW1hZ2VfYnl0ZXM="}

    def test_coerce_deeply_nested_structure(self):
        """deeply nested structure with mixed types should be recursively coerced."""
        value = {
            "metadata": {
                "ids": [uuid.UUID("550e8400-e29b-41d4-a716-446655440000")],
                "timestamps": [pd.Timestamp("2024-01-15")],
                "counts": [np.int64(42)],
            },
            "binary": b"data",
            "array": [
                {"nested_id": uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")},
                {"nested_int": np.int64(100)},
            ],
        }
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)

        # Verify nested structure is properly coerced
        assert parsed["binary"] == base64.b64encode(b"data").decode("utf-8")
        assert parsed["metadata"]["ids"][0] == "550e8400-e29b-41d4-a716-446655440000"
        assert "2024-01-15" in parsed["metadata"]["timestamps"][0]
        assert parsed["metadata"]["counts"][0] == 42
        assert parsed["array"][0]["nested_id"] == "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        assert parsed["array"][1]["nested_int"] == 100

    def test_coerce_list_with_nat_and_nan(self):
        """list containing NaT and NaN should be recursively coerced to None."""
        value = [pd.NaT, float("nan"), "valid", np.int64(42)]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed[0] is None
        assert parsed[1] is None
        assert parsed[2] == "valid"
        assert parsed[3] == 42

    def test_coerce_dict_with_na_values(self):
        """dict containing NA values should be recursively coerced to None."""
        value = {"nat": pd.NaT, "nan": float("nan"), "name": "test"}
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed["nat"] is None
        assert parsed["nan"] is None
        assert parsed["name"] == "test"

    def test_coerce_list_with_large_integers(self):
        """list containing large integers should be recursively coerced."""
        value = [np.int64(9_999_999_999), 100, np.int64(-3_000_000_000)]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        # Large ints are represented as [value, "EdmType.INT64"] in JSON
        assert parsed[0] == [9_999_999_999, "EdmType.INT64"]
        assert parsed[1] == 100
        assert parsed[2] == [-3_000_000_000, "EdmType.INT64"]

    # ========== PyArrow Scalar Tests ==========

    def test_coerce_pyarrow_int64_to_native_int(self):
        """PyArrow int64 scalar should be converted to native int."""
        value = pa.scalar(42, type=pa.int64())
        result = _coerce_value(value)
        assert result == 42
        assert isinstance(result, int)
        assert not isinstance(result, pa.Scalar)

    def test_coerce_pyarrow_int32_to_native_int(self):
        """PyArrow int32 scalar should be converted to native int."""
        value = pa.scalar(100, type=pa.int32())
        result = _coerce_value(value)
        assert result == 100
        assert isinstance(result, int)

    def test_coerce_pyarrow_float64_to_native_float(self):
        """PyArrow float64 scalar should be converted to native float."""
        value = pa.scalar(3.14, type=pa.float64())
        result = _coerce_value(value)
        assert result == 3.14
        assert isinstance(result, float)
        assert not isinstance(result, pa.Scalar)

    def test_coerce_pyarrow_float32_to_native_float(self):
        """PyArrow float32 scalar should be converted to native float."""
        value = pa.scalar(2.71, type=pa.float32())
        result = _coerce_value(value)
        # Float32 may have slight precision differences, so use approximate comparison
        assert abs(result - 2.71) < 0.01
        assert isinstance(result, float)

    def test_coerce_pyarrow_bool_to_native_bool(self):
        """PyArrow bool scalar should be converted to native bool."""
        value = pa.scalar(True, type=pa.bool_())
        result = _coerce_value(value)
        assert result is True
        assert isinstance(result, bool)
        assert not isinstance(result, pa.Scalar)

    def test_coerce_pyarrow_bool_false_to_native_bool(self):
        """PyArrow bool(False) scalar should be converted to native bool."""
        value = pa.scalar(False, type=pa.bool_())
        result = _coerce_value(value)
        assert result is False
        assert isinstance(result, bool)

    def test_coerce_pyarrow_string_to_native_string(self):
        """PyArrow string scalar should be converted to native string."""
        value = pa.scalar("hello", type=pa.string())
        result = _coerce_value(value)
        assert result == "hello"
        assert isinstance(result, str)

    def test_coerce_pyarrow_binary_to_base64(self):
        """PyArrow binary scalar should be converted to base64 string."""
        value = pa.scalar(b"hello", type=pa.binary())
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == base64.b64encode(b"hello").decode("utf-8")
        assert result == "aGVsbG8="

    def test_coerce_pyarrow_timestamp_to_datetime(self):
        """PyArrow timestamp scalar should be converted to native datetime."""
        value = pa.scalar(pd.Timestamp("2024-01-15 10:30:00"), type=pa.timestamp("us"))
        result = _coerce_value(value)
        assert isinstance(result, datetime)
        # PyArrow timestamp conversions should result in tz-aware datetime
        assert result.tzinfo is not None or result.year == 2024

    def test_coerce_pyarrow_date32_to_date_string(self):
        """PyArrow date32 scalar should be converted to date string via .item()."""
        value = pa.scalar(date(2024, 1, 15), type=pa.date32())
        result = _coerce_value(value)
        # After .item() converts to Python date, should be converted to ISO string
        assert isinstance(result, str)
        assert result == "2024-01-15"

    def test_coerce_pyarrow_date64_to_date_string(self):
        """PyArrow date64 scalar should be converted to date string."""
        value = pa.scalar(date(2024, 1, 15), type=pa.date64())
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "2024-01-15"

    def test_coerce_pyarrow_large_int64_positive(self):
        """PyArrow large positive int64 should be wrapped as (value, EdmType.INT64)."""
        value = pa.scalar(3_000_000_000, type=pa.int64())
        result = _coerce_value(value)
        assert isinstance(result, tuple)
        assert result[0] == 3_000_000_000
        assert result[1] == EdmType.INT64

    def test_coerce_pyarrow_large_int64_negative(self):
        """PyArrow large negative int64 should be wrapped as (value, EdmType.INT64)."""
        value = pa.scalar(-3_000_000_000, type=pa.int64())
        result = _coerce_value(value)
        assert isinstance(result, tuple)
        assert result[0] == -3_000_000_000
        assert result[1] == EdmType.INT64

    def test_coerce_pyarrow_null_scalar_to_none(self):
        """PyArrow null scalar should be converted to None."""
        value = pa.scalar(None, type=pa.int64())
        result = _coerce_value(value)
        assert result is None

    def test_coerce_pyarrow_utf8_string(self):
        """PyArrow utf8 string scalar should be converted to native string."""
        value = pa.scalar("test", type=pa.utf8())
        result = _coerce_value(value)
        assert result == "test"
        assert isinstance(result, str)

    def test_coerce_pyarrow_large_string(self):
        """PyArrow large_string scalar should be converted to native string."""
        value = pa.scalar("long text", type=pa.large_string())
        result = _coerce_value(value)
        assert result == "long text"
        assert isinstance(result, str)

    def test_coerce_pyarrow_uint64_to_native_int(self):
        """PyArrow uint64 scalar should be converted to native int."""
        value = pa.scalar(100, type=pa.uint64())
        result = _coerce_value(value)
        assert result == 100
        assert isinstance(result, int)

    def test_coerce_pyarrow_uint32_to_native_int(self):
        """PyArrow uint32 scalar should be converted to native int."""
        value = pa.scalar(50, type=pa.uint32())
        result = _coerce_value(value)
        assert result == 50
        assert isinstance(result, int)

    def test_coerce_list_with_pyarrow_scalars(self):
        """list containing PyArrow scalars should be recursively coerced."""
        value = [
            pa.scalar(42, type=pa.int64()),
            pa.scalar(3.14, type=pa.float64()),
            pa.scalar(True, type=pa.bool_()),
        ]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == [42, 3.14, True]

    def test_coerce_dict_with_pyarrow_scalars(self):
        """dict containing PyArrow scalars should be recursively coerced."""
        value = {
            "count": pa.scalar(100, type=pa.int64()),
            "ratio": pa.scalar(0.95, type=pa.float64()),
            "active": pa.scalar(True, type=pa.bool_()),
        }
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == {"count": 100, "ratio": 0.95, "active": True}

    def test_coerce_list_with_mixed_numpy_and_pyarrow_scalars(self):
        """list with both numpy and PyArrow scalars should be coerced."""
        value = [
            np.int64(42),
            pa.scalar(100, type=pa.int64()),
            np.float64(3.14),
            pa.scalar(2.71, type=pa.float64()),
        ]
        result = _coerce_value(value)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed[0] == 42
        assert parsed[1] == 100
        assert abs(parsed[2] - 3.14) < 0.01
        assert abs(parsed[3] - 2.71) < 0.01

    def test_coerce_pyarrow_int8_small_value(self):
        """PyArrow int8 scalar should be converted to native int."""
        value = pa.scalar(100, type=pa.int8())
        result = _coerce_value(value)
        assert result == 100
        assert isinstance(result, int)

    def test_coerce_pyarrow_int16_small_value(self):
        """PyArrow int16 scalar should be converted to native int."""
        value = pa.scalar(1000, type=pa.int16())
        result = _coerce_value(value)
        assert result == 1000
        assert isinstance(result, int)


class TestAzureTableSerializer:
    """Test the AzureTableSerializer class."""

    def test_serialize_dataframe_with_native_types(self):
        """Serializer should handle DataFrames with native Python types."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Name": "John",
                    "Age": 30,
                    "Active": True,
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["PartitionKey"] == "pk1"
        assert result["RowKey"] == "rk1"
        assert result["Name"] == "John"
        assert result["Age"] == 30
        assert result["Active"] is True

    def test_serialize_dataframe_with_numpy_scalars(self):
        """Serializer should convert numpy scalars to native types."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Count": np.int64(42),
                    "Ratio": np.float64(3.14),
                    "Flag": np.bool_(True),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["Count"] == 42
        assert isinstance(result["Count"], int)
        assert result["Ratio"] == 3.14
        assert isinstance(result["Ratio"], float)
        assert result["Flag"] is True
        assert isinstance(result["Flag"], bool)

    def test_serialize_dataframe_with_nat(self):
        """Serializer should convert NaT to None."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "CreatedAt": pd.NaT,
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["CreatedAt"] is None

    def test_serialize_dataframe_with_nan(self):
        """Serializer should convert NaN to None."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Score": float("nan"),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["Score"] is None

    def test_serialize_dataframe_with_pd_na(self):
        """Serializer should convert pd.NA to None."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Optional": pd.NA,
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["Optional"] is None

    def test_serialize_dataframe_with_tz_naive_timestamp(self):
        """Serializer should convert tz-naive Timestamp to UTC datetime."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "EventTime": pd.Timestamp("2024-01-15 10:30:00"),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert isinstance(result["EventTime"], datetime)
        assert result["EventTime"].tzinfo == timezone.utc

    def test_serialize_dataframe_with_tz_aware_timestamp(self):
        """Serializer should preserve tz-aware Timestamp."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "EventTime": pd.Timestamp("2024-01-15 10:30:00", tz="UTC"),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert isinstance(result["EventTime"], datetime)
        assert result["EventTime"].tzinfo == timezone.utc

    def test_serialize_dataframe_with_large_int(self):
        """Serializer should wrap large integers as (value, EdmType.INT64)."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "BigID": 3_000_000_000,
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert isinstance(result["BigID"], tuple)
        assert result["BigID"][0] == 3_000_000_000
        assert result["BigID"][1] == EdmType.INT64

    def test_serialize_dataframe_with_nested_dict(self):
        """Serializer should convert nested dicts to JSON strings."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Metadata": {"key": "value", "count": 5},
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert isinstance(result["Metadata"], str)
        assert json.loads(result["Metadata"]) == {"key": "value", "count": 5}

    def test_serialize_dataframe_complex_types_combined(self):
        """Serializer should handle multiple problematic types in one DataFrame."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "ID": np.int64(9_999_999_999),  # Large int
                    "Score": np.float64(98.6),  # Numpy float
                    "Active": np.bool_(True),  # Numpy bool
                    "CreatedAt": pd.NaT,  # Missing datetime
                    "UpdatedAt": pd.Timestamp("2024-01-15"),  # Tz-naive timestamp
                    "Metadata": {"version": 1},  # Nested dict
                    "Rating": float("nan"),  # Missing float
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        # Large int should be wrapped
        assert isinstance(result["ID"], tuple)
        assert result["ID"][0] == 9_999_999_999
        assert result["ID"][1] == EdmType.INT64

        # Numpy types should be converted
        assert result["Score"] == 98.6
        assert isinstance(result["Score"], float)
        assert result["Active"] is True
        assert isinstance(result["Active"], bool)

        # Missing values should be None
        assert result["CreatedAt"] is None
        assert result["Rating"] is None

        # Timestamps should be datetime
        assert isinstance(result["UpdatedAt"], datetime)
        assert result["UpdatedAt"].tzinfo == timezone.utc

        # Dicts should be JSON strings
        assert isinstance(result["Metadata"], str)
        assert json.loads(result["Metadata"]) == {"version": 1}

    def test_serialize_converts_partition_and_row_keys_to_str(self):
        """Serializer should convert PartitionKey and RowKey to strings."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": 123,
                    "RowKey": 456,
                    "Data": "test",
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["PartitionKey"] == "123"
        assert isinstance(result["PartitionKey"], str)
        assert result["RowKey"] == "456"
        assert isinstance(result["RowKey"], str)

    def test_serialize_preserves_column_keys(self):
        """Serializer should preserve all column names from DataFrame."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk",
                    "RowKey": "rk",
                    "FieldA": 1,
                    "FieldB": "text",
                    "FieldC": 3.14,
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert set(result.keys()) == {"PartitionKey", "RowKey", "FieldA", "FieldB", "FieldC"}

    def test_serialize_ignores_additional_kwargs(self):
        """Serializer should ignore additional keyword arguments."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk",
                    "RowKey": "rk",
                    "Value": 42,
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df, unused_arg="ignored", another="also_ignored")

        assert result["Value"] == 42

    # ========== PyArrow Scalar Tests in Serializer ==========

    def test_serialize_dataframe_with_pyarrow_scalars(self):
        """Serializer should convert PyArrow scalars to native types."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Count": pa.scalar(42, type=pa.int64()),
                    "Ratio": pa.scalar(3.14, type=pa.float64()),
                    "Flag": pa.scalar(True, type=pa.bool_()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["Count"] == 42
        assert isinstance(result["Count"], int)
        assert result["Ratio"] == 3.14
        assert isinstance(result["Ratio"], float)
        assert result["Flag"] is True
        assert isinstance(result["Flag"], bool)

    def test_serialize_dataframe_with_pyarrow_large_int(self):
        """Serializer should wrap PyArrow large integers as (value, EdmType.INT64)."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "BigID": pa.scalar(9_999_999_999, type=pa.int64()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert isinstance(result["BigID"], tuple)
        assert result["BigID"][0] == 9_999_999_999
        assert result["BigID"][1] == EdmType.INT64

    def test_serialize_dataframe_with_pyarrow_string(self):
        """Serializer should handle PyArrow string scalars."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Message": pa.scalar("hello world", type=pa.string()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["Message"] == "hello world"
        assert isinstance(result["Message"], str)

    def test_serialize_dataframe_with_pyarrow_binary(self):
        """Serializer should convert PyArrow binary to base64."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Data": pa.scalar(b"test", type=pa.binary()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert isinstance(result["Data"], str)
        assert result["Data"] == base64.b64encode(b"test").decode("utf-8")

    def test_serialize_dataframe_with_pyarrow_null(self):
        """Serializer should convert PyArrow null scalar to None."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "Optional": pa.scalar(None, type=pa.int64()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["Optional"] is None

    def test_serialize_dataframe_with_mixed_numpy_and_pyarrow_scalars(self):
        """Serializer should handle both numpy and PyArrow scalars."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "NumpyInt": np.int64(100),
                    "PyArrowInt": pa.scalar(200, type=pa.int64()),
                    "NumpyFloat": np.float64(1.5),
                    "PyArrowFloat": pa.scalar(2.5, type=pa.float64()),
                    "NumpyBool": np.bool_(True),
                    "PyArrowBool": pa.scalar(False, type=pa.bool_()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["NumpyInt"] == 100
        assert isinstance(result["NumpyInt"], int)
        assert result["PyArrowInt"] == 200
        assert isinstance(result["PyArrowInt"], int)
        assert result["NumpyFloat"] == 1.5
        assert isinstance(result["NumpyFloat"], float)
        assert result["PyArrowFloat"] == 2.5
        assert isinstance(result["PyArrowFloat"], float)
        assert result["NumpyBool"] is True
        assert isinstance(result["NumpyBool"], bool)
        assert result["PyArrowBool"] is False
        assert isinstance(result["PyArrowBool"], bool)

    def test_serialize_dataframe_with_pyarrow_uint_types(self):
        """Serializer should convert PyArrow unsigned integer types."""
        df = pd.DataFrame(
            [
                {
                    "PartitionKey": "pk1",
                    "RowKey": "rk1",
                    "UInt32": pa.scalar(500, type=pa.uint32()),
                    "UInt64": pa.scalar(1000, type=pa.uint64()),
                }
            ]
        )
        serializer = AzureTableSerializer()
        result = serializer(df)

        assert result["UInt32"] == 500
        assert isinstance(result["UInt32"], int)
        assert result["UInt64"] == 1000
        assert isinstance(result["UInt64"], int)
