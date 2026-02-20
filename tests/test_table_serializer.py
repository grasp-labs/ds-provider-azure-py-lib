"""
**File:** ``test_table_serializer.py``
**Region:** ``tests/test_table_serializer``

Unit tests for AzureTableSerializer and _coerce_value function.

Covers:
- Handling of pandas missing values (NaT, NaN, pd.NA)
- Conversion of numpy scalars to native Python types
- Conversion of pyarrow scalars to native Python types
- Handling of large integers that overflow Int32
- Handling of tz-naive and tz-aware timestamps
- Handling of nested dicts
- Round-trip serialization
"""

import base64
import json
import uuid
from datetime import date, datetime, time, timezone

import numpy as np
import pandas as pd
from azure.data.tables import EdmType

from ds_provider_azure_py_lib.serde.table import AzureTableSerializer, _coerce_value


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

    def test_coerce_timedelta_to_iso8601_string(self):
        """pd.Timedelta should be converted to ISO 8601 duration string."""
        value = pd.Timedelta("1 days")
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "PT86400.0S"  # 1 day = 86400 seconds

    def test_coerce_timedelta_zero(self):
        """pd.Timedelta of zero should be converted to PT0S."""
        value = pd.Timedelta("0 days")
        result = _coerce_value(value)
        assert result == "PT0.0S"

    def test_coerce_timedelta_hours(self):
        """pd.Timedelta with hours should be converted correctly."""
        value = pd.Timedelta(hours=2)
        result = _coerce_value(value)
        assert isinstance(result, str)
        assert result == "PT7200.0S"  # 2 hours = 7200 seconds

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
