"""
**File:** ``03_table_dataset_read_update_delete.py``
**Region:** ``examples/03_table_dataset_read_update_delete``

Example 03: Connect to Azure Table Storage using AzureTable dataset, read, update, and delete table entities.

This example demonstrates how to:
- Create an instance of `AzureTable` dataset with the necessary settings, serializer, deserializer
, and linked service.
- Read an entity from the specified table using partition key and row key.
- If the entity does not exist, create a new entity and update the table.
- Delete the entity from the table.

Prerequisites:
- An Azure Storage account with a table named "testazurepackage".
- Entities in the table with specified partition key and row key for reading and deleting.
- The entity data should be in a format compatible with the deserializer (Pandas DataFrame).
- The access key for the storage account should have sufficient permissions to read, write, and delete entities in the table.
- The `ds_provider_azure_py_lib` library should be installed and accessible in the Python environment.
"""

import os
import uuid
from datetime import date, time

import numpy as np
import pandas as pd
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError

from ds_provider_azure_py_lib.dataset import AzureTable, AzureTableDatasetSettings
from ds_provider_azure_py_lib.dataset.table import AzureTableSerializer, AzureTableDeserializer, ReadSettings, \
    PurgeSettings, CreateSettings
from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings


def main():
    """
    Demonstrates AzureTable dataset with comprehensive type support.

    Tests serialization of various data types that were previously causing crashes:
    - Missing values (NaT, NaN, pd.NA)
    - Pandas scalars (Timestamp, Timedelta)
    - NumPy scalars (int64, float64, bool_)
    - Standard library types (bytes, date, time, UUID)
    - Collections (list, tuple, dict)
    """
    dataset = AzureTable(
        serializer=AzureTableSerializer(),
        deserializer=AzureTableDeserializer(),
        settings=AzureTableDatasetSettings(
            table_name="testazurepackage",
            read=ReadSettings(query_filter="PartitionKey eq 'colors'"),
            purge=PurgeSettings(delete_table=True, wait_after_table_deletion=True),
            create=CreateSettings(retry_on_table_being_deleted=True)
        ),
        linked_service=AzureLinkedService(
            settings=AzureLinkedServiceSettings(
                account_name=os.environ.get("ACCOUNT_NAME"), access_key=os.environ.get("ACCOUNT_KEY")
            ),
            id=uuid.uuid4(),
            name="testazurepackage",
            version="0.0.1",
            description="testazurepackage",
        ),
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
        description="testazurepackage",
    )

    dataset.linked_service.connect()
    try:
        dataset.read()
        row = dataset.output
    except ReadError:
        row = pd.DataFrame({})

    if not row.empty:
        print("Read existing entities from table:")
        print(row)
    else:
        print("No entities found, creating new test data...")

        test_data = {
            "PartitionKey": ["colors", "colors"],
            "RowKey": [str(uuid.uuid4()), str(uuid.uuid4())],
            "CreatedAt": [pd.Timestamp("2026-01-27 10:30:00"), pd.NaT],  # NaT → None
            "Score": [98.6, float("nan")],  # NaN → None
            "Optional": [pd.NA, None],  # pd.NA → None
            "ItemCount": [np.int64(100), np.int64(250)],  # np.int64 → int
            "Ratio": [np.float64(0.95), np.float64(0.88)],  # np.float64 → float
            "IsActive": [np.bool_(True), np.bool_(False)],  # np.bool_ → bool
            "UniqueID": [np.int64(9_999_999_999), np.int64(8_888_888_888)],  # > 2^31 → EdmType.INT64
            "Duration": [pd.Timedelta("1 days"), pd.Timedelta(hours=2)],  # → ISO 8601 string
            "BinaryData": [b"color_rgb_128_128_128", b"color_rgb_255_0_0"],  # → base64 string
            "CreatedDate": [date(2026, 1, 27), date(2026, 1, 28)],  # → ISO 8601 date string
            "CreatedTime": [time(10, 30, 45), time(14, 15, 30)],  # → ISO 8601 time string
            "ProcessID": [
                uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
                uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
            ],  # → string
            "Tags": [["primary", "color"], ["metadata", "system"]],  # → JSON string
            "Properties": [{"hex": "FF0000", "rgb": "255,0,0"}, {"hex": "808080", "rgb": "128,128,128"}],  # → JSON string
        }

        dataset.input = pd.DataFrame(test_data)

        print("\nTest DataFrame Created:")
        print(dataset.input)
        print("\nDataFrame Data Types:")
        print(dataset.input.dtypes)

        print("\nCreating entities with comprehensive data types...")
        dataset.create()

        print("Entities created successfully!")
        print("\nReading created entities back from table...")
        dataset.read()
        print(dataset.output)

    # Update test: modify and save
    if not dataset.output.empty:
        print("\nTesting update operation...")
        dataset.input = dataset.output.copy()
        dataset.input["Score"] = [r/2 for r in dataset.input["Score"]]
        dataset.update()
        print("Update completed successfully!")

        # Read again to confirm update
        dataset.read()
        print("Data after update:")
        print(dataset.output)

    # Delete test
    if not dataset.output.empty:
        print("\nTesting purge operation...")
        dataset.input = dataset.output.copy()
        dataset.purge()
        print("Delete completed successfully!")

        try:
            dataset.read()
            remaining_count = len(dataset.output)
            print(f"Remaining entities after delete: {remaining_count}")
            if remaining_count == 0:
                print("All entities deleted successfully!")
            print(dataset.output)
        except ReadError:
            print("Table deleted successfully")
    dataset.create()


if __name__ == "__main__":
    main()
