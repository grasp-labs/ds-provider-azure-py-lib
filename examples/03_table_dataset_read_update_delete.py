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

import base64
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone

import numpy as np
import pandas as pd

from ds_provider_azure_py_lib.dataset import AzureTable, AzureTableDatasetSettings
from ds_provider_azure_py_lib.dataset.table import AzureTableSerializer, AzureTableDeserializer, ReadSettings, \
    DeleteSettings
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
            read=ReadSettings(query_filter="PartitionKey eq '1'"),
            delete=DeleteSettings(delete_table=False),  # Set to True to delete table after test
        ),
        linked_service=AzureLinkedService(
            settings=AzureLinkedServiceSettings(
                account_name=os.environ.get("ACCOUNT_NAME"),
                access_key=os.environ.get("ACCOUNT_KEY")
            ),
            id=uuid.uuid4(),
            name="testazurepackage",
            version="0.0.1",
            description="testazurepackage",
        ),
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
        description="testazurepackage"
    )

    dataset.linked_service.connect()
    dataset.create()
    dataset.read()
    row = dataset.output

    if not row.empty:
        print("Read existing entities from table:")
        print(row)
    else:
        print("No entities found, creating new test data...")

        # Create test DataFrame with all problematic data types
        test_data = {
            # Required Azure Table Storage fields
            # NOTE: All entities in a batch transaction must have the same PartitionKey
            "PartitionKey": ["colors", "colors"],
            "RowKey": [str(uuid.uuid4()), str(uuid.uuid4())],

            # ===== PHASE 1: Original 8 Types Fixed =====

            # 1. Missing Values
            "CreatedAt": [pd.Timestamp("2026-01-27 10:30:00"), pd.NaT],  # NaT → None
            "Score": [98.6, float('nan')],  # NaN → None
            "Optional": [pd.NA, None],  # pd.NA → None

            # 2. NumPy Scalars
            "ItemCount": [np.int64(100), np.int64(250)],  # np.int64 → int
            "Ratio": [np.float64(0.95), np.float64(0.88)],  # np.float64 → float
            "IsActive": [np.bool_(True), np.bool_(False)],  # np.bool_ → bool

            # 3. Large Integers
            "UniqueID": [np.int64(9_999_999_999), np.int64(8_888_888_888)],  # > 2^31 → EdmType.INT64

            # ===== PHASE 2: New 5 Type Categories =====

            # 4. Timedeltas
            "Duration": [pd.Timedelta('1 days'), pd.Timedelta(hours=2)],  # → ISO 8601 string

            # 5. Bytes (Binary Data)
            "BinaryData": [b'color_rgb_128_128_128', b'color_rgb_255_0_0'],  # → base64 string

            # 6. Date Objects
            "CreatedDate": [date(2026, 1, 27), date(2026, 1, 28)],  # → ISO 8601 date string

            # 7. Time Objects
            "CreatedTime": [time(10, 30, 45), time(14, 15, 30)],  # → ISO 8601 time string

            # 8. UUID Objects
            "ProcessID": [uuid.UUID('550e8400-e29b-41d4-a716-446655440000'),
                         uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')],  # → string

            # 9. Collections - Lists
            "Tags": [['primary', 'color'], ['metadata', 'system']],  # → JSON string

            # 10. Collections - Dicts
            "Properties": [
                {'hex': 'FF0000', 'rgb': '255,0,0'},
                {'hex': '808080', 'rgb': '128,128,128'}
            ],  # → JSON string

            # Additional fields for reference
            "Name": ["Red", "Gray"],
            "RGB": ["rgb(255,0,0)", "rgb(128,128,128)"],
            "HEX": ["FF0000", "808080"],
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
        print("\n✏Testing update operation...")
        dataset.input = dataset.output.copy()
        dataset.input["Score"] = [99.5, 97.0]  # Modify some data
        dataset.update()
        print("Update completed successfully!")

        # Read again to confirm update
        dataset.read()
        print("Data after update:")
        print(dataset.output)

    # Delete test
    if not dataset.output.empty and len(dataset.output) > 0:
        print("\nTesting delete operation...")
        dataset.input = dataset.output.copy()
        dataset.delete()
        print("Delete completed successfully!")

        # Verify deletion
        dataset.read()
        remaining_count = len(dataset.output)
        print(f"Remaining entities after delete: {remaining_count}")
        if remaining_count == 0:
            print("All entities deleted successfully!")
        print(dataset.output)


if __name__ == "__main__":
    main()
