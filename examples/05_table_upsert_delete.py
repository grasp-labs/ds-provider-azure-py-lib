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

import pandas as pd
from ds_resource_plugin_py_lib.common.resource.errors import ValidationError

from ds_provider_azure_py_lib.dataset import AzureTable, AzureTableDatasetSettings
from ds_provider_azure_py_lib.dataset.table import AzureTableSerializer, AzureTableDeserializer, ReadSettings
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

    print("No entities found, creating new test data...")

    test_data = {
        "PartitionKey": ["colors", "colors"],
        "RowKey": ["3", "2"],
        "StringColumn": ["green", "blue"],
        "IntColumn": [67, 84],
    }

    dataset.input = pd.DataFrame(test_data)

    print("\nTest DataFrame Created:")
    print(dataset.input)
    print("\nDataFrame Data Types:")
    print(dataset.input.dtypes)

    print("\nUpserting entities to table...")
    dataset.upsert()

    print("Entities created successfully!")
    print("\nReading created entities back from table...")
    dataset.read()
    print(dataset.output)

    dataset.input = pd.DataFrame({
        "PartitionKey": ["colors"],
        "RowKey": ["9"],
        "StringColumn": ["abcd"],
        "IntColumn": [99],
    })
    dataset.delete()
    print("This should raise warning")

    try:
        dataset.input = pd.DataFrame({
            "PartitionKey": ["colors"],
        })
        dataset.delete()
    except ValidationError:
        print("Validation error raised as expected for missing RowKey in delete operation")
    dataset.input = pd.DataFrame({
        "PartitionKey": ["colors"],
        "RowKey": ["2"],
    })
    dataset.delete()

    print("\nEntity  deleted successfully!")
    print("\nReading remaining entities from table...")
    dataset.read()
    print(dataset.output)


if __name__ == "__main__":
    main()
