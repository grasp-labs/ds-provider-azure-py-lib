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

from ds_provider_azure_py_lib.dataset import AzureTable, AzureTableDatasetSettings
from ds_provider_azure_py_lib.dataset.table import AzureTableSerializer, AzureTableDeserializer, ReadSettings, \
    DeleteSettings
from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings


def main():
    dataset = AzureTable(
        serializer=AzureTableSerializer(), # can be omitted as the default serializer is AzureTableSerializer
        deserializer=AzureTableDeserializer(), # can be omitted as the default deserializer is AzureTableDeserializer
        settings=AzureTableDatasetSettings(
            table_name="testazurepackage",
            read=ReadSettings(query_filter="PartitionKey eq '1'"),
            delete=DeleteSettings(delete_table=True),
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
        print(row)
    else:
        dataset.input = pd.DataFrame({
            "Name": ["Grays", "Red"],
            "RGB": ["rgb(128,128,128)", "rgb(255,0,0)"],
            "HEX": ["808080", "FF0000"],
            "Timestamp": ["2026-01-27T21:17:02.7102891Z", "2026-01-28T21:17:02.7102891Z"],
            "PartitionKey": ["1", "1"],
            "RowKey": ["2", "4"],
        })
        dataset.update()
        dataset.read()
        print(dataset.output)

    dataset.input = dataset.output
    print(f"dataset input length:  {len(dataset.input)}")
    dataset.delete()
    dataset.read()
    print(dataset.output)


if __name__ == "__main__":
    main()
