"""
**File:** ``01_linked_service_connect_and_list_resources.py``
**Region:** ``examples/01_linked_service_connect_and_list_resources``

Example 01: Connect to Azure storage using AzureLinkedService and list Blob containers and Table storage tables.

This example demonstrates how to:
- Create an instance of `AzureLinkedService` with the necessary settings.
- Connect to Azure Blob Storage and Azure Table Storage using the linked service.
- List all tables in the Table storage.
- List all Blob containers in the storage account.
"""
import os
import uuid

from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings


def main():
    account_name = os.environ.get("ACCOUNT_NAME")
    account_key = os.environ.get("ACCOUNT_KEY")

    linked_service = AzureLinkedService(
        settings=AzureLinkedServiceSettings(
            account_name=account_name,
            access_key=account_key,
        ),
        id=uuid.uuid4(),
        name="testazurepackage",
        version="0.0.1",
        description="testazurepackage"
    )

    linked_service.connect()
    linked_service.test_connection()
    tables = linked_service.table_service_client.list_tables()

    print("Tables:")
    for table in tables:
        print(f" -{table.name}")

    containers = linked_service.blob_service_client.list_containers()
    print("\nContainers:")
    container_name = None
    for container in containers:
        print(f" -{container.name}")
        container_name = container.name

    if container_name:
        test_container = linked_service.blob_service_client.get_container_client(container_name)
        if test_container.exists():
            blobs = test_container.list_blobs()
            print(f"\nBlobs in {container_name} container:")
            for blob in blobs:
                print(f" -{blob.name}")

if __name__ == "__main__":
    main()
