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

from ds_provider_azure_py_lib.linked_service import AzureLinkedService, AzureLinkedServiceSettings


def main():
    linked_service = AzureLinkedService(
        settings=AzureLinkedServiceSettings(
            account_name="...",
            access_key="...",
        )
    )

    blob_service_client, table_service_client = linked_service.connect()
    tables = table_service_client.list_tables()

    print("Tables:")
    for table in tables:
        print(f" -{table.name}")

    containers = blob_service_client.list_containers()
    print("\nContainers:")
    for container in containers:
        print(f" -{container.name}")

    test_container = blob_service_client.get_container_client("test-blob")
    blobs = test_container.list_blobs()
    print("\nBlobs in 'test-blob' container:")
    for blob in blobs:
        print(f" -{blob.name}")


if __name__ == "__main__":
    main()
