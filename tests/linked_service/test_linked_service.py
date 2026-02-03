"""
**File:** ``test_linked_service.py``
**Region:** ``tests/linked_service/test_linked_service``

Unit tests for AzureLinkedService class.

covers:
- Initialization and property access.
- Credential creation.
- Connection methods for Blob and Table services.
- Connection testing with success and failure scenarios.
"""

import unittest
from unittest.mock import MagicMock, patch

from azure.core.credentials import AzureNamedKeyCredential
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError

from ds_provider_azure_py_lib.enums import ResourceType
from ds_provider_azure_py_lib.linked_service.storage_account import AzureLinkedService, AzureLinkedServiceSettings


class AzureLinkedServiceTests(unittest.TestCase):
    def test_type(self):
        """
        Test the type property of AzureLinkedService.
        """
        # given
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings

        # provide account_name and an auth.get_credential return value
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"
        # when
        azure_linked_service = AzureLinkedService(settings=mock_settings)
        # then
        self.assertEqual(azure_linked_service.type, ResourceType.STORAGE_ACCOUNT)

    def test_settings(self):
        """
        Test the settings property of AzureLinkedService.
        """
        # given
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings

        # provide account_name and an auth.get_credential return value
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"

        # when
        azure_linked_service = AzureLinkedService(settings=mock_settings)
        # then
        self.assertIs(azure_linked_service.settings, mock_settings)

    def test_credential(self):
        """
        Test the credential property of AzureLinkedService.
        """
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"
        # when
        azure_linked_service = AzureLinkedService(settings=mock_settings)
        # then
        self.assertEqual(azure_linked_service.credential.named_key.key, "123")
        self.assertIsInstance(azure_linked_service.credential, AzureNamedKeyCredential)

    @patch("ds_provider_azure_py_lib.linked_service.storage_account.BlobServiceClient")
    @patch("ds_provider_azure_py_lib.linked_service.storage_account.TableServiceClient")
    def test_connect_blob_and_table_create_clients(self, mock_table_client_cls, mock_blob_client_cls):
        """
        Test connecting to Blob and Table services creates the clients.
        Args:
            mock_table_client_cls:
            mock_blob_client_cls:
        """
        # given
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"

        mock_blob_client = MagicMock()
        mock_table_client = MagicMock()
        mock_blob_client.account_name = "my_account"
        mock_table_client.account_name = "my_account"

        mock_blob_client_cls.return_value = mock_blob_client
        mock_table_client_cls.return_value = mock_table_client

        svc = AzureLinkedService(settings=mock_settings)

        # when
        svc.connect()

        mock_blob_client_cls.assert_called_with(
            account_url=f"https://{mock_settings.account_name}.blob.core.windows.net/",
            credential=svc.credential,
        )
        mock_table_client_cls.assert_called_with(
            endpoint=f"https://{mock_settings.account_name}.table.core.windows.net/",
            credential=svc.credential,
        )
        # then
        self.assertIs(svc.blob_service_client, mock_blob_client)
        self.assertIs(svc.table_service_client, mock_table_client)

    @patch("ds_provider_azure_py_lib.linked_service.storage_account.BlobServiceClient")
    @patch("ds_provider_azure_py_lib.linked_service.storage_account.TableServiceClient")
    def test_connect_and_test_connection_success(self, mock_table_client_cls, mock_blob_client_cls):
        """
        Test connecting to Blob and Table services and testing connection succeeds.
        Args:
            mock_table_client_cls:
            mock_blob_client_cls:
        """
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"

        mock_blob_client = MagicMock()
        mock_table_client = MagicMock()
        # account_name is used in the success message
        mock_blob_client.account_name = "blob_account"
        mock_table_client.account_name = "table_account"

        mock_blob_client_cls.return_value = mock_blob_client
        mock_table_client_cls.return_value = mock_table_client

        svc = AzureLinkedService(settings=mock_settings)
        # connect uses both if None and returns tuple
        blob_client, table_client = svc.connect()
        self.assertIs(blob_client, mock_blob_client)
        self.assertIs(table_client, mock_table_client)

        ok, message = svc.test_connection()
        self.assertTrue(ok)
        self.assertIn("blob_account", message)
        self.assertIn("table_account", message)

    @patch("ds_provider_azure_py_lib.linked_service.storage_account.BlobServiceClient", side_effect=Exception("boom"))
    def test_test_connection_failure_returns_false_and_error(self, mock_blob_client_cls):
        """
        Test that test_connection returns False and error message on failure.
        Args:
            mock_blob_client_cls:
        """
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"

        svc = AzureLinkedService(settings=mock_settings)
        ok, message = svc.test_connection()
        self.assertFalse(ok)
        self.assertIn("boom", message)

    def test_check_settings_is_set_raises_when_wrong_type(self):
        """
        Test that initializing AzureLinkedService with wrong settings type raises AttributeError.
        """
        # settings not an AzureLinkedServiceSettings instance -> should raise on init
        bad_settings = MagicMock()
        with self.assertRaises(AttributeError):
            AzureLinkedService(settings=bad_settings)  # type: ignore

    def test_close_is_noop(self):
        """
        Test that close method is a no-op and returns None.
        """
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"
        svc = AzureLinkedService(settings=mock_settings)
        # close should not raise and return None
        self.assertIsNone(svc.close())

    def test_raises_auth_error_when_no_key_provided(self):
        """
        Test that initializing AzureLinkedService without access_key raises AuthenticationError.
        """
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = ""  # No access key provided

        with self.assertRaises(AuthenticationError):
            AzureLinkedService(settings=mock_settings)
