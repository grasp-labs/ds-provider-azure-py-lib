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
import uuid
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
        azure_linked_service = AzureLinkedService(
            settings=mock_settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1"
        )
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
        azure_linked_service = AzureLinkedService(
            settings=mock_settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1"
        )
        # then
        self.assertIs(azure_linked_service.settings, mock_settings)

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

        svc = AzureLinkedService(settings=mock_settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        ok, message = svc.test_connection()
        self.assertFalse(ok)
        self.assertIn("boom", message)

    def test_close_is_noop(self):
        """
        Test that close method is a no-op and returns None.
        """
        mock_settings = MagicMock(spec=AzureLinkedServiceSettings)
        mock_settings.__class__ = AzureLinkedServiceSettings
        mock_settings.account_name = "my_account"
        mock_settings.access_key = "123"
        svc = AzureLinkedService(settings=mock_settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        # close should not raise and return None
        self.assertIsNone(svc.close())

    def _mock_settings(self, account_name="my_account", access_key="123") -> AzureLinkedServiceSettings:
        settings = AzureLinkedServiceSettings(
            account_name=account_name,
            access_key=access_key,
        )
        return settings

    def test_check_settings_missing_access_key_raises(self):
        settings = self._mock_settings(access_key="")
        svc = AzureLinkedService(settings=settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        with self.assertRaises(AuthenticationError):
            svc.check_settings_is_set()

    def test_check_settings_missing_account_name_raises(self):
        settings = self._mock_settings(account_name="")
        svc = AzureLinkedService(settings=settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        with self.assertRaises(AuthenticationError):
            svc.check_settings_is_set()

    def test_blob_service_client_without_connect_raises(self):
        svc = AzureLinkedService(settings=self._mock_settings(), id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        with self.assertRaises(ConnectionError):
            _ = svc.blob_service_client

    def test_table_service_client_without_connect_raises(self):
        svc = AzureLinkedService(settings=self._mock_settings(), id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        with self.assertRaises(ConnectionError):
            _ = svc.table_service_client

    @patch("ds_provider_azure_py_lib.linked_service.storage_account.TableServiceClient")
    @patch("ds_provider_azure_py_lib.linked_service.storage_account.BlobServiceClient")
    def test_connect_blob_and_table_create_clients(self, mock_blob_client_cls, mock_table_client_cls):
        mock_blob_client = MagicMock()
        mock_table_client = MagicMock()
        mock_blob_client_cls.return_value = mock_blob_client
        mock_table_client_cls.return_value = mock_table_client
        settings = self._mock_settings()
        svc = AzureLinkedService(settings=settings, id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        svc.connect()
        mock_blob_client_cls.assert_called_with(
            account_url="https://my_account.blob.core.windows.net/",
            credential=svc._credential,
        )
        mock_table_client_cls.assert_called_with(
            endpoint="https://my_account.table.core.windows.net/",
            credential=svc._credential,
        )
        self.assertIsInstance(svc._credential, AzureNamedKeyCredential)
        self.assertIs(svc.blob_service_client, mock_blob_client)
        self.assertIs(svc.table_service_client, mock_table_client)

    @patch("ds_provider_azure_py_lib.linked_service.storage_account.TableServiceClient")
    @patch("ds_provider_azure_py_lib.linked_service.storage_account.BlobServiceClient")
    def test_test_connection_success(self, mock_blob_client_cls, mock_table_client_cls):
        mock_blob_client = MagicMock()
        mock_table_client = MagicMock()
        mock_blob_client.list_containers.return_value = []
        mock_table_client.list_tables.return_value = []

        mock_blob_client_cls.return_value = mock_blob_client
        mock_table_client_cls.return_value = mock_table_client
        svc = AzureLinkedService(settings=self._mock_settings(), id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        ok, message = svc.test_connection()

        self.assertTrue(ok)
        self.assertEqual(message, "Connection successful.")
        mock_blob_client.list_containers.assert_called_once()
        mock_table_client.list_tables.assert_called_once()

    @patch("ds_provider_azure_py_lib.linked_service.storage_account.BlobServiceClient", side_effect=Exception("boom"))
    def test_test_connection_failure_returns_false_and_error2(self, _):
        svc = AzureLinkedService(settings=self._mock_settings(), id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        ok, message = svc.test_connection()
        self.assertFalse(ok)
        self.assertIn("boom", message)

    def test_close_is_noop2(self):
        svc = AzureLinkedService(settings=self._mock_settings(), id=uuid.uuid4(), name="testazurepackage", version="0.0.1")
        self.assertIsNone(svc.close())

    def test_check_settings_is_set_raises_attribute_error(self):
        svc = AzureLinkedService(
            settings=object(), id=uuid.uuid4(), name="testazurepackage", version="0.0.1"
        )  # Invalid settings type
        self.assertRaises(AttributeError, svc.check_settings_is_set)
