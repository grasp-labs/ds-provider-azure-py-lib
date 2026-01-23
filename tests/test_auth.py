"""
**File:** ``test_auth.py``
**Region:** ``tests/test_auth``

Azure auth  tests.

Covers:
- get_client_credential
- get_azure_named_key_credential
- get_credential

"""

import unittest
from unittest.mock import MagicMock, patch

from azure.core.credentials import AzureNamedKeyCredential

from ds_provider_azure_py_lib.linked_service.auth import AzureAuth


class MyTestCase(unittest.TestCase):
    def test_creating_azure_auth_instance_with_no_credentials_passed(self):
        azure_auth = AzureAuth()
        self.assertIsInstance(azure_auth, AzureAuth)

    def test_creating_azure_auth_instance_with_access_key_only(self):
        # given
        access_key = "123abc"
        # then
        azure_auth = AzureAuth(access_key=access_key)
        # then
        assert azure_auth.access_key == access_key
        self.assertRaises(ValueError, azure_auth.get_client_credential)

    def test_get_azure_named_key_credential_returns_credential_instance(self):
        # given
        access_key = "123abc"
        account_name = "testaccount"
        # when
        azure_auth = AzureAuth(access_key=access_key)
        result = azure_auth.get_azure_named_key_credential(account_name)
        # then
        self.assertIsInstance(result, AzureNamedKeyCredential)
        self.assertEqual(result.named_key.name, account_name)
        self.assertEqual(result.named_key.key, access_key)

    def test_creating_azure_auth_instance_with_azure_credentials_passed(self):
        tenant_id = "tenant_id"
        client_id = "client_id"
        client_secret = "client_secret"
        azure_auth = AzureAuth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        self.assertIsInstance(azure_auth, AzureAuth)
        self.assertEqual(azure_auth.tenant_id, tenant_id)
        self.assertEqual(azure_auth.client_id, client_id)
        self.assertEqual(azure_auth.client_secret, client_secret)

    @patch("azure.core.credentials.AzureNamedKeyCredential")
    def test_get_azure_named_key_credential_with_mock(self, mock_credential):
        access_key = "mock_access_key"
        account_name = "mock_account_name"

        mock_instance = MagicMock()
        mock_instance.named_key = MagicMock()
        mock_instance.named_key.name = account_name
        mock_instance.named_key.key = access_key
        mock_credential.return_value = mock_instance

        azure_auth = AzureAuth(access_key=access_key)

        result = azure_auth.get_azure_named_key_credential(account_name)

        # assert the constructor was called with keyword arguments
        mock_credential.assert_called_once_with(name=account_name, key=access_key)
        self.assertEqual(result.named_key.name, account_name)
        self.assertEqual(result.named_key.key, access_key)

    @patch("azure.identity.ClientSecretCredential")
    def test_get_credential_with_azure_credentials(self, mock_cred):
        tenant_id = "00000000-0000-0000-0000-000000000000"
        client_id = "client_id"
        client_secret = "client_secret"

        mock_instance = MagicMock()
        mock_instance.tenant_id = tenant_id
        mock_instance.client_id = client_id
        mock_instance.client_secret = client_secret
        mock_cred.return_value = mock_instance

        azure_auth = AzureAuth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

        credential = azure_auth.get_credential()

        mock_cred.assert_called_once()
        args, kwargs = mock_cred.call_args
        if kwargs:
            self.assertEqual(kwargs.get("tenant_id"), tenant_id)
            self.assertEqual(kwargs.get("client_id"), client_id)
            self.assertEqual(kwargs.get("client_secret"), client_secret)
        else:
            self.assertEqual(args, (tenant_id, client_id, client_secret))

        self.assertIs(credential, mock_instance)
        self.assertEqual(credential.tenant_id, tenant_id)
        self.assertEqual(credential.client_id, client_id)
        self.assertEqual(credential.client_secret, client_secret)

    def test_get_credential_access_key_without_account_name_raises_type_error(self):
        azure_auth = AzureAuth(access_key="123abc")
        with self.assertRaises(TypeError):
            # calling without the required account_name argument
            azure_auth.get_credential()

    def test_get_azure_named_key_credential(self):
        access_key = "123abc"
        account_name = "testaccount"

        azure_auth = AzureAuth(access_key=access_key)
        credential = azure_auth.get_azure_named_key_credential(account_name)

        self.assertIsInstance(credential, AzureNamedKeyCredential)
        self.assertEqual(credential.named_key.name, account_name)
        self.assertEqual(credential.named_key.key, access_key)

    def test_get_client_credential_requires_all_fields(self):
        # missing client_secret
        auth = AzureAuth(tenant_id="t_id", client_id="c_id")
        with self.assertRaises(ValueError) as cm:
            auth.get_client_credential()
        self.assertIn("Tenant ID, Client ID, and Client Secret", str(cm.exception))

        # missing tenant_id
        auth = AzureAuth(client_id="c_id", client_secret="secret")
        with self.assertRaises(ValueError):
            auth.get_client_credential()

    def test_get_azure_named_key_credential_requires_access_key(self):
        auth = AzureAuth()
        with self.assertRaises(ValueError) as cm:
            auth.get_azure_named_key_credential("account")
        self.assertIn("Access Key is required", str(cm.exception))

    def test_get_credential_returns_none_when_no_credentials(self):
        auth = AzureAuth()
        self.assertIsNone(auth.get_credential())


if __name__ == "__main__":
    unittest.main()
