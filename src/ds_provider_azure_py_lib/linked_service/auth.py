from dataclasses import dataclass
from typing import Any

import azure.core.credentials as azure_credentials
import azure.identity as azure_identity


@dataclass(kw_only=True)
class AzureAuth:
    """
    Abstract base class for all authentication methods.
    """

    # Azure AD Service Principal authentication
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    # Access Key authentication
    access_key: str | None = None

    def get_client_credential(self) -> azure_identity.ClientSecretCredential:
        if not (self.tenant_id and self.client_id and self.client_secret):
            raise ValueError("Tenant ID, Client ID, and Client Secret are required for Azure AD authentication.")

        return azure_identity.ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    def get_azure_named_key_credential(self, account_name: str) -> Any:
        """
        Args:
            account_name: Name of the storage account.

        Returns:
            An AzureNamedKeyCredential instance (or a mocked equivalent).
        """

        if not self.access_key:
            raise ValueError("Access Key is required for Azure Named Key authentication.")

        return azure_credentials.AzureNamedKeyCredential(
            name=account_name,
            key=self.access_key,
        )

    def get_credential(self, *args: Any, **kwargs: Any) -> Any:
        """
        Retrieve the appropriate credential for Azure Storage.
        This method must be implemented by all subclasses.
        """

        if self.tenant_id or self.client_id or self.client_secret:
            return self.get_client_credential()
        if self.access_key:
            return self.get_azure_named_key_credential(*args, **kwargs)
        return None
