"""
**File:** ``__init__.py``
**Region:** ``ds_provider_azure_py_lib/serde``

Serde for Azure Table Storage data.
"""

from .table import AzureTableDeserializer, AzureTableSerializer

__all__ = ["AzureTableDeserializer", "AzureTableSerializer"]
