"""
**File:** ``__init__.py``
**Region:** ``ds-provider-azure-py-lib``

Description
-----------
A Python package from the ds-provider-azure-py-lib library.

Example
-------
.. code-block:: python

    from ds_provider_azure_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

__version__ = version("ds-provider-azure-py-lib")
__all__ = ["__version__"]
