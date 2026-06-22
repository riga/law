# ruff: noqa: I001

"""
Awkward array contrib functionality.
"""

__all__ = ["AwkwardFormatter", "from_parquet"]

# provisioning imports
from law.contrib.awkward.formatter import AwkwardFormatter
from law.contrib.awkward.util import from_parquet
