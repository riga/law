"""
Pytest configuration, loaded before any test module is imported.
"""

from __future__ import annotations

import os

this_dir = os.path.dirname(os.path.abspath(__file__))

# define the luigi and law configs, before luigi or law are imported by any test module
os.environ["LUIGI_CONFIG_PATH"] = os.path.join(this_dir, "luigi.cfg")
os.environ["LAW_CONFIG_FILE"] = os.path.join(this_dir, "law.cfg")
