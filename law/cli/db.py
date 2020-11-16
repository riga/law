# coding: utf-8

"""
Temporary subprogram that logs an error when "law db" is started.
This was renamed to "law index". To be removed for v0.1.
"""


import sys
import logging


logger = logging.getLogger(__name__)


def setup_parser(sub_parsers):
    sub_parsers.add_parser("db", prog="law db", description="Renamed to 'law index'!")


def execute(args):
    logger.error("The 'law db' command was renamed. Use 'law index' instead!")
    logger.error("Also consider to rename 'exclude_db' to 'exclude_index' in your tasks.")
    sys.exit(1)
