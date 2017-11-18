# -*- coding: utf-8 -*-

"""
"law completion" command line tool
"""


import os

import law


def setup_parser(sub_parsers):
    sub_parsers.add_parser("completion", prog="law completion",
        description="law completion file lookup")


def execute(args):
    print(os.path.join(os.path.dirname(law.__file__), "completion.sh"))
