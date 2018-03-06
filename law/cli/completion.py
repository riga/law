# -*- coding: utf-8 -*-

"""
"law completion" cli subprogram.
"""


from law.util import law_src_path


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *completion* subprogram and adds it to *sub_parsers*.
    """
    sub_parsers.add_parser("completion", prog="law completion",
        description="law completion file lookup")


def execute(args):
    """
    Executes the *completion* subprogram with parsed commandline *args*.
    """
    print(law_src_path("cli", "completion.sh"))
