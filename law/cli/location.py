# coding: utf-8

"""
"law location" cli subprogram.
"""


from law.util import law_src_path


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *location* subprogram and adds it to *sub_parsers*.
    """
    sub_parsers.add_parser("location", prog="law location", description="Print the location of the "
        "law installation directory.")


def execute(args):
    """
    Executes the *location* subprogram with parsed commandline *args*.
    """
    print(law_src_path())
