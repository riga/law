# coding: utf-8

"""
"law luigid" cli subprogram.
"""


from law.config import Config


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *luigid* subprogram and adds it to *sub_parsers*.
    This is identical to running ``luigid`` directly from the command line, but this also loads the
    law config prior to starting the scheduler process and optionally syncrhonizes it with luigi's
    config.
    """
    parser = sub_parsers.add_parser(
        "luigid",
        prog="law luigid",
        description="Starts the 'luigid' central scheduler.",
        add_help=False,
    )

    parser.add_argument(
        "arguments",
        nargs="*",
        help="luigid arguments",
    )


def execute(args, argv):
    """
    Initializes the law config object and executes the *luigid* subprogram with parsed commandline
    *args*.
    """
    # initialize the config
    Config.instance()

    # forward to luigid
    from luigi.cmdline import luigid
    luigid(argv[2:])
