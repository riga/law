# -*- coding: utf-8 -*-

"""
law command line interface that prints the location of the completion.sh file.
"""


import os
from argparse import ArgumentParser

import law


def main():
    parser = ArgumentParser(description="law completion file lookup")
    parser.parse_args()

    print(os.path.join(os.path.dirname(os.path.dirname(law.__file__)), "completion.sh"))


if __name__ == "__main__":
    main()
