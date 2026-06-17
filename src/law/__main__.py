#!/usr/bin/env python
# coding: utf-8

import os
import sys


def main() -> None:
    # amend sys.path to ensure the law package relative to _this_ file is imported
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parent_dir)

    # load the law cli
    from law.cli import run
    run()


if __name__ == "__main__":
    main()
