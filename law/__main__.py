#!/usr/bin/env python
# coding: utf-8


if __name__ == "__main__":
    # amend sys.path to ensure the law package relative to _this_ file is imported
    import os
    import sys

    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parent_dir)

    # load the law cli
    from law.cli import run
    run()
