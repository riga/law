# -*- coding: utf-8 -*-

"""
Matplotlib target formatter.
"""


from law.target.formatter import Formatter
from law.target.file import get_path


class MatplotlibFormatter(Formatter):

    name = "mpl"

    @classmethod
    def accepts(cls, path):
        path = get_path(path)
        return path.endswith(".pdf") or path.endswith(".png")

    @classmethod
    def dump(cls, path, fig, *args, **kwargs):
        fig.savefig(get_path(path), *args, **kwargs)
