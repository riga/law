# coding: utf-8

"""
Matplotlib target formatter.
"""

__all__ = ["MatplotlibFormatter"]


from law.target.formatter import Formatter
from law.target.file import get_path
from law.util import no_value


class MatplotlibFormatter(Formatter):

    name = "mpl"

    @classmethod
    def accepts(cls, path, mode):
        # only dumping supported
        return mode == "dump" and get_path(path).endswith((".pdf", ".png"))

    @classmethod
    def dump(cls, path, fig, *args, **kwargs):
        perm = kwargs.pop("perm", no_value)

        fig.savefig(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)
