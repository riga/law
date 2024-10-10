# coding: utf-8

"""
Matplotlib target formatter.
"""

__all__ = ["MatplotlibFormatter"]


from law.target.formatter import Formatter
from law.target.file import get_path, FileSystemTarget


class MatplotlibFormatter(Formatter):

    name = "mpl"

    @classmethod
    def accepts(cls, path, mode):
        # only dumping supported
        return mode == "dump" and get_path(path).endswith((".pdf", ".png"))

    @classmethod
    def dump(cls, path, fig, *args, **kwargs):
        fig.savefig(get_path(path), *args, **kwargs)
        if isinstance(path, FileSystemTarget):
            path.chmod(path.fs.default_file_perm)
