# coding: utf-8

"""
Keras target formatters.
"""

from __future__ import annotations

__all__ = ["KerasModelFormatter", "KerasWeightsFormatter"]

import pathlib

from law.target.formatter import Formatter
from law.target.file import FileSystemFileTarget, get_path
from law.logger import get_logger
from law.util import no_value
from law._types import Any


logger = get_logger(__name__)


class KerasModelFormatter(Formatter):

    name = "keras_model"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".hdf5", ".h5", ".json", ".yaml", ".yml"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import keras  # type: ignore[import-untyped, import-not-found]

        path = get_path(path)

        # the method for loading the model depends on the file extension
        if path.endswith(".json"):
            with open(path, "r") as f:
                return keras.models.model_from_json(f.read(), *args, **kwargs)

        if path.endswith((".yml", ".yaml")):
            with open(path, "r") as f:
                return keras.models.model_from_yaml(f.read(), *args, **kwargs)

        # .hdf5, .h5, bundle
        return keras.models.load_model(path, *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, model, *args, **kwargs) -> Any:
        _path = get_path(path)
        perm = kwargs.pop("perm", no_value)

        # the method for saving the model depends on the file extension
        ret = None
        if _path.endswith(".json"):
            with open(_path, "w") as f:
                f.write(model.to_json(*args, **kwargs))

        elif _path.endswith((".yml", ".yaml")):
            with open(_path, "w") as f:
                f.write(model.to_yaml(*args, **kwargs))

        else:  # .hdf5, .h5, bundle
            ret = model.save(_path, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class KerasWeightsFormatter(Formatter):

    name = "keras_weights"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def load(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        model: Any,
        *args,
        **kwargs,
    ) -> Any:
        return model.load_weights(get_path(path), *args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        model: Any,
        *args,
        **kwargs,
    ) -> Any:
        perm = kwargs.pop("perm", no_value)

        ret = model.save_weights(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
