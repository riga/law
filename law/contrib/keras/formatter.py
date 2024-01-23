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
from law._types import Any


logger = get_logger(__name__)


class KerasModelFormatter(Formatter):

    name = "keras_model"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return str(get_path(path)).endswith((".hdf5", ".h5", ".json", ".yaml", ".yml"))

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, model, *args, **kwargs) -> Any:
        path = str(get_path(path))

        # the method for saving the model depends on the file extension
        if path.endswith(".json"):
            with open(path, "w") as f:
                f.write(model.to_json(*args, **kwargs))
            return

        if path.endswith((".yml", ".yaml")):
            with open(path, "w") as f:
                f.write(model.to_yaml(*args, **kwargs))
            return

        # .hdf5, .h5, bundle
        return model.save(path, *args, **kwargs)

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import keras  # type: ignore[import-untyped, import-not-found]

        path = str(get_path(path))

        # the method for loading the model depends on the file extension
        if path.endswith(".json"):
            with open(path, "r") as f:
                return keras.models.model_from_json(f.read(), *args, **kwargs)

        if path.endswith((".yml", ".yaml")):
            with open(path, "r") as f:
                return keras.models.model_from_yaml(f.read(), *args, **kwargs)

        # .hdf5, .h5, bundle
        return keras.models.load_model(path, *args, **kwargs)


class KerasWeightsFormatter(Formatter):

    name = "keras_weights"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return str(get_path(path)).endswith((".hdf5", ".h5"))

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        model: Any,
        *args,
        **kwargs,
    ) -> Any:
        return model.save_weights(get_path(path), *args, **kwargs)

    @classmethod
    def load(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        model: Any,
        *args,
        **kwargs,
    ) -> Any:
        return model.load_weights(get_path(path), *args, **kwargs)
