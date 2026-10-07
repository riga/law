"""
Keras target formatters.
"""

from __future__ import annotations

__all__ = ["KerasModelFormatter", "KerasWeightsFormatter"]

import pathlib

from law._types import Any
from law.logger import get_logger
from law.target.file import FileSystemFileTarget, get_path
from law.target.formatter import Formatter
from law.util import no_value

logger = get_logger(__name__)


class KerasModelFormatter(Formatter):
    """
    Formatter for keras models, stored in hdf5 (``.hdf5``, ``.h5``), json (``.json``) or yaml files (``.yaml``,
    ``.yml``). Hdf5 files contain the full model and are handled by ``keras.models.load_model`` and ``model.save``,
    whereas json and yaml files only contain the model architecture. Additional arguments are forwarded. When dumping,
    the file permission can be set via *perm*. Its name is ``"keras_model"``, which can be passed as *formatter* to
    select it explicitly.
    """

    name = "keras_model"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".hdf5", ".h5", ".json", ".yaml", ".yml"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import keras

        path = get_path(path)

        # the method for loading the model depends on the file extension
        if path.endswith(".json"):
            with open(path, encoding="utf-8") as f:
                return keras.models.model_from_json(f.read(), *args, **kwargs)

        if path.endswith((".yml", ".yaml")):
            with open(path, encoding="utf-8") as f:
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
            with open(_path, "w", encoding="utf-8") as f:
                f.write(model.to_json(*args, **kwargs))

        elif _path.endswith((".yml", ".yaml")):
            with open(_path, "w", encoding="utf-8") as f:
                f.write(model.to_yaml(*args, **kwargs))

        else:  # .hdf5, .h5, bundle
            ret = model.save(_path, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class KerasWeightsFormatter(Formatter):
    """
    Formatter for weights of keras models in hdf5 files (``.hdf5``, ``.h5``). Both ``load`` and ``dump`` expect the
    model as their first argument and call its ``load_weights`` and ``save_weights`` methods, respectively. Additional
    arguments are forwarded. When dumping, the file permission can be set via *perm*. Its name is ``"keras_weights"``,
    which can be passed as *formatter* to select it explicitly.
    """

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
