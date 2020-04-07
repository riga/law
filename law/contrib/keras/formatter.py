# coding: utf-8

"""
Keras target formatters.
"""


__all__ = ["KerasModelFormatter", "KerasWeightsFormatter", "TFKerasModelFormatter"]


import logging

from law.target.formatter import Formatter
from law.target.file import get_path


logger = logging.getLogger(__name__)


class KerasModelFormatter(Formatter):

    name = "keras_model"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5", ".json", ".yaml", ".yml"))

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        path = get_path(path)

        # the method for saving the model depends on the file extension
        if path.endswith((".hdf5", ".h5")):
            return model.save(path, *args, **kwargs)
        elif path.endswith(".json"):
            with open(path, "w") as f:
                f.write(model.to_json())
        else:  # .yml, .yaml
            with open(path, "w") as f:
                f.write(model.to_yaml())

    @classmethod
    def load(cls, path, *args, **kwargs):
        import keras

        path = get_path(path)

        # the method for loading the model depends on the file extension
        if path.endswith((".hdf5", ".h5")):
            return keras.models.load_model(path, *args, **kwargs)
        elif path.endswith(".json"):
            with open(path, "r") as f:
                return keras.models.model_from_json(f.read(), *args, **kwargs)
        else:  # .yml, .yaml
            with open(path, "r") as f:
                return keras.models.model_from_yaml(f.read(), *args, **kwargs)


class KerasWeightsFormatter(Formatter):

    name = "keras_weights"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        return model.save_weights(get_path(path), *args, **kwargs)

    @classmethod
    def load(cls, path, model, *args, **kwargs):
        return model.load_weights(get_path(path), *args, **kwargs)


class TFKerasModelFormatter(Formatter):

    name = "tf_keras"

    @classmethod
    def accepts(cls, path, mode):
        return False

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        # deprecation warning until v0.1
        logger.warning("law.contrib.keras.TFKerasModelFormatter is deprecated, please use "
            "law.contrib.tensorflow.TFKerasModelFormatter (named 'tf_keras_model') instead")

        model.save(get_path(path), *args, **kwargs)

    @classmethod
    def load(cls, path, *args, **kwargs):
        # deprecation warning until v0.1
        logger.warning("law.contrib.keras.TFKerasModelFormatter is deprecated, please use "
            "law.contrib.tensorflow.TFKerasModelFormatter (named 'tf_keras_model') instead")

        import tensorflow as tf
        return tf.keras.models.load_model(get_path(path), *args, **kwargs)
