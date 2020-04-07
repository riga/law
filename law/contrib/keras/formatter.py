# coding: utf-8

"""
Keras target formatters.
"""


__all__ = ["KerasModelFormatter", "TFKerasModelFormatter"]


import logging

from law.target.formatter import Formatter
from law.target.file import get_path


logger = logging.getLogger(__name__)


class KerasModelFormatter(Formatter):

    name = "keras"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        from keras.models import load_model
        return load_model(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        model.save(path, *args, **kwargs)


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

        model.save(path, *args, **kwargs)

    @classmethod
    def load(cls, path, *args, **kwargs):
        # deprecation warning until v0.1
        logger.warning("law.contrib.keras.TFKerasModelFormatter is deprecated, please use "
            "law.contrib.tensorflow.TFKerasModelFormatter (named 'tf_keras_model') instead")

        import tensorflow as tf
        return tf.keras.models.load_model(path, *args, **kwargs)
