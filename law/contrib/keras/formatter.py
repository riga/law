# coding: utf-8

"""
Keras target formatters.
"""

from law.target.formatter import Formatter
from law.target.file import get_path


class ModelFormatter(Formatter):

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".h5")

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        model.save(path, *args, **kwargs)


class KerasModelFormatter(ModelFormatter):

    name = "keras"

    @classmethod
    def load(cls, path, *args, **kwargs):
        from keras.models import load_model
        return load_model(path, *args, **kwargs)


class TFKerasModelFormatter(ModelFormatter):

    name = "tf_keras"

    @classmethod
    def load(cls, path, *args, **kwargs):
        from tensorflow import keras
        return keras.models.load_model(path, *args, **kwargs)
