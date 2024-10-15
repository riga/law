# coding: utf-8

"""
Keras target formatters.
"""

__all__ = ["KerasModelFormatter", "KerasWeightsFormatter", "TFKerasModelFormatter"]


from law.target.formatter import Formatter
from law.target.file import get_path
from law.logger import get_logger
from law.util import no_value


logger = get_logger(__name__)


class KerasModelFormatter(Formatter):

    name = "keras_model"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5", ".json", ".yaml", ".yml"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        import keras

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
    def dump(cls, path, model, *args, **kwargs):
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
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def load(cls, path, model, *args, **kwargs):
        return model.load_weights(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        perm = kwargs.pop("perm", no_value)

        ret = model.save_weights(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class TFKerasModelFormatter(Formatter):

    name = "tf_keras"

    @classmethod
    def accepts(cls, path, mode):
        return False

    @classmethod
    def load(cls, path, *args, **kwargs):
        # deprecation warning until v0.1
        logger.warning_once("law.contrib.keras.TFKerasModelFormatter is deprecated, please use "
            "law.contrib.tensorflow.TFKerasModelFormatter (named 'tf_keras_model') instead")

        import tensorflow as tf
        return tf.keras.models.load_model(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        # deprecation warning until v0.1
        logger.warning_once("law.contrib.keras.TFKerasModelFormatter is deprecated, please use "
            "law.contrib.tensorflow.TFKerasModelFormatter (named 'tf_keras_model') instead")

        perm = kwargs.pop("perm", no_value)

        model.save(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)
