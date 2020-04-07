# coding: utf-8

"""
TensorFlow target formatters.
"""


__all__ = ["TFConstantGraphFormatter", "TFKerasModelFormatter", "TFKerasWeightsFormatter"]


import os

from law.target.formatter import Formatter
from law.target.file import get_path


class TFConstantGraphFormatter(Formatter):

    name = "tf_const_graph"

    @classmethod
    def import_tf(cls):
        import tensorflow as tf

        # keep a reference to the v1 API as long as v2 provides compatibility
        tf1 = None
        if tf.__version__.startswith("1."):
            tf1 = tf
        elif getattr(tf, "compat", None) and getattr(tf.compat, "v1"):
            tf1 = tf.compat.v1

        return tf, tf1

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".pb", ".pbtxt", ".pb.txt"))

    @classmethod
    def load(cls, path, create_session=None, as_text=None):
        """
        Reads a saved TensorFlow graph from *path* and returns it. When *create_session* is *True*,
        a session object (compatible with the v1 API) is created and returned as the second value of
        a 2-tuple. The default value of *create_session* is *True* when TensorFlow v1 is detected,
        and *False* otherwise. When *as_text* is *True*, or *None* and the file extension is
        ``".pbtxt"`` or ``".pb.txt"``, the content of the file at *path* is expected to be a
        human-readable text file. Otherwise, it is expected to be a binary protobuf file. Example:

        .. code-block:: python

            graph = TFConstantGraphFormatter.load("path/to/model.pb", create_session=False)

            graph, session = TFConstantGraphFormatter.load("path/to/model.pb", create_session=True)
        """
        tf, tf1 = cls.import_tf()
        path = get_path(path)

        # default create_session value
        if create_session is None:
            create_session = tf1 is not None

        # default as_text value
        if as_text is None:
            as_text = path.endswith((".pbtxt", ".pb.txt"))

        graph = tf.Graph()
        with graph.as_default():
            graph_def = graph.as_graph_def()

            if as_text:
                # use a simple pb reader to load the file into graph_def
                from google.protobuf import text_format
                with open(path, "r") as f:
                    text_format.Merge(f.read(), graph_def)

            else:
                # use the gfile api depending on the TF version
                if tf1:
                    from tensorflow.python.platform import gfile
                    with gfile.FastGFile(path, "rb") as f:
                        graph_def.ParseFromString(f.read())
                else:
                    with tf.io.gfile.GFile(path, "rb") as f:
                        graph_def.ParseFromString(f.read())

            # import the graph_def (pb object) into the actual graph
            tf.import_graph_def(graph_def, name="")

        if create_session:
            if not tf1:
                raise NotImplementedError("the v1 compatibility layer of TensorFlow v2 is missing, "
                    "but required by when create_session is True")
            session = tf1.Session(graph=graph)
            return graph, session
        else:
            return graph

    @classmethod
    def dump(cls, path, session, output_names, *args, **kwargs):
        """
        Takes a TensorFlow *session* object (compatible with the v1 API), converts its contained
        graph into a simpler version with variables translated into constant tensors, and saves it
        to a protobuf file at *path*. *output_numes* must be a list of names of output tensors to
        save. In turn, TensorFlow internally determines which subgraph(s) to convert and save. All
        *args* and *kwargs* are forwarded to :py:func:`tf.compat.v1.train.write_graph`.

        .. note::

            When used with TensorFlow v2, this function requires the v1 API compatibility layer.
            When :py:attr:`tf.compat.v1` is not available, a *NotImplementedError* is raised.
        """
        _, tf1 = cls.import_tf()
        path = get_path(path)

        # complain when the v1 compatibility layer is not existing
        if not tf1:
            raise NotImplementedError("the v1 compatibility layer of TensorFlow v2 is missing, but "
                "required")

        # convert the graph
        constant_graph = tf1.graph_util.convert_variables_to_constants(session,
            session.graph.as_graph_def(), output_names)

        # default as_text value
        kwargs.setdefault("as_text", path.endswith((".pbtxt", ".pb.txt")))

        # write the graph
        graph_dir, graph_name = os.path.split(path)
        return tf1.train.write_graph(constant_graph, graph_dir, graph_name, *args, **kwargs)


class TFKerasModelFormatter(Formatter):

    name = "tf_keras_model"

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
        import tensorflow as tf

        path = get_path(path)

        # the method for loading the model depends on the file extension
        if path.endswith((".hdf5", ".h5")):
            return tf.keras.models.load_model(path, *args, **kwargs)
        elif path.endswith(".json"):
            with open(path, "r") as f:
                return tf.keras.models.model_from_json(f.read(), *args, **kwargs)
        else:  # .yml, .yaml
            with open(path, "r") as f:
                return tf.keras.models.model_from_yaml(f.read(), *args, **kwargs)


class TFKerasWeightsFormatter(Formatter):

    name = "tf_keras_weights"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def dump(cls, path, model, *args, **kwargs):
        return model.save_weights(get_path(path), *args, **kwargs)

    @classmethod
    def load(cls, path, model, *args, **kwargs):
        return model.load_weights(get_path(path), *args, **kwargs)
