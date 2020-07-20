# coding: utf-8

"""
TensorFlow target formatters.
"""


__all__ = ["TFGraphFormatter", "TFKerasModelFormatter", "TFKerasWeightsFormatter"]


import os

from law.target.formatter import Formatter
from law.target.file import get_path


class TFGraphFormatter(Formatter):

    name = "tf_graph"

    @classmethod
    def import_tf(cls):
        import tensorflow as tf

        # keep a reference to the v1 API as long as v2 provides compatibility
        tf1 = None
        tf_version = tf.__version__.split(".", 2)
        if tf_version[0] == "1":
            tf1 = tf
        elif getattr(tf, "compat", None) is not None and getattr(tf.compat, "v1", None) is not None:
            tf1 = tf.compat.v1

        return tf, tf1, tf_version

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".pb", ".pbtxt", ".pb.txt"))

    @classmethod
    def load(cls, path, create_session=None, as_text=None):
        """
        Reads a saved TensorFlow graph from *path* and returns it. When *create_session* is *True*,
        a session object (compatible with the v1 API) is created and returned as the second value of
        a 2-tuple. The default value of *create_session* is *True* when TensorFlow v1 is detected,
        and *False* otherwise. When *as_text* is either *True*, or *None* and the file extension is
        ``".pbtxt"`` or ``".pb.txt"``, the content of the file at *path* is expected to be a
        human-readable text file. Otherwise, it is read as a binary protobuf file. Example:

        .. code-block:: python

            graph = TFConstantGraphFormatter.load("path/to/model.pb", create_session=False)

            graph, session = TFConstantGraphFormatter.load("path/to/model.pb", create_session=True)
        """
        tf, tf1, tf_version = cls.import_tf()
        path = get_path(path)

        # default create_session value
        if create_session is None:
            create_session = tf_version[0] == "1"
        if create_session and not tf1:
            raise NotImplementedError("the v1 compatibility layer of TensorFlow v2 is missing, "
                "but required by when create_session is True")

        # default as_text value
        if as_text is None:
            as_text = path.endswith((".pbtxt", ".pb.txt"))

        graph = tf.Graph()
        with graph.as_default():
            graph_def = graph.as_graph_def()

            if as_text:
                # use a simple pb reader to load the file into graph_def
                from google.protobuf import text_format
                with open(path, "rb") as f:
                    text_format.Merge(f.read(), graph_def)

            else:
                # use the gfile api depending on the TF version
                if tf_version[0] == "1":
                    from tensorflow.python.platform import gfile
                    with gfile.FastGFile(path, "rb") as f:
                        graph_def.ParseFromString(f.read())
                else:
                    with tf.io.gfile.GFile(path, "rb") as f:
                        graph_def.ParseFromString(f.read())

            # import the graph_def (pb object) into the actual graph
            tf.import_graph_def(graph_def, name="")

        if create_session:
            session = tf1.Session(graph=graph)
            return graph, session
        else:
            return graph

    @classmethod
    def dump(cls, path, obj, variables_to_constants=False, output_names=None, *args, **kwargs):
        """
        Extracts a TensorFlow graph from an object *obj* and saves it at *path*. The graph is
        optionally transformed into a simpler representation with all its variables converted to
        constants when *variables_to_constants* is *True*. The saved file contains the graph as a
        protobuf. The accepted types of *obj* greatly depend on the available API versions.

        When the v1 API is found (which is also the case when ``tf.compat.v1`` is available in v2),
        ``Graph``, ``GraphDef`` and ``Session`` objects are accepted. However, when
        *variables_to_constants* is *True*, *obj* must be a session and *output_names* should refer
        to names of operations whose subgraphs are extracted (usually just one).

        For TensorFlow v2, *obj* can also be a compiled keras model, or either a polymorphic or
        concrete function as returned by ``tf.function``. Polymorphic functions either must have a
        defined input signature (``tf.function(input_signature=(...,))``) or they must accept no
        arguments in the first place. See the TensorFlow documentation on `concrete functions
        <https://www.tensorflow.org/guide/concrete_function>`__ for more info.

        *args* and *kwargs* are forwarded to ``tf.train.write_graph`` (v1) or ``tf.io.write_graph``
        (v2).
        """
        tf, tf1, tf_version = cls.import_tf()
        path = get_path(path)
        graph_dir, graph_name = os.path.split(path)

        # default as_text value
        kwargs.setdefault("as_text", path.endswith((".pbtxt", ".pb.txt")))

        # convert keras models and polymorphic functions to concrete functions, v2 only
        if tf_version[0] != "1":
            from tensorflow.python.keras.saving import saving_utils
            from tensorflow.python.eager.def_function import Function
            from tensorflow.python.eager.function import ConcreteFunction

            if isinstance(obj, tf.keras.Model):
                learning_phase_orig = tf.keras.backend.learning_phase()
                tf.keras.backend.set_learning_phase(False)
                model_func = saving_utils.trace_model_call(obj)
                if model_func.function_spec.arg_names and not model_func.input_signature:
                    raise ValueError("when obj is a keras model callable accepting arguments, its "
                        "input signature must be frozen by building the model")
                obj = model_func.get_concrete_function()
                tf.keras.backend.set_learning_phase(learning_phase_orig)

            elif isinstance(obj, Function):
                if obj.function_spec.arg_names and not obj.input_signature:
                    raise ValueError("when obj is a polymorphic function accepting arguments, its "
                        "input signature must be frozen")
                obj = obj.get_concrete_function()

        # convert variables to constants
        if variables_to_constants:
            if tf1 and isinstance(obj, tf1.Session):
                if not output_names:
                    raise ValueError("when variables_to_constants is true, output_names must "
                        "contain operations to export, got '{}' instead".format(output_names))
                obj = tf1.graph_util.convert_variables_to_constants(obj, obj.graph.as_graph_def(),
                    output_names)

            elif tf_version[0] != "1":
                from tensorflow.python.framework import convert_to_constants

                if not isinstance(obj, ConcreteFunction):
                    raise TypeError("when variables_to_constants is true, obj must be a concrete "
                        "or polymorphic function, got '{}' instead".format(obj))
                obj = convert_to_constants.convert_variables_to_constants_v2(obj)

            else:
                raise TypeError("cannot convert variables to constants for object '{}', type not "
                    "understood for TensorFlow version {}".format(obj, tf.__version__))

        # extract the graph
        if tf1 and isinstance(obj, tf1.Session):
            graph = obj.graph
        elif tf_version[0] != "1" and isinstance(obj, ConcreteFunction):
            graph = obj.graph
        else:
            graph = obj

        # write it
        if tf_version[0] == "1":
            return tf1.train.write_graph(graph, graph_dir, graph_name, *args, **kwargs)
        else:
            return tf.io.write_graph(graph, graph_dir, graph_name, *args, **kwargs)


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
