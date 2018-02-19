# -*- coding: utf-8 -*-

"""
TensorFlow target formatters.
"""


import os

from law.target.formatter import Formatter
from law.target.file import get_path


class TFConstantGraphFormatter(Formatter):

    name = "tf_const_graph"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".pb")

    @classmethod
    def load(cls, path, graph_def=None):
        import tensorflow as tf
        from tensorflow.python.platform import gfile

        if not graph_def:
            graph_def = tf.GraphDef()

        with gfile.FastGFile(get_path(path), "rb") as f:
            graph_def.ParseFromString(f.read())

        return graph_def

    @classmethod
    def dump(cls, path, session, output_names, *args, **kwargs):
        import tensorflow as tf

        graph_dir, graph_name = os.path.split(get_path(path))

        const_graph = tf.graph_util.convert_variables_to_constants(
            session, session.graph.as_graph_def(), output_names)
        tf.train.write_graph(const_graph, graph_dir, graph_name, *args, **kwargs)
