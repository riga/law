# coding: utf-8

"""
TensorFlow target formatters.
"""


__all__ = ["TFConstantGraphFormatter"]


import os

from law.target.formatter import Formatter
from law.target.file import get_path


class TFConstantGraphFormatter(Formatter):

    name = "tf_const_graph"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".pb")

    @classmethod
    def load(cls, path, graph=None, graph_def=None, as_text=False):
        import tensorflow as tf

        if not graph:
            graph = tf.Graph()

        with graph.as_default():
            if not graph_def:
                graph_def = tf.GraphDef()

            if as_text:
                from google.protobuf import text_format
                with open(get_path(path), "r") as f:
                    text_format.Merge(f.read(), graph_def)
            else:
                from tensorflow.python.platform import gfile
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
