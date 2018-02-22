# -*- coding: utf-8 -*-

"""
Tasks that provide common and often used functionality.
"""


__all__ = ["TransferLocalFile", "CascadeMerge"]


import os
from collections import OrderedDict
from abc import abstractmethod

import luigi
import six

from law import Task, LocalWorkflow, FileSystemTarget, LocalFileTarget, TargetCollection, \
    SiblingFileCollection, cached_workflow_property, NO_INT
from law.decorator import log
from law.util import iter_chunks


class TransferLocalFile(Task):

    source_path = luigi.Parameter(description="path to the file to transfer")
    replicas = luigi.IntParameter(default=0, description="number of replicas to generate, uses "
        "replica_format when > 0 for creating target basenames, default: 0")

    replica_format = "{name}.{i}{ext}"

    exclude_db = True

    def get_source_target(self):
        # when self.source_path is set, return a target around it
        # otherwise assume self.requires() returns a task with a single local target
        return LocalFileTarget(self.source_path) if self.source_path else self.input()

    @abstractmethod
    def single_output(self):
        pass

    def output(self):
        output = self.single_output()
        if self.replicas <= 0:
            return output

        # prepare replica naming
        name, ext = os.path.splitext(output.basename)
        basename = lambda i: self.replica_format.format(name=name, ext=ext, i=i)

        # return the replicas in a SiblingFileCollection
        output_dir = output.parent
        return SiblingFileCollection([
            output_dir.child(basename(i), "f") for i in six.moves.range(self.replicas)
        ])

    @log
    def run(self):
        self.transfer(self.get_source_target())

    def transfer(self, src_path):
        output = self.output()

        # single output or replicas?
        if not isinstance(output, SiblingFileCollection):
            output.copy_from_local(src_path, cache=False)
        else:
            # upload all replicas
            progress_callback = self.create_progress_callback(self.replicas)
            for i, replica in enumerate(output.targets):
                replica.copy_from_local(src_path, cache=False)
                progress_callback(i)
                self.publish_message("uploaded {}".format(replica.basename))


class CascadeMerge(LocalWorkflow):

    cascade_tree = luigi.IntParameter(default=0, description="the index of the cascade tree, in "
        "case multiple trees (a forrest) are used, default: 0")
    cascade_depth = luigi.IntParameter(default=0, description="the depth of this workflow in the "
        "cascade tree with 0 being the root of the tree, default: 0")
    keep_nodes = luigi.BoolParameter(significant=False, description="keep merged results from "
        "intermediary nodes in the cascade cache directory")

    # internal parameter
    n_cascade_leaves = luigi.IntParameter(default=NO_INT, significant=False)

    # fixate some workflow parameters
    acceptance = 1.
    tolerance = 0.
    pilot = False

    node_format = "{name}.d{depth}.b{branch}{ext}"
    merge_factor = 2

    exclude_params_db = {"n_cascade_leaves"}

    exclude_db = True

    def __init__(self, *args, **kwargs):
        super(CascadeMerge, self).__init__(*args, **kwargs)

        # the merge factor should not be 1
        if self.merge_factor == 1:
            raise ValueError("the merge factor should not be 1")

        self._build = False

    @cached_workflow_property
    def cascade_forest(self):
        self._build_cascade_forest()
        return self._cascade_forest

    @cached_workflow_property
    def leaves_per_tree(self):
        self._build_cascade_forest()
        return self._leaves_per_tree

    def _build_cascade_forest(self):
        # a node in the tree can be described by a tuple of integers, where each value denotes the
        # branch path to go down the tree to reach the node (e.g. (2, 0) -> 2nd branch, 0th branch),
        # so the length of the tuple defines the depth of the node via ``depth = len(node) - 1``
        # the tree itself is a dict that maps depths to lists of nodes with that depth
        # when multiple trees are used (a forest), each one handles ``n_leaves / n_trees`` leaves

        if self._build:
            return

        # helper to convert nested lists of leaf number chunks into a list of nodes in the format
        # described above
        def nodify(obj, node=None, root_id=0):
            if not isinstance(obj, list):
                return []
            nodes = []
            if node is None:
                node = tuple()
            else:
                nodes.append(node)
            for i, _obj in enumerate(obj):
                nodes += nodify(_obj, node + (i if node else root_id,))
            return nodes

        # first, determine the number of files to merge in total when not already set via params
        if self.n_cascade_leaves == NO_INT:
            # get inputs, i.e. outputs of workflow requirements and trace actual inputs to merge
            # an integer number representing the number of inputs is also valid
            inputs = luigi.task.getpaths(self.cascade_workflow_requires())
            inputs = self.trace_cascade_workflow_inputs(inputs)
            self.n_cascade_leaves = inputs if isinstance(inputs, six.integer_types) else len(inputs)

        # infer the number of trees from the cascade output
        output = self.cascade_output()
        n_trees = 1 if not isinstance(output, TargetCollection) else len(output)

        if self.n_cascade_leaves < n_trees:
            raise Exception("too many leaves ({}) for number of requested trees ({})".format(
                self.n_cascade_leaves, n_trees))

        # determine the number of leaves per tree
        n_min = self.n_cascade_leaves // n_trees
        n_trees_overlap = self.n_cascade_leaves % n_trees
        leaves_per_tree = n_trees_overlap * [n_min + 1] + (n_trees - n_trees_overlap) * [n_min]

        # build the trees
        forest = []
        for i, n_leaves in enumerate(leaves_per_tree):
            # build a nested list of leaf numbers using the merge factor
            # e.g. 9 leaves with factor 3 -> [[0, 1, 2], [3, 4, 5], [6, 7, 8]]
            # TODO: this point defines the actual tree structure, which is bottom-up at the moment,
            # but maybe it's good to have this configurable
            nested_leaves = list(iter_chunks(n_leaves, self.merge_factor))
            while len(nested_leaves) > 1:
                nested_leaves = list(iter_chunks(nested_leaves, self.merge_factor))

            # convert the list of nodes to the tree format described above
            tree = {}
            for node in nodify(nested_leaves, root_id=i):
                depth = len(node) - 1
                tree.setdefault(depth, []).append(node)

            forest.append(tree)

        # store values
        self._leaves_per_tree = leaves_per_tree
        self._cascade_forest = forest
        self._build = True

    def create_branch_map(self):
        tree = self.cascade_forest[self.cascade_tree]
        nodes = tree[self.cascade_depth]
        return dict(enumerate(nodes))

    @property
    def is_root(self):
        return self.cascade_depth == 0

    @property
    def is_leaf(self):
        tree = self.cascade_forest[self.cascade_tree]
        max_depth = max(tree.keys())
        return self.cascade_depth == max_depth

    def trace_cascade_workflow_inputs(self, inputs):
        # should convert inputs to an object with a length (e.g. list, tuple, TargetCollection, ...)
        return inputs

    def trace_cascade_inputs(self, inputs):
        # should convert inputs into an iterable sequence (list, tuple, ...), no TargetCollection!
        return inputs

    @abstractmethod
    def cascade_workflow_requires(self):
        # should return the leaf requirements of a cascading task workflow
        pass

    @abstractmethod
    def cascade_requires(self):
        # should return the leaf requirements of a cascading task branch
        pass

    @abstractmethod
    def cascade_output(self):
        # this should return a single target to explicitely denote a single tree
        # or a target collection whose targets are accessible as items via the tree numbers
        pass

    @abstractmethod
    def merge(self, inputs, output):
        pass

    def workflow_requires(self):
        reqs = super(CascadeMerge, self).workflow_requires()

        if self.is_leaf:
            # this is simply the cascade requirement
            reqs["cascade"] = self.cascade_workflow_requires()

        else:
            # not a leaf, just require the next cascade depth
            reqs["cascade"] = self.req(self, cascade_depth=self.cascade_depth + 1)

        return reqs

    def requires(self):
        reqs = OrderedDict()

        if self.is_leaf:
            # this is simply the cascade requirement
            # also determine and pass the corresponding leaf number range
            self.cascade_forest
            sum_n_leaves = sum(self.leaves_per_tree)
            offset = sum(self.leaves_per_tree[:self.cascade_tree])
            merge_factor = self.merge_factor
            if merge_factor <= 0:
                merge_factor = self.leaves_per_tree[self.cascade_tree]
            start_leaf = offset + self.branch * merge_factor
            end_leaf = min(start_leaf + merge_factor, sum_n_leaves)
            reqs["cascade"] = self.cascade_requires(start_leaf, end_leaf)

        else:
            # get all child nodes in the next layer at depth = depth + 1, store their branches
            # note: child node tuples contain the exact same values plus an additional one
            node = self.branch_data
            tree = self.cascade_forest[self.cascade_tree]
            branches = [i for i, n in enumerate(tree[self.cascade_depth + 1]) if n[:-1] == node]

            # add to requirements
            reqs["cascade"] = {
                b: self.req(self, branch=b, cascade_depth=self.cascade_depth + 1) for b in branches
            }

        return reqs

    def cascade_cache_directory(self):
        # by default, use the targets parent directory, also for SinglingFileCollections
        # otherwise, no default decision is implemented
        output = self.cascade_output()
        if isinstance(output, FileSystemTarget):
            return output.parent
        elif isinstance(output, SiblingFileCollection):
            return output.dir
        else:
            raise NotImplementedError("{}.cascade_cache_directory is not implemented".format(
                self.__class__.__name__))

    def output(self):
        output = self.cascade_output()
        if isinstance(output, TargetCollection):
            output = output[self.cascade_tree]

        if self.is_root:
            return output

        else:
            name, ext = os.path.splitext(output.basename)
            basename = self.node_format.format(name=name, ext=ext, branch=self.branch,
                depth=self.cascade_depth)
            return self.cascade_cache_directory().child(basename, "f")

    @log
    def run(self):
        # trace actual inputs to merge
        inputs = self.input()["cascade"]
        if self.is_leaf:
            inputs = self.trace_cascade_inputs(inputs)
        else:
            inputs = inputs.values()

        # merge
        self.publish_message("start merging {} inputs of node {}".format(
            len(inputs), self.branch_data))
        self.merge(inputs, self.output())

        # remove intermediate nodes
        if not self.is_leaf and not self.keep_nodes:
            with self.publish_step("removing intermediate results to node {}".format(
                    self.branch_data)):
                for inp in inputs:
                    inp.remove()
