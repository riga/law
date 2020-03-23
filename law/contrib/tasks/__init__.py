# coding: utf-8

"""
Tasks that provide common and often used functionality.
"""


__all__ = ["RunOnceTask", "TransferLocalFile", "CascadeMerge", "ForestMerge"]


import os
from collections import OrderedDict
from abc import abstractmethod
import logging

import luigi
import six

from law.task.base import Task
from law.workflow.base import cached_workflow_property
from law.workflow.local import LocalWorkflow
from law.target.file import FileSystemTarget
from law.target.local import LocalFileTarget
from law.target.collection import TargetCollection, SiblingFileCollection
from law.parameter import NO_INT, NO_STR
from law.decorator import factory
from law.util import iter_chunks


logger = logging.getLogger(__name__)


class RunOnceTask(Task):

    @staticmethod
    @factory(accept_generator=True)
    def complete_on_success(fn, opts, task, *args, **kwargs):
        def before_call():
            return None

        def call(state):
            return fn(task, *args, **kwargs)

        def after_call(state):
            task.mark_complete()

        return before_call, call, after_call

    def __init__(self, *args, **kwargs):
        super(RunOnceTask, self).__init__(*args, **kwargs)

        self._has_run = False

    @property
    def has_run(self):
        return self._has_run

    def mark_complete(self):
        self._has_run = True

    def complete(self):
        return self.has_run


class TransferLocalFile(Task):

    source_path = luigi.Parameter(default=NO_STR, description="path to the file to transfer")
    replicas = luigi.IntParameter(default=0, description="number of replicas to generate, uses "
        "replica_format when > 0 for creating target basenames, default: 0")

    replica_format = "{name}.{i}{ext}"

    exclude_index = True

    def get_source_target(self):
        # when self.source_path is set, return a target around it
        # otherwise assume self.requires() returns a task with a single local target
        if self.source_path not in (NO_STR, None):
            return LocalFileTarget(self.source_path)
        else:
            return self.input()

    @abstractmethod
    def single_output(self):
        return

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

    cascade_tree = luigi.IntParameter(default=0, description="the index of the cascade tree, only "
        "necessary when multiple trees (a forrest) are used, -1 denotes a wrapper that requires "
        "and outputs all trees, default: 0")
    cascade_depth = luigi.IntParameter(default=0, description="the depth of this workflow in the "
        "cascade tree with 0 being the root of the tree, default: 0")
    keep_nodes = luigi.BoolParameter(significant=False, description="keep merged results from "
        "intermediary nodes in the cascade cache directory")

    # internal parameter
    n_cascade_leaves = luigi.IntParameter(default=NO_INT, significant=False)

    # fix some workflow parameters
    acceptance = 1.
    tolerance = 0.
    pilot = False

    node_format = "{name}.d{depth}.b{branch}{ext}"
    merge_factor = 2

    exclude_index = True

    exclude_params_req_set = {"start_branch", "end_branch", "branches"}
    exclude_params_index = {"n_cascade_leaves"}

    def __init__(self, *args, **kwargs):
        super(CascadeMerge, self).__init__(*args, **kwargs)

        # deprecation warning until v0.1
        if self.is_workflow():
            logger.warning("law.tasks.CascadeMerge is deprecated, please use law.tasks.ForestMerge "
                "instead")

        # the merge factor should not be 1
        if self.merge_factor == 1:
            raise ValueError("the merge factor should not be 1")

        self._forest_built = False

    def is_branch(self, default=False):
        return super(CascadeMerge, self).is_branch() or (not default and self.is_forest())

    def max_depth(self):
        tree = self._get_tree()
        return max(tree.keys())

    def is_forest(self):
        return self.cascade_tree < 0

    def is_root(self):
        if self.is_forest():
            return False

        return self.cascade_depth == 0

    def is_leaf(self):
        if self.is_forest():
            return False

        return self.cascade_depth == self.max_depth()

    @cached_workflow_property
    def cascade_forest(self):
        self._build_cascade_forest()
        return self.cascade_forest

    @cached_workflow_property
    def leaves_per_tree(self):
        self._build_cascade_forest()
        return self.leaves_per_tree

    def _get_tree(self):
        try:
            return self.cascade_forest[self.cascade_tree]
        except IndexError:
            raise Exception("cascade tree {} not found, forest only contains {} tree(s)".format(
                self.cascade_tree, len(self.cascade_forest)))

    def _build_cascade_forest(self):
        # a node in the tree can be described by a tuple of integers, where each value denotes the
        # branch path to go down the tree to reach the node (e.g. (2, 0) -> 2nd branch, 0th branch),
        # so the length of the tuple defines the depth of the node via ``depth = len(node) - 1``
        # the tree itself is a dict that maps depths to lists of nodes with that depth
        # when multiple trees are used (a forest), each one handles ``n_leaves / n_trees`` leaves

        if self._forest_built:
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
            if self.is_branch(default=True):
                raise Exception("number of files to merge cannot be computed for a branch")
            # get inputs, i.e. outputs of workflow requirements and trace actual inputs to merge
            # an integer number representing the number of inputs is also valid
            inputs = luigi.task.getpaths(self.cascade_workflow_requires())
            inputs = self.trace_cascade_workflow_inputs(inputs)
            self.n_cascade_leaves = inputs if isinstance(inputs, six.integer_types) else len(inputs)

        # infer the number of trees from the cascade output
        output = self.cascade_output()
        n_trees = 1 if not isinstance(output, TargetCollection) else len(output)

        if self.n_cascade_leaves < n_trees:
            raise Exception("too few leaves ({}) for number of requested trees ({})".format(
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
        self.leaves_per_tree = leaves_per_tree
        self.cascade_forest = forest
        self._forest_built = True

        # complain when the cascade_depth is too large
        max_depth = self.max_depth()
        if self.cascade_depth > max_depth:
            raise ValueError("cascade_depth too large, maximum depth is {}".format(max_depth))

    def create_branch_map(self):
        if self.is_forest():
            raise Exception("cannot define a branch map when in forest mode (cascade_tree < 0)")

        tree = self._get_tree()
        nodes = tree[self.cascade_depth]
        return dict(enumerate(nodes))

    def trace_cascade_workflow_inputs(self, inputs):
        # should convert inputs to an object with a length (e.g. list, tuple, TargetCollection, ...)

        # for convenience, check if inputs results from the default workflow output, i.e. a dict
        # which stores a TargetCollection in the "collection" field
        if isinstance(inputs, dict) and "collection" in inputs:
            collection = inputs["collection"]
            if isinstance(collection, TargetCollection):
                return collection

        return inputs

    def trace_cascade_inputs(self, inputs):
        # should convert inputs into an iterable sequence (list, tuple, ...), no TargetCollection!
        return inputs

    @abstractmethod
    def cascade_workflow_requires(self):
        # should return the leaf requirements of a cascading task workflow
        return

    @abstractmethod
    def cascade_requires(self, start_leaf, end_leaf):
        # should return the leaf requirements of a cascading task branch
        return

    @abstractmethod
    def cascade_output(self):
        # this should return a single target when the output should be a single tree
        # or a target collection whose targets are accessible as items via cascade tree numbers
        return

    @abstractmethod
    def merge(self, inputs, output):
        return

    def workflow_requires(self):
        self._build_cascade_forest()

        reqs = super(CascadeMerge, self).workflow_requires()

        if self.is_leaf():
            # this is simply the cascade requirement
            reqs["cascade"] = self.cascade_workflow_requires()

        else:
            # not a leaf, just require the next cascade depth
            reqs["cascade"] = self.req(self, cascade_depth=self.cascade_depth + 1)

        return reqs

    def requires(self):
        reqs = OrderedDict()

        if self.is_forest():
            # require the workflows for all cascade trees
            n_trees = len(self.cascade_forest)
            reqs["forest"] = {t: self.req(self, branch=-1, cascade_tree=t) for t in range(n_trees)}

        elif self.is_leaf():
            # this is simply the cascade requirement
            # also determine and pass the corresponding leaf number range
            n_leaves = self.leaves_per_tree[self.cascade_tree]
            offset = sum(self.leaves_per_tree[:self.cascade_tree])
            merge_factor = self.merge_factor
            if merge_factor <= 0:
                merge_factor = n_leaves
            start_leaf = offset + self.branch * merge_factor
            end_leaf = min(start_leaf + merge_factor, offset + n_leaves)
            reqs["cascade"] = self.cascade_requires(start_leaf, end_leaf)

        else:
            # get all child nodes in the next layer at depth = depth + 1, store their branches
            # note: child node tuples contain the exact same values plus an additional one
            tree = self._get_tree()
            node = self.branch_data
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

        if self.is_forest():
            return output

        if isinstance(output, TargetCollection):
            output = output[self.cascade_tree]

        if self.is_root():
            return output

        else:
            name, ext = os.path.splitext(output.basename)
            basename = self.node_format.format(name=name, ext=ext, branch=self.branch,
                depth=self.cascade_depth)
            return self.cascade_cache_directory().child(basename, "f")

    def run(self):
        if self.is_forest():
            return

        # trace actual inputs to merge
        inputs = self.input()["cascade"]
        if self.is_leaf():
            inputs = self.trace_cascade_inputs(inputs)
        else:
            inputs = inputs.values()

        # merge
        self.publish_message("start merging {} inputs of node {}".format(
            len(inputs), self.branch_data))
        self.merge(inputs, self.output())

        # remove intermediate nodes
        if not self.is_leaf() and not self.keep_nodes:
            with self.publish_step("removing intermediate results of node {}".format(
                    self.branch_data)):
                for inp in inputs:
                    inp.remove()


class ForestMerge(LocalWorkflow):

    branch = luigi.IntParameter(default=0, description="the branch number/index to run this "
        "task for, -1 means this task is the workflow, default: 0")
    tree_index = luigi.IntParameter(default=-1, description="the index of the merged tree in the "
        "forest, -1 denotes the forest itself which requires and outputs all trees, default: -1")
    tree_depth = luigi.IntParameter(default=0, description="the depth of this workflow in the "
        "merge tree, 0 denotes the root, default: 0")
    keep_nodes = luigi.BoolParameter(significant=False, description="keep merged results, i.e., "
        "task outputs from intermediate nodes in the merge tree, default: False")

    # fix some workflow parameters
    acceptance = 1.
    tolerance = 0.
    pilot = False

    node_format = "{name}.d{depth}.b{branch}{ext}"
    merge_factor = 2

    exclude_index = True

    exclude_params_req_set = {"start_branch", "end_branch", "branches"}

    @classmethod
    def modify_param_values(cls, params):
        # when tree_index is negative which refers to the merge forest, make sure this is branch 0
        if "tree_index" in params and "branch" in params and params["tree_index"] < 0:
            params["branch"] = 0

        return params

    @classmethod
    def _req_set_n_leaves(cls, inst, *args, **kwargs):
        new_inst = super(ForestMerge, cls).req(inst, *args, **kwargs)
        new_inst._n_leaves = inst._n_leaves
        return new_inst

    def __init__(self, *args, **kwargs):
        super(ForestMerge, self).__init__(*args, **kwargs)

        self._n_leaves = None
        self._forest_built = False

        # the merge factor should not be 1
        if self.merge_factor == 1:
            raise ValueError("the merge factor must not be 1")

        # modify_param_values prevents the forest from being a workflow, but still check
        if self.is_forest() and self.is_workflow():
            raise Exception("the merge forest must not be a workflow, ForestMerge misconfigured")

        # since the forest counts as a branch, as_workflow should point the tree_index 0
        # which is only used to compute the overall merge tree
        if self.is_forest():
            self._workflow_task = self.req(self, branch=-1, tree_index=0,
                _exclude=self.exclude_params_workflow)

    def is_forest(self):
        return self.tree_index < 0

    def is_root(self):
        return not self.is_forest() and self.tree_depth == 0

    def is_leaf(self):
        return not self.is_forest() and self.tree_depth == self.max_depth

    @property
    def max_depth(self):
        return max(self._get_tree().keys())

    @cached_workflow_property
    def merge_forest(self):
        self._build_merge_forest()
        return self.merge_forest

    @cached_workflow_property
    def leaves_per_tree(self):
        self._build_merge_forest()
        return self.leaves_per_tree

    def _get_tree(self):
        if self.is_forest():
            raise Exception("merge tree cannot be determined for the merge forest, ForestMerge "
                "misconfigured")

        try:
            return self.merge_forest[self.tree_index]
        except IndexError:
            raise Exception("merge tree {} not found, forest only contains {} tree(s)".format(
                self.tree_index, len(self.merge_forest)))

    def _build_merge_forest(self):
        # a node in the tree can be described by a tuple of integers, where each value denotes the
        # branch path to go down the tree to reach the node (e.g. (2, 0) -> 2nd branch, 0th branch),
        # so the length of the tuple defines the depth of the node via ``depth = len(node) - 1``
        # the tree itself is a dict that maps depths to lists of nodes with that depth
        # when multiple trees are used (a forest), each one handles ``n_leaves / n_trees`` leaves

        if self._forest_built:
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
        if self._n_leaves is None:
            # the following lines build the workflow requirements,
            # which strictly requires this task to be a workflow (and also not the forest)
            # for branches, this block is executed anyway via cached workflow properties
            if self.is_branch():
                raise Exception("number of files to merge should not be computed for a branch")

            # get inputs, i.e. outputs of workflow requirements and trace actual inputs to merge
            # an integer number representing the number of inputs is also valid
            inputs = luigi.task.getpaths(self.merge_workflow_requires())
            inputs = self.trace_merge_workflow_inputs(inputs)
            self._n_leaves = inputs if isinstance(inputs, six.integer_types) else len(inputs)

        # infer the number of trees from the merge output
        output = self.merge_output()
        n_trees = 1 if not isinstance(output, TargetCollection) else len(output)

        if self._n_leaves < n_trees:
            raise Exception("too few leaves ({}) for number of requested trees ({})".format(
                self._n_leaves, n_trees))

        # determine the number of leaves per tree
        n_min = self._n_leaves // n_trees
        n_trees_overlap = self._n_leaves % n_trees
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
        self.leaves_per_tree = leaves_per_tree
        self.merge_forest = forest
        self._forest_built = True

        # complain when the depth is too large
        if self.tree_depth > self.max_depth:
            raise ValueError("tree_depth {} exceeds maximum depth {}".format(
                self.tree_depth, self.max_depth))

    def create_branch_map(self):
        tree = self._get_tree()
        nodes = tree[self.tree_depth]
        return dict(enumerate(nodes))

    def trace_merge_workflow_inputs(self, inputs):
        # should convert inputs to an object with a length (e.g. list, tuple, TargetCollection, ...)

        # for convenience, check if inputs results from the default workflow output, i.e. a dict
        # which stores a TargetCollection in the "collection" field
        if isinstance(inputs, dict) and "collection" in inputs:
            collection = inputs["collection"]
            if isinstance(collection, TargetCollection):
                return collection

        return inputs

    def trace_merge_inputs(self, inputs):
        # should convert inputs into an iterable sequence (list, tuple, ...), no TargetCollection!
        return inputs

    @abstractmethod
    def merge_workflow_requires(self):
        # should return the requirements of the merge workflow
        return

    @abstractmethod
    def merge_requires(self, start_leaf, end_leaf):
        # should return the requirements of a merge task, depending on the leaf range
        return

    @abstractmethod
    def merge_output(self):
        # this should return a single target when the output should be a single tree
        # or a target collection whose targets are accessible as items via merge tree indices
        return

    @abstractmethod
    def merge(self, inputs, output):
        return

    def workflow_requires(self):
        self._build_merge_forest()

        reqs = super(ForestMerge, self).workflow_requires()

        if self.is_forest():
            raise Exception("workflow requirements cannot be determined for the merge forest, "
                "ForestMerge misconfigured")

        elif self.is_leaf():
            # this is simply the merge workflow requirement
            reqs["forest_merge"] = self.merge_workflow_requires()

        else:
            # intermediate node, just require the next tree depth
            reqs["forest_merge"] = self._req_set_n_leaves(self, tree_depth=self.tree_depth + 1)

        return reqs

    def requires(self):
        reqs = OrderedDict()

        if self.is_forest():
            n_trees = len(self.merge_forest)
            reqs["forest_merge"] = {
                i: self._req_set_n_leaves(self, branch=-1, tree_index=i)
                for i in range(n_trees)
            }

        elif self.is_leaf():
            # this is simply the merge requirement
            # also determine and pass the corresponding leaf number range,
            # where 0 refers to the overall first element to merge
            n_leaves = self.leaves_per_tree[self.tree_index]
            offset = sum(self.leaves_per_tree[:self.tree_index])
            merge_factor = self.merge_factor
            if merge_factor <= 0:
                merge_factor = n_leaves
            start_leaf = offset + self.branch * merge_factor
            end_leaf = min(start_leaf + merge_factor, offset + n_leaves)
            reqs["forest_merge"] = self.merge_requires(start_leaf, end_leaf)

        else:
            # get all child nodes in the next layer at depth = depth + 1 and store their branches
            # note: child node tuples contain the exact same values plus an additional one
            tree = self._get_tree()
            node = self.branch_data
            branches = [i for i, n in enumerate(tree[self.tree_depth + 1]) if n[:-1] == node]

            # add to requirements
            reqs["forest_merge"] = {
                b: self._req_set_n_leaves(self, branch=b, tree_depth=self.tree_depth + 1)
                for b in branches
            }

        return reqs

    def output(self):
        output = self.merge_output()

        if self.is_forest():
            return output

        if isinstance(output, TargetCollection):
            output = output[self.tree_index]

        if self.is_root():
            return output

        else:
            name, ext = os.path.splitext(output.basename)
            basename = self.node_format.format(name=name, ext=ext, branch=self.branch,
                depth=self.tree_depth)
            return self._merge_cache_directory().child(basename, "f")

    def run(self):
        # nothing to do for the forest
        if self.is_forest():
            return

        # trace actual inputs to merge
        inputs = self.input()["forest_merge"]
        if self.is_leaf():
            inputs = self.trace_merge_inputs(inputs)
        else:
            inputs = inputs.values()

        # merge
        self.publish_message("start merging {} inputs of node {}".format(
            len(inputs), self.branch_data))
        self.merge(inputs, self.output())

        # remove intermediate nodes
        if not self.is_leaf() and not self.keep_nodes:
            with self.publish_step("removing intermediate results of node {}".format(
                    self.branch_data)):
                for inp in inputs:
                    inp.remove()

    def _merge_cache_directory(self):
        # by default, use the targets parent directory, also for SinglingFileCollections
        # otherwise, no default decision is implemented
        output = self.merge_output()
        if isinstance(output, FileSystemTarget):
            return output.parent
        elif isinstance(output, SiblingFileCollection):
            return output.dir
        else:
            raise NotImplementedError("{}._merge_cache_directory is not implemented for cases "
                "the merge output is neither a FileSystemTarget nor a SiblingFileCollection".format(
                    self.__class__.__name__))
