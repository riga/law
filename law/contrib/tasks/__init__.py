# coding: utf-8

"""
Tasks that provide common and often used functionality.
"""

from __future__ import annotations

__all__ = ["RunOnceTask", "TransferLocalFile", "ForestMerge"]

import os
import pathlib
import abc

import luigi  # type: ignore[import-untyped]

from law.task.base import Task
from law.workflow.local import LocalWorkflow
from law.target.file import FileSystemTarget, FileSystemFileTarget
from law.target.local import LocalFileTarget
from law.target.collection import TargetCollection, SiblingFileCollection
from law.parameter import NO_STR
from law.decorator import factory
from law.util import iter_chunks, flatten, map_struct, range_expand, DotDict
from law.logger import get_logger
from law._types import Callable, Any, Sequence, Iterator


logger = get_logger(__name__)


class RunOnceTask(Task):

    @staticmethod
    @factory(accept_generator=True)
    def complete_on_success(
        fn: Callable,
        opts: dict[str, Any],
        task: Task,
        *args,
        **kwargs,
    ) -> tuple[Callable, Callable, Callable]:
        def before_call() -> None:
            return None

        def call(state: None):
            return fn(task, *args, **kwargs)

        def after_call(state: None) -> None:
            task.mark_complete()

        return before_call, call, after_call

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._has_run = False

    @property
    def has_run(self) -> bool:
        return self._has_run

    def mark_complete(self) -> None:
        self._has_run = True

    def complete(self) -> bool:
        return self.has_run


class TransferLocalFile(Task):

    source_path = luigi.Parameter(
        default=NO_STR,
        description="path to the file to transfer; when empty, the task input is used; default: "
        "empty",
    )
    replicas = luigi.IntParameter(
        default=0,
        description="number of replicas to generate; when > 0 the output will be a file collection "
        "instead of a single file; default: 0",
    )

    exclude_index = True

    def get_source_target(self) -> LocalFileTarget:
        # when self.source_path is set, return a target around it
        # otherwise assume self.requires() returns a task with a single local target
        if self.source_path not in (NO_STR, None):
            source_path = os.path.expandvars(os.path.expanduser(str(self.source_path)))
            return LocalFileTarget(os.path.abspath(source_path))
        return self.input()

    @abc.abstractmethod
    def single_output(self) -> FileSystemFileTarget:
        ...

    def get_replicated_path(self, basename: str, i: int | None = None) -> str:
        if i is None:
            return basename

        name, ext = os.path.splitext(basename)
        return f"{name}.{i}{ext}"

    def output(self) -> SiblingFileCollection:
        output = self.single_output()
        if self.replicas <= 0:
            return output

        # return the replicas in a SiblingFileCollection
        return SiblingFileCollection([
            output.sibling(self.get_replicated_path(output.basename, i), "f")
            for i in range(self.replicas)
        ])

    def run(self) -> None:
        self.transfer(self.get_source_target())

    def transfer(self, src_path: str | pathlib.Path | LocalFileTarget) -> None:
        output = self.output()

        # single output
        if not isinstance(output, SiblingFileCollection):
            output.copy_from_local(src_path, cache=False)
            return

        # upload all replicas
        progress_callback = self.create_progress_callback(self.replicas)
        for i, replica in enumerate(output.targets):
            replica.copy_from_local(src_path, cache=False)
            progress_callback(i)
            self.publish_message(f"uploaded {replica.basename}")


class ForestMerge(LocalWorkflow):

    tree_index = luigi.IntParameter(
        default=-1,
        description="the index of the merged tree in the forest; -1 denotes the forest itself "
        "which requires and outputs all trees; default: -1",
    )
    tree_depth = luigi.IntParameter(
        default=0,
        description="the depth of this workflow in the merge tree; 0 denotes the root; default: 0",
    )
    keep_nodes = luigi.BoolParameter(
        significant=False,
        description="keep merged results, i.e., task outputs from intermediate nodes in the merge "
        "tree; default: False",
    )

    # fix some workflow parameters
    acceptance = 1.0
    tolerance = 0.0
    pilot = False

    node_format = "{name}.t{tree}.d{depth}.b{branch}{ext}"
    postfix_format = "t{tree}_d{depth}"
    merge_factor = 2

    exclude_index = True
    exclude_params_forest_merge = {"tree_index", "tree_depth", "keep_nodes", "branch", "branches"}

    @classmethod
    def modify_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().modify_param_values(params)

        # when tree_index is negative, which refers to the merge forest, make sure this is branch 0
        if "tree_index" in params and "branch" in params and params["tree_index"] < 0:
            params["branch"] = 0

        return params

    @classmethod
    def _req_tree(cls, inst: ForestMerge, *args, **kwargs) -> ForestMerge:
        # amend workflow branch parameters to exclude
        kwargs["_exclude"] = set(kwargs.pop("_exclude", set())) | {"branches"}

        # just as for all workflows that require branches of themselves (or vice versa,
        # skip task level excludes
        kwargs["_skip_task_excludes"] = True

        # create the required instance
        new_inst = super(ForestMerge, cls).req(inst, *args, **kwargs)

        # forward the _n_leaves attribute
        new_inst._n_leaves = inst._n_leaves

        # when set, also forward the tree itself and caching decisions
        if inst._merge_forest_built:
            new_inst._cache_forest = inst._cache_forest
            new_inst._merge_forest = inst._merge_forest
            new_inst._merge_forest_built = new_inst._merge_forest is not None
            new_inst._leaves_per_tree = inst._leaves_per_tree

        return new_inst

    @classmethod
    def _mark_merge_output_placeholder(cls, target: FileSystemTarget) -> FileSystemTarget:
        """
        Marks a *target*, such as the output of :py:meth:`merge_output` as temporary placeholder.
        When such a target is received while building the merge forest, no actual merging structure
        is constructed, but rather deferred to a future call.
        """
        target._is_merge_output_placeholder = True
        return target

    @classmethod
    def _check_merge_output_placeholder(cls, target: FileSystemTarget) -> bool:
        return bool(getattr(target, "_is_merge_output_placeholder", False))

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # set attributes
        self._n_leaves: int | None = None
        self._cache_forest = True
        self._merge_forest_built = False
        self._merge_forest: list[dict[int, list[tuple[int, ...]]]] | None = None
        self._leaves_per_tree: list[int] | None = None

        # the merge factor should not be 1
        if self.merge_factor == 1:
            raise ValueError("merge factor must not be 1")

        # modify_param_values prevents the forest from being a workflow, but still check
        if self.is_forest() and self.is_workflow():
            raise Exception(f"merge forest must not be a workflow, {self} misconfigured")

    def is_forest(self) -> bool:
        return self.tree_index < 0

    def is_root(self) -> bool:
        return not self.is_forest() and self.tree_depth == 0

    def is_leaf(self) -> bool:
        return not self.is_forest() and self.tree_depth == self.max_tree_depth

    def req_workflow(self, **kwargs) -> ForestMerge:
        # since the forest counts as a branch, as_workflow should point the tree_index 0
        # which is only used to compute the overall merge tree
        if self.is_forest():
            kwargs["tree_index"] = 0
            kwargs["_skip_task_excludes"] = False

        return super().req_workflow(**kwargs)

    @property
    def max_tree_depth(self) -> int:
        return max(self._get_tree().keys())

    @property
    def merge_forest(self) -> list[dict[int, list[tuple[int, ...]]]]:
        self._build_merge_forest()
        return self._merge_forest  # type: ignore[return-value]

    @property
    def leaves_per_tree(self) -> list[int]:
        self._build_merge_forest()
        return self._leaves_per_tree  # type: ignore[return-value]

    @property
    def leaf_range(self) -> tuple[int, int]:
        if not self.is_leaf():
            raise Exception("leaf_range can only be accessed by leaves")

        # compute the range
        leaves_per_tree = self.leaves_per_tree
        merge_factor = self.merge_factor
        n_leaves = leaves_per_tree[self.tree_index]
        offset = sum(leaves_per_tree[:self.tree_index])
        if merge_factor <= 0:
            merge_factor = n_leaves
        start_leaf = offset + self.branch * merge_factor
        end_leaf = min(start_leaf + merge_factor, offset + n_leaves)

        return start_leaf, end_leaf

    def _get_tree(self) -> dict[int, list[tuple[int, ...]]]:
        if self.is_forest():
            raise Exception(
                "merge tree cannot be determined for the merge forest, ForestMerge misconfigured",
            )

        try:
            return self.merge_forest[self.tree_index]
        except IndexError:
            raise Exception(
                f"merge tree {self.tree_index} not found, forest only contains "
                f"{len(self.merge_forest)} tree(s)",
            )

    def _build_merge_forest(self) -> None:
        # a node in the tree can be described by a tuple of integers, where each value denotes the
        # branch path to go down the tree to reach the node (e.g. (2, 0) -> 2nd branch, 0th branch),
        # so the length of the tuple defines the depth of the node via ``depth = len(node) - 1``
        # the tree itself is a dict that maps depths to lists of nodes with that depth
        # when multiple trees are used (a forest), each one handles ``n_leaves / n_trees`` leaves

        # when the forest was already built and saved by means of the _cache_forest flag, do nothing
        if self._merge_forest_built and self._merge_forest is not None:
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

        # infer the number of trees from the merge output
        output = self.merge_output()
        is_placeholder = self._check_merge_output_placeholder(output)
        n_trees = 1
        if not is_placeholder:
            n_trees = len(output) if isinstance(output, (list, tuple, TargetCollection)) else 1

        # first, determine the number of files to merge in total when not already set via params
        reset_n_leaves = False
        if self._n_leaves is None:
            # defer computation when the output is a placeholder
            if is_placeholder:
                self._n_leaves = 1
                reset_n_leaves = True
            else:
                # the following lines build the workflow requirements,
                # which strictly requires this task to be a workflow
                wf = self.as_workflow()

                # get inputs, i.e. outputs of workflow requirements and trace actual inputs to merge
                # an integer number representing the number of inputs is also valid
                inputs = luigi.task.getpaths(wf.merge_workflow_requires())
                inputs = wf.trace_merge_workflow_inputs(inputs)
                self._n_leaves = inputs if isinstance(inputs, int) else len(inputs)

        # complain when there are too few leaves for the configured number of trees to create
        if self._n_leaves < n_trees:
            raise Exception(
                f"insufficient number of leaves ({self._n_leaves}) for number of requested trees "
                f"({n_trees})",
            )

        # determine the number of leaves per tree
        n_min = self._n_leaves // n_trees
        n_trees_overlap = self._n_leaves % n_trees
        leaves_per_tree = n_trees_overlap * [n_min + 1] + (n_trees - n_trees_overlap) * [n_min]
        merge_factor = self.merge_factor

        # when the output is a placeholder, define a one-element tree
        # otherwise, built the forest the normal way
        forest: list[dict[int, list[tuple[int, ...]]]] = []
        if is_placeholder:
            forest.append({0: [(0,)]})
        else:
            for i, n_leaves in enumerate(leaves_per_tree):
                # build a nested list of leaf numbers using the merge factor
                # e.g. 9 leaves with factor 3 -> [[0, 1, 2], [3, 4, 5], [6, 7, 8]]
                nested_leaves: list[list[int]] = list(iter_chunks(n_leaves, merge_factor))
                while len(nested_leaves) > 1:
                    nested_leaves = list(iter_chunks(nested_leaves, merge_factor))

                # convert the list of nodes to the tree format described above
                tree: dict[int, list[tuple[int, ...]]] = {}
                for node in nodify(nested_leaves, root_id=i):
                    depth = len(node) - 1
                    tree.setdefault(depth, []).append(node)

                forest.append(tree)

        # store values, declare the forest as cached for now so that the check below works
        self._leaves_per_tree = leaves_per_tree
        self._merge_forest = forest
        self._merge_forest_built = True

        # complain when the depth is too large
        if not self.is_forest() and self.tree_depth > self.max_tree_depth:
            raise ValueError(
                f"tree_depth {self.tree_depth} exceeds maximum depth {self.max_tree_depth} in task "
                f"{self}",
            )

        # set the final cache decisions
        self._merge_forest_built = self._cache_forest and not is_placeholder
        if reset_n_leaves:
            self._n_leaves = None

    def create_branch_map(self) -> dict[int, tuple[int, ...]]:
        tree = self._get_tree()
        nodes = tree[self.tree_depth]
        return dict(enumerate(nodes))

    def trace_merge_workflow_inputs(self, inputs: Any) -> Sequence[Any] | TargetCollection:
        # should convert inputs to an object with a length (e.g. list, tuple, TargetCollection, ...)

        # for convenience, check if inputs results from the default workflow output, i.e. a dict
        # which stores a TargetCollection in the "collection" field
        if isinstance(inputs, dict) and "collection" in inputs:
            collection = inputs["collection"]
            if isinstance(collection, TargetCollection):
                return collection

        return inputs

    def trace_merge_inputs(self, inputs: Any) -> Sequence[Any]:
        # should convert inputs into an iterable sequence (list, tuple, ...), no TargetCollection!
        return inputs

    @abc.abstractmethod
    def merge_workflow_requires(self) -> Any:
        # should return the requirements of the merge workflow
        ...

    @abc.abstractmethod
    def merge_requires(self, start_leaf: int, end_leaf: int) -> Any:
        # should return the requirements of a merge task, depending on the leaf range
        ...

    @abc.abstractmethod
    def merge_output(self) -> FileSystemFileTarget:
        # this should return a single target when the output should be a single tree
        # or a target collection, list or tuple with item access through tree indices
        ...

    @abc.abstractmethod
    def merge(self, inputs: list[FileSystemFileTarget], output: FileSystemTarget) -> None:
        ...

    def workflow_requires(self) -> Any:
        self._build_merge_forest()

        reqs = super(ForestMerge, self).workflow_requires()

        if self.is_forest():
            raise Exception(
                "workflow requirements cannot be determined for the merge forest, ForestMerge "
                "misconfigured",
            )

        elif self.is_leaf():
            # this is simply the merge workflow requirement
            reqs["forest_merge"] = self.merge_workflow_requires()

        else:
            # intermediate node, just require the next tree depth
            reqs["forest_merge"] = self._req_tree(self, tree_depth=self.tree_depth + 1)

        return reqs

    def _forest_requires(self) -> dict[int, ForestMerge]:
        if not self.is_forest():
            raise Exception(
                "_forest_requires can only be determined for the forest, ForestMerge misconfigured",
            )

        n_trees = len(self.merge_forest)
        indices: range | list[int] = range(n_trees)

        # interpret branches as tree indices when given
        if self.branches:
            indices = [
                i
                for i in range_expand(list(self.branches), min_value=0, max_value=n_trees)
                if 0 <= i < n_trees
            ]

        return {
            i: self._req_tree(
                self,
                branch=-1,
                tree_index=i,
                _exclude=self.exclude_params_workflow,
            )
            for i in indices
        }

    def requires(self) -> DotDict:
        reqs = DotDict()

        if self.is_forest():
            reqs["forest_merge"] = self._forest_requires()

        elif self.is_leaf():
            # this is simply the merge requirement
            reqs["forest_merge"] = self.merge_requires(*self.leaf_range)

        else:
            # get all child nodes in the next layer at depth = depth + 1 and store their branches
            # note: child node tuples contain the exact same values plus an additional one
            tree = self._get_tree()
            node = self.branch_data
            branches = [i for i, n in enumerate(tree[self.tree_depth + 1]) if n[:-1] == node]

            # add to requirements
            reqs["forest_merge"] = {
                b: self._req_tree(self, branch=b, tree_depth=self.tree_depth + 1)
                for b in branches
            }

        return reqs

    def output(self) -> Any:
        output = self.merge_output()

        if self.is_forest():
            return output

        if isinstance(output, (list, tuple, TargetCollection)):
            output = output[self.tree_index]

        if self.is_root():
            return output

        # get the directory in which intermediate outputs are stored
        if isinstance(output, SiblingFileCollection):
            intermediate_dir = output.dir
        else:
            first_output = flatten(output)[0]
            if not isinstance(first_output, FileSystemTarget):
                raise Exception(
                    f"cannot determine directory for intermediate merged outputs from '{output}'",
                )
            intermediate_dir = first_output.parent

        # helper to create an intermediate output
        def get_intermediate_output(leaf_output):
            name, ext = os.path.splitext(leaf_output.basename)
            basename = self.node_format.format(
                name=name,
                ext=ext,
                tree=self.tree_index,
                branch=self.branch,
                depth=self.tree_depth,
            )
            return intermediate_dir.child(basename, type="f")

        # return intermediate outputs in the same structure
        if isinstance(output, TargetCollection):
            return output.map(get_intermediate_output)
        return map_struct(get_intermediate_output, output)

    def run(self) -> None | Iterator[Any]:
        # nothing to do for the forest
        if self.is_forest():
            # yield the forest dependencies again
            yield self._forest_requires()
            return

        # trace actual inputs to merge
        inputs = self.input()["forest_merge"]
        inputs = list(self.trace_merge_inputs(inputs) if self.is_leaf() else inputs.values())

        # merge
        self.publish_message(f"start merging {len(inputs)} inputs of node {self.branch_data}")
        self.merge(inputs, self.output())

        # remove intermediate nodes
        if not self.is_leaf() and not self.keep_nodes:
            msg = f"removing intermediate results of node {self.branch_data}"
            with self.publish_step(msg):
                for inp in flatten(inputs):
                    inp.remove()

    def control_output_postfix(self):
        postfix = super(ForestMerge, self).control_output_postfix()
        return ("{pf}_" + self.postfix_format).format(
            pf=postfix,
            tree=self.tree_index,
            depth=self.tree_depth,
        )
