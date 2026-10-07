"""
Tasks that provide common and often used functionality.
"""

from __future__ import annotations

__all__ = ["ForestMerge", "RunOnceTask", "TransferLocalFile"]

import abc
import os
import pathlib

import luigi

from law._types import Any, Callable, Iterator, Sequence
from law.decorator import factory
from law.logger import get_logger
from law.parameter import NO_STR
from law.target.collection import SiblingFileCollection, TargetCollection
from law.target.file import FileSystemFileTarget, FileSystemTarget
from law.target.local import LocalFileTarget
from law.task.base import Task
from law.util import DotDict, flatten, iter_chunks, map_struct, range_expand
from law.workflow.local import LocalWorkflow

logger = get_logger(__name__)


class RunOnceTask(Task):
    """
    Base class of tasks without outputs that are complete once they ran. Completeness is tracked per instance, so they
    run again in new processes. The run method should be decorated with :py:meth:`complete_on_success`, or call
    :py:meth:`mark_complete` itself. Example:

    .. code-block:: python

        class MyTask(law.tasks.RunOnceTask):

            @law.tasks.RunOnceTask.complete_on_success
            def run(self):
                ...
    """

    @staticmethod
    @factory(accept_generator=True)
    def complete_on_success(
        fn: Callable,
        opts: dict[str, Any],
        task: Task,
        *args,
        **kwargs,
    ) -> tuple[Callable, Callable, Callable]:
        """
        Decorator for the run method that marks the task as complete after it ran successfully.
        """
        def before_call() -> None:
            return None

        def call(state: None):
            return fn(task, *args, **kwargs)

        def after_call(state: None) -> None:
            task.mark_complete()  # type: ignore[attr-defined]

        return before_call, call, after_call

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._has_run = False

    @property
    def has_run(self) -> bool:
        """
        Whether the task was marked as complete.
        """
        return self._has_run

    def mark_complete(self) -> None:
        """
        Marks the task as complete.
        """
        self._has_run = True

    def complete(self) -> bool:
        return self.has_run


class TransferLocalFile(Task):
    """
    Base class of tasks that transfer a local file to the output of the task, e.g. on a remote file system. The file is
    given by the *source_path* parameter or, when empty, the input of the task. Inheriting classes must implement
    :py:meth:`single_output`. When *replicas* is positive, multiple copies are created and the output is a
    :py:class:`~law.target.collection.SiblingFileCollection`.
    """

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
    exclude_params_repr_empty = {"source_path"}

    def get_source_target(self) -> LocalFileTarget:
        """
        Returns the target of the file to transfer.

        :return: The target referring to *source_path* when set, and the input of the task otherwise.
        """
        # when self.source_path is set, return a target around it
        # otherwise assume self.requires() returns a task with a single local target
        if self.source_path not in (NO_STR, None):
            source_path = os.path.expandvars(os.path.expanduser(str(self.source_path)))
            return LocalFileTarget(os.path.abspath(source_path))
        return self.input()

    @abc.abstractmethod
    def single_output(self) -> FileSystemFileTarget:
        """
        Hook that returns the output target of a single transferred file. Must be implemented by inheriting classes.

        :return: The output target.
        """
        ...

    def get_replicated_path(self, basename: str, i: int | None = None) -> str:
        """
        Returns the name of the replica *i* of a file with *basename* by inserting the replica number before the
        extension.

        :param basename: The base name of the file.
        :param i: The replica number. When *None*, *basename* is returned unchanged.
        :return: The replicated name.
        """
        if i is None:
            return basename

        name, ext = os.path.splitext(basename)
        return f"{name}.{i}{ext}"

    def output(self) -> SiblingFileCollection | FileSystemFileTarget:
        replicas: int = self.replicas
        output = self.single_output()
        if replicas <= 0:
            return output

        # return the replicas in a SiblingFileCollection
        return SiblingFileCollection([
            output.sibling(self.get_replicated_path(output.basename, i), "f")
            for i in range(replicas)
        ])

    def run(self) -> None:
        self.transfer(self.get_source_target())

    def trace_transfer_output(self, output: Any) -> SiblingFileCollection | FileSystemFileTarget:
        """
        Hook to convert the *output* of the task into the target or collection to transfer to.

        :param output: The output of the task.
        :return: The target or collection, which is *output* by default.
        """
        return output

    def transfer(
        self,
        src_path: str | pathlib.Path | LocalFileTarget,
        output: SiblingFileCollection | FileSystemFileTarget | None = None,
    ) -> None:
        """
        Copies a local file to the output. When the output is a collection of replicas, the file is copied to each of
        them and the progress is published.

        :param src_path: The path of the local file.
        :param output: The target or collection to copy to, defaulting to the output of the task traced through
            :py:meth:`trace_transfer_output`.
        """
        # get the output target to transfer
        if output is None:
            output = self.output()
            output = self.trace_transfer_output(output)

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
    """
    Workflow that merges a potentially large number of inputs in a tree-like structure, where each node merges up to
    :py:attr:`merge_factor` inputs. Inputs are the leaves of the tree, and the root node produces the final output.
    Multiple trees (a forest) are built when :py:meth:`merge_output` returns multiple targets, in which case the inputs
    are distributed among the trees.

    The *tree_index* parameter selects the tree, while the default value *-1* refers to the forest, which requires all
    trees. *tree_depth* denotes the depth of the workflow in the tree, with *0* being the root. Intermediate outputs are
    removed after they were merged unless *keep_nodes* is set.

    Inheriting classes must implement :py:meth:`merge_workflow_requires`, :py:meth:`merge_requires`,
    :py:meth:`merge_output` and :py:meth:`merge`.

    .. py:classattribute:: merge_factor

        type: int

        The maximum number of inputs that are merged per node. A non-positive value merges all
        inputs at once. Defaults to *2*.

    .. py:classattribute:: node_format

        type: str

        The format of the names of intermediate outputs, receiving *name*, *ext*, *tree*, *depth*
        and *branch*. Defaults to ``"{name}.t{tree}.d{depth}.b{branch}{ext}"``.

    .. py:classattribute:: postfix_format

        type: str

        The format of the postfix that is added to control outputs, receiving *tree* and *depth*.
        Defaults to ``"t{tree}_d{depth}"``.
    """

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
        new_inst = super().req(inst, *args, **kwargs)

        # forward the _n_leaves attribute
        new_inst._n_leaves = inst._n_leaves  # type: ignore[attr-defined]

        # when set, also forward the tree itself and caching decisions
        if inst._merge_forest_built:
            new_inst._cache_forest = inst._cache_forest  # type: ignore[attr-defined]
            new_inst._merge_forest = inst._merge_forest  # type: ignore[attr-defined]
            new_inst._merge_forest_built = new_inst._merge_forest is not None  # type: ignore[attr-defined]
            new_inst._leaves_per_tree = inst._leaves_per_tree  # type: ignore[attr-defined]

        return new_inst  # type: ignore[return-value]

    @classmethod
    def _mark_merge_output_placeholder(cls, target: FileSystemTarget) -> FileSystemTarget:
        """
        Marks a *target*, such as the output of :py:meth:`merge_output` as temporary placeholder. When such a target is
        received while building the merge forest, no actual merging structure is constructed, but rather deferred to a
        future call.

        :param target: The target to mark.
        :return: The marked target.
        """
        target._is_merge_output_placeholder = True  # type: ignore[attr-defined]
        return target

    @classmethod
    def _check_merge_output_placeholder(cls, target: Sequence | FileSystemTarget | TargetCollection) -> bool:
        return bool(getattr(target, "_is_merge_output_placeholder", False))

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # set attributes
        self._n_leaves: int | None = None
        self._cache_forest = True
        self._merge_forest_built = False
        self._merge_forest: list[dict[int, list[tuple[int, ...]]]] | None = None
        self._leaves_per_tree: list[int] | None = None

        # modify_param_values prevents the forest from being a workflow, but still check
        if self.is_forest() and self.is_workflow():
            raise RuntimeError(f"merge forest must not be a workflow, {self} misconfigured")

    def is_forest(self) -> bool:
        """
        Returns whether this task refers to the forest, i.e., whether *tree_index* is negative.

        :return: Whether this task is the forest.
        """
        return self.tree_index < 0

    def is_root(self) -> bool:
        """
        Returns whether this task refers to the root of a tree.

        :return: Whether this task is a root.
        """
        return not self.is_forest() and self.tree_depth == 0

    def is_leaf(self) -> bool:
        """
        Returns whether this task refers to the leaves of a tree, i.e., the nodes that merge the actual inputs.

        :return: Whether this task is a leaf.
        """
        return not self.is_forest() and self.tree_depth == self.max_tree_depth

    def req_workflow(self, **kwargs) -> ForestMerge:
        # since the forest counts as a branch, as_workflow should point the tree_index 0
        # which is only used to compute the overall merge tree
        if self.is_forest():
            kwargs["tree_index"] = 0
            kwargs["_skip_task_excludes"] = False

        return super().req_workflow(**kwargs)  # type: ignore[return-value]

    @property
    def max_tree_depth(self) -> int:
        """
        The maximum depth of the tree.
        """
        return max(self._get_tree().keys())

    @property
    def merge_forest(self) -> list[dict[int, list[tuple[int, ...]]]]:
        """
        The structure of the forest as a list of trees, each mapping depths to lists of nodes, which are tuples of
        branch indices that describe the path from the root.
        """
        self._build_merge_forest()
        return self._merge_forest  # type: ignore[return-value]

    @property
    def leaves_per_tree(self) -> list[int]:
        """
        The number of inputs that are merged per tree.
        """
        self._build_merge_forest()
        return self._leaves_per_tree  # type: ignore[return-value]

    @property
    def leaf_range(self) -> tuple[int, int]:
        """
        The range of input numbers that are merged by this leaf as a 2-tuple, with the end being exclusive. Only
        accessible for leaves.
        """
        if not self.is_leaf():
            raise RuntimeError("leaf_range can only be accessed by leaves")

        # compute the range
        tree_index: int = self.tree_index
        leaves_per_tree = self.leaves_per_tree
        merge_factor = self.merge_factor
        n_leaves = leaves_per_tree[tree_index]
        offset = sum(leaves_per_tree[:tree_index])
        if merge_factor <= 0:
            merge_factor = n_leaves
        start_leaf = offset + self.branch * merge_factor
        end_leaf = min(start_leaf + merge_factor, offset + n_leaves)

        return start_leaf, end_leaf

    def _get_tree(self) -> dict[int, list[tuple[int, ...]]]:
        if self.is_forest():
            raise RuntimeError(
                "merge tree cannot be determined for the merge forest, ForestMerge misconfigured",
            )

        try:
            return self.merge_forest[self.tree_index]
        except IndexError as e:
            raise IndexError(
                f"merge tree {self.tree_index} not found, forest only contains {len(self.merge_forest)} tree(s)",
            ) from e

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
                node = ()
            else:
                nodes.append(node)
            for i, _obj in enumerate(obj):
                nodes += nodify(_obj, (*node, (i if node else root_id)))
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
            raise ValueError(
                f"insufficient number of leaves ({self._n_leaves}) for number of requested trees ({n_trees})",
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
            raise ValueError(f"tree_depth {self.tree_depth} exceeds maximum depth {self.max_tree_depth} in task {self}")

        # set the final cache decisions
        self._merge_forest_built = self._cache_forest and not is_placeholder
        if reset_n_leaves:
            self._n_leaves = None

    def create_branch_map(self) -> dict[int, tuple[int, ...]]:
        tree = self._get_tree()
        nodes = tree[self.tree_depth]
        return dict(enumerate(nodes))

    def trace_merge_workflow_inputs(self, inputs: Any) -> Sequence[Any] | TargetCollection:
        """
        Hook to convert the outputs of :py:meth:`merge_workflow_requires` into an object with a length that represents
        all inputs to merge. By default, the ``"collection"`` of workflow outputs is extracted when present.

        :param inputs: The outputs of :py:meth:`merge_workflow_requires`.
        :return: An object with a length, or an integer number of inputs.
        """
        # should convert inputs to an object with a length (e.g. list, tuple, TargetCollection, ...)

        # for convenience, check if inputs results from the default workflow output, i.e. a dict
        # which stores a TargetCollection in the "collection" field
        if isinstance(inputs, dict) and "collection" in inputs:
            collection = inputs["collection"]
            if isinstance(collection, TargetCollection):
                return collection

        return inputs

    def trace_merge_inputs(self, inputs: Any) -> Sequence[Any]:
        """
        Hook to convert the outputs of :py:meth:`merge_requires` into a sequence of inputs to merge.

        :param inputs: The outputs of :py:meth:`merge_requires`.
        :return: The sequence of inputs, which is *inputs* by default.
        """
        # should convert inputs into an iterable sequence (list, tuple, ...), no TargetCollection!
        return inputs

    @abc.abstractmethod
    def merge_workflow_requires(self) -> Any:
        """
        Hook that returns the requirements that provide all inputs to merge, usually a workflow. Must be implemented by
        inheriting classes.

        :return: The requirements.
        """
        # should return the requirements of the merge workflow
        ...

    @abc.abstractmethod
    def merge_requires(self, start_leaf: int, end_leaf: int) -> Any:
        """
        Hook that returns the requirements that provide a range of inputs, e.g. the corresponding branches of the
        workflow returned by :py:meth:`merge_workflow_requires`. Must be implemented by inheriting classes.

        :param start_leaf: The number of the first input.
        :param end_leaf: The number of the last input (exclusive).
        :return: The requirements.
        """
        # should return the requirements of a merge task, depending on the leaf range
        ...

    @abc.abstractmethod
    def merge_output(self) -> Sequence | FileSystemFileTarget | TargetCollection:
        """
        Hook that returns the final merged output. Must be implemented by inheriting classes.

        :return: The output. When it is a list, tuple or target collection, one tree is built per element.
        """
        # this should return a single target when the output should be a single tree
        # or a target collection, list or tuple with item access through tree indices
        ...

    @abc.abstractmethod
    def merge(self, inputs: list[FileSystemFileTarget], output: FileSystemTarget) -> None:
        """
        Hook that merges *inputs* into *output*. Must be implemented by inheriting classes.

        :param inputs: The input targets.
        :param output: The output target.
        """
        ...

    def workflow_requires(self) -> Any:
        self._build_merge_forest()

        reqs = super().workflow_requires()

        if self.is_forest():
            raise RuntimeError(
                "workflow requirements cannot be determined for the merge forest, ForestMerge "
                "misconfigured",
            )

        if self.is_leaf():
            # this is simply the merge workflow requirement
            reqs["forest_merge"] = self.merge_workflow_requires()

        else:
            # intermediate node, just require the next tree depth
            reqs["forest_merge"] = self._req_tree(self, tree_depth=self.tree_depth + 1)

        return reqs

    def _forest_requires(self) -> dict[int, ForestMerge]:
        if not self.is_forest():
            raise RuntimeError(
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
            tree_depth: int = self.tree_depth
            tree = self._get_tree()
            node = self.branch_data
            branches = [i for i, n in enumerate(tree[tree_depth + 1]) if n[:-1] == node]

            # add to requirements
            reqs["forest_merge"] = {
                b: self._req_tree(self, branch=b, tree_depth=tree_depth + 1)
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
                raise ValueError(
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

    def run(self) -> Iterator[Any] | None:
        # nothing to do for the forest
        if self.is_forest():
            # yield the forest dependencies again
            yield self._forest_requires()
            return None

        # trace actual inputs to merge
        inputs = self.input()["forest_merge"]
        inputs = list(self.trace_merge_inputs(inputs) if self.is_leaf() else inputs.values())

        # merge
        node_position = (self.tree_index, *self.branch_data)
        self.publish_message(f"start merging {len(inputs)} inputs of node {node_position}")
        self.merge(inputs, self.output())

        # remove intermediate nodes
        if not self.is_leaf() and not self.keep_nodes:
            msg = f"removing intermediate results of node {node_position}"
            with self.publish_step(msg):
                for inp in flatten(inputs):
                    inp.remove()

        return None

    def control_output_postfix(self):
        """
        Returns the postfix of control outputs, extended by the tree index and depth following
        :py:attr:`postfix_format`.

        :return: The postfix.
        """
        postfix = super().control_output_postfix()  # type: ignore[misc]
        return ("{pf}_" + self.postfix_format).format(
            pf=postfix,
            tree=self.tree_index,
            depth=self.tree_depth,
        )
