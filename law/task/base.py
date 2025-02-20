# coding: utf-8

"""
Custom luigi base task definitions.
"""

from __future__ import annotations

__all__ = ["Task", "WrapperTask", "ExternalTask"]

import sys
import time
import pathlib
import logging
import contextlib
from abc import ABCMeta, abstractmethod
import inspect

import luigi  # type: ignore[import-untyped]

from law.config import Config
from law.parameter import NO_STR, CSVParameter
from law.target.file import localize_file_targets
from law.target.local import LocalFileTarget
from law.parser import root_task, global_cmdline_values
from law.logger import setup_logger
from law.util import (
    no_value, abort, law_run, common_task_params, colored, uncolored, make_list, multi_match,
    flatten, BaseStream, human_duration, patch_object, round_discrete, empty_context, make_set,
    map_struct, mask_struct,
)
from law.logger import get_logger
from law._types import Any, Sequence, Iterator, Generator, Callable, Iterable, T, TextIO


logger = get_logger(__name__)


class BaseRegister(luigi.task_register.Register):

    def __new__(
        metacls,
        cls_name: str,
        bases: tuple[type],
        cls_dict: dict[str, Any],
    ) -> BaseRegister:
        # default attributes, irrespective of inheritance
        cls_dict.setdefault("exclude_index", False)

        # unite "exclude_params_*" sets with those of all base classes
        for base in bases:
            for attr, base_params in vars(base).items():
                if attr.startswith("exclude_params_") and isinstance(base_params, set):
                    params = cls_dict.setdefault(attr, set())
                    if isinstance(params, set):
                        params.update(base_params)

        # remove those parameter names from "exclude_params_*" sets which are explicitly
        # listed in corresponding "include_params_*" sets defined on the class itself
        for attr, include_params in cls_dict.items():
            if attr.startswith("include_params_") and isinstance(include_params, set):
                exclude_attr = "exclude" + attr[len("include"):]
                if exclude_attr in cls_dict and isinstance(cls_dict[exclude_attr], set):
                    cls_dict[exclude_attr] -= include_params

        # create the class, bypassing the luigi task register
        cls: BaseRegister = ABCMeta.__new__(metacls, cls_name, bases, cls_dict)

        # default attributes, apart from inheritance
        if getattr(cls, "update_register", None) is None:
            cls.update_register = False  # type: ignore[attr-defined]

        # deregister when requested
        if cls.update_register:  # type: ignore[attr-defined]
            cls.deregister()  # type: ignore[attr-defined]

        # invoke the class-level attribute update hook
        cls.modify_task_attributes()  # type: ignore[attr-defined]

        # add to register (mimic luigi.task_register.Register.__new__)
        cls._namespace_at_class_time = metacls._get_namespace(cls.__module__)  # type: ignore[attr-defined] # noqa
        metacls._reg.append(cls)

        return cls


class BaseTask(luigi.Task, metaclass=BaseRegister):

    exclude_index = True
    exclude_params_index: set[str] = set()
    exclude_params_req: set[str] = set()
    exclude_params_req_set: set[str] = set()
    exclude_params_req_get: set[str] = set()
    prefer_params_cli: set[str] = set()

    # whether to cache the result of requires() for input() and potentially also other calls
    cache_requirements = False

    @classmethod
    def deregister(cls, task_cls: BaseRegister | None = None) -> bool:
        """
        Removes a task class *task_cls* from the luigi task register. When *None*, *this* class is
        used. Task family strings and patterns are accepted as well. *True* is returned when at
        least one class was successfully removed, and *False* otherwise.
        """
        # always compare task families
        if task_cls is None:
            task_family = cls.get_task_family()
        elif isinstance(task_cls, str):
            task_family = task_cls
        else:
            task_family = task_cls.get_task_family()  # type: ignore[attr-defined]

        success = False

        # remove from the register
        i = -1
        while True:
            i += 1
            if i >= len(Register._reg):
                break
            registered_cls = Register._reg[i]

            if multi_match(registered_cls.get_task_family(), task_family, mode=any):
                Register._reg.pop(i)
                i -= 1
                success = True
                logger.debug(f"removed task class {registered_cls} from register")

        return success

    @classmethod
    def modify_task_attributes(cls) -> None:
        """
        Hook to modify class attributes before the class is added to the register.
        """
        return

    @classmethod
    def modify_param_args(
        cls,
        params: list[tuple[str, luigi.Parameter]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> tuple[list[tuple[str, luigi.Parameter]], tuple[Any, ...], dict[str, Any]]:
        """
        Hook to modify command line arguments before they are event created within
        :py:meth:`get_param_values`.
        """
        return params, args, kwargs

    @classmethod
    def modify_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Hook to modify command line arguments before instances of this class are created.
        """
        return params

    @classmethod
    def get_param_values(
        cls,
        params: list[tuple[str, luigi.Parameter]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> list[Any]:
        # call the hook optionally modifying the values before values are assigned
        params, args, kwargs = cls.modify_param_args(params, args, kwargs)

        # assign to actual parameters
        values = super().get_param_values(params, args, kwargs)

        # parse left-over strings in case values were given programmatically, but unencoded
        values = [
            (name, (getattr(cls, name).parse(value) if isinstance(value, str) else value))
            for name, value in values
        ]

        # call the hook optionally modifying the values afterwards,
        # but remove temporary objects that might have been placed into it
        param_names = {name for name, _ in params}
        values = [
            (name, value)
            for name, value in cls.modify_param_values(dict(values)).items()
            if name in param_names
        ]

        return values

    @classmethod
    def req(cls, inst: BaseTask, **kwargs) -> BaseTask:
        return cls(**cls.req_params(inst, **kwargs))

    @classmethod
    def req_params(
        cls,
        inst: BaseTask,
        _exclude: str | Sequence[str] | set[str] | None = None,
        _prefer_cli: str | Sequence[str] | set[str] | None = None,
        _skip_task_excludes: bool = False,
        _skip_task_excludes_get: bool | None = None,
        _skip_task_excludes_set: bool | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        # common/intersection params
        params = common_task_params(inst, cls)

        # determine parameters to exclude
        _exclude = set() if _exclude is None else make_set(_exclude)

        # also use this class' req and req_get sets
        # and the req and req_set sets of the instance's class
        # unless explicitly skipped
        if _skip_task_excludes_get is None:
            _skip_task_excludes_get = _skip_task_excludes
        if not _skip_task_excludes_get:
            _exclude.update(cls.exclude_params_req, cls.exclude_params_req_get)

        if _skip_task_excludes_set is None:
            _skip_task_excludes_set = _skip_task_excludes
        if not _skip_task_excludes_set:
            _exclude.update(inst.exclude_params_req, inst.exclude_params_req_set)

        # remove excluded parameters
        for name in list(params.keys()):
            if multi_match(name, _exclude, any):
                del params[name]

        # add kwargs
        params.update(kwargs)

        # remove params that are preferably set via cli class arguments
        prefer_cli = (
            make_set(cls.prefer_params_cli or ())
            if _prefer_cli is None
            else make_set(_prefer_cli)
        )
        if prefer_cli:
            cls_args = []
            prefix = cls.get_task_family() + "_"
            if luigi.cmdline_parser.CmdlineParser.get_instance():
                for key in (global_cmdline_values() or {}).keys():
                    if key.startswith(prefix):
                        cls_args.append(key[len(prefix):])
            for name in prefer_cli:
                if name in params and name in cls_args:
                    del params[name]

        return params

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # task level logger, created lazily
        self._task_logger: logging.Logger | None = None

        # attribute for cached requirements if enabled
        self._cached_requirements = no_value

    def complete(self) -> bool:
        # create a flat list of all outputs
        outputs = flatten(self.output())

        if len(outputs) == 0:
            logger.warning(f"task {self!r} has no outputs or no custom complete() method")
            return True

        return all(t.complete() for t in outputs)

    def input(self) -> Any:
        # get potentially cached requirements
        if self.cache_requirements:
            if self._cached_requirements is no_value:
                self._cached_requirements = self.requires()
            reqs = self._cached_requirements
        else:
            reqs = self.requires()

        return luigi.task.getpaths(reqs)

    @abstractmethod
    def run(self) -> None | Iterator[Any]:
        ...

    def get_logger_name(self) -> str:
        return self.task_id

    def _create_logger(self, name: str, level: str | int | None = None) -> logging.Logger:
        return setup_logger(name, level=level)

    @property
    def logger(self) -> logging.Logger:
        if self._task_logger is None:
            name = self.get_logger_name()
            existing = name in logging.root.manager.loggerDict
            self._task_logger = logging.getLogger(name) if existing else self._create_logger(name)

        return self._task_logger

    @property
    def live_task_id(self) -> str:
        """
        The task id depends on the task family and parameters, and is generated by luigi once in the
        constructor. As the latter may change, this property returns to the id with the current set
        of parameters.
        """
        # create a temporary dictionary of param_kwargs that is patched for the duration of the
        # call to create the string representation of the parameters
        param_kwargs = {attr: getattr(self, attr) for attr in self.param_kwargs}
        # only_public was introduced in luigi 2.8.0, so check if that arg exists
        str_params_kwargs = {"only_significant": True}
        if "only_public" in inspect.getfullargspec(self.to_str_params).args:
            str_params_kwargs["only_public"] = True
        with patch_object(self, "param_kwargs", param_kwargs):
            str_params = self.to_str_params(**str_params_kwargs)

        # create the task id
        task_id = luigi.task.task_id_str(self.get_task_family(), str_params)

        return task_id

    def walk_deps(
        self,
        max_depth: int = -1,
        order: str = "level",
        yield_last_flag: bool = False,
    ) -> Iterator[tuple[BaseTask, list[BaseTask], int] | tuple[BaseTask, list[BaseTask], int, bool]]:
        # see https://en.wikipedia.org/wiki/Tree_traversal
        if order not in ("level", "pre"):
            raise ValueError(f"unknown traversal order '{order}', use 'level' or 'pre'")

        # yielding the last flag as well is only available in 'pre' order
        if order != "pre" and yield_last_flag:
            raise ValueError(f"yield_last_flag can only be used in 'pre' order, but got '{order}'")

        tasks = [(self, 0)]
        while len(tasks):
            task, depth = tasks.pop(0)
            deps = flatten(task.requires())
            next_depth = tasks[0][1] if tasks else None

            # define the tuple of objects to yield
            tpl = (task, deps, depth)
            if not yield_last_flag:
                yield tpl

            # define the next deps, considering the maximum depth if set
            deps_gen = (
                (d, depth + 1)
                for d in deps
                if max_depth < 0 or depth < max_depth
            )

            # add to the tasks run process, depending on the traversal order
            if order == "level":
                tasks[len(tasks):] = deps_gen
            elif order == "pre":
                tasks[:0] = deps_gen

            # when an additional flag should be yielded that denotes whether the object is the last
            # one in its depth, evaluate this decision here and then yield
            # note: this assumes that the deps were not changed by the using context
            if yield_last_flag:
                is_last = next_depth is None or next_depth < depth
                yield tpl + (is_last,)

    def cli_args(
        self,
        exclude: str | Sequence[str] | set[str] | None = None,
        replace: dict[str, Any] | None = None,
        skip_empty_bools: bool = True,
    ) -> dict[str, str]:
        exclude = set() if exclude is None else make_set(exclude)
        if replace is None:
            replace = {}

        args = {}
        for name, param in self.get_params():
            # skip excluded parameters
            if multi_match(name, exclude, any):
                continue
            # get the raw value
            raw = replace.get(name, getattr(self, name))
            # when the parameter is bool but its value is None, skip it
            if skip_empty_bools and isinstance(param, luigi.BoolParameter) and raw is None:
                continue
            # serialize and add it
            args["--" + name.replace("_", "-")] = str(param.serialize(raw))

        return args


class Register(BaseRegister):

    def __call__(cls, *args, **kwargs) -> Task:
        inst = super().__call__(*args, **kwargs)

        # check for interactive parameters
        for param in inst.interactive_params:
            value = getattr(inst, param)
            if value:
                # reset the interactive parameter
                setattr(inst, param, ())

                # at this point, inst must be the root task so set the global value
                root_task(inst)

                skip_abort = False
                try:
                    logger.debug(f"evaluating interactive parameter '{param}' with value {value}")
                    skip_abort = getattr(inst, "_" + param)(value)

                except KeyboardInterrupt:
                    print("\naborted")

                # abort the process if not explicitly skipped
                if not skip_abort:
                    abort(exitcode=0)
                print("")

        return inst


class Task(BaseTask, metaclass=Register):

    log_file = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="a custom log file; default: <task.default_log_file>",
    )
    print_deps = CSVParameter(
        default=(),
        significant=False,
        description="print task dependencies but do not run any task; this CSV parameter accepts a "
        "single integer value which sets the task recursion depth (0 means non-recursive)",
    )
    print_status = CSVParameter(
        default=(),
        significant=False,
        description="print the task status but do not run any task; this CSV parameter accepts up "
        "to three values: 1. the task recursion depth (0 means non-recursive), 2. the depth of the "
        "status text of target collections (default: 0), 3. a flag that is passed to the status "
        "text creation (default: '')",
    )
    print_output = CSVParameter(
        default=(),
        significant=False,
        description="print a flat list of output targets but do not run any task; this CSV "
        "parameter accepts up to two values: 1. the task recursion depth (0 means non-recursive), "
        "2. a boolean flag that decides whether paths of file targets should contain file system "
        "schemes (default: True)",
    )
    remove_output = CSVParameter(
        default=(),
        significant=False,
        description="remove task outputs but do not run any task by default; this CSV parameter "
        "accepts up to three values: 1. the task recursion depth (0 means non-recursive), 2. one "
        "of the modes 'i' (interactive), 'a' (all), 'd' (dry run) (default: 'i'), 3. a boolean "
        "flag that decides whether the task is run after outputs were removed (default: False)",
    )
    fetch_output = CSVParameter(
        default=(),
        significant=False,
        description="copy all task outputs into a local directory but do not run any task; this "
        "CSV parameter accepts up to four values: 1. the task recursion depth (0 means "
        "non-recursive), 2. one of the modes 'i' (interactive), 'a' (all), 'd' (dry run) (default: "
        "'i'), 3. the target directory (default: '.'), 4. a boolean flag that decides whether "
        "external outputs and outputs of external tasks should be fetched (default: False)",
    )

    interactive_params = [
        "print_deps", "print_status", "print_output", "fetch_output", "remove_output",
    ]

    # cache size for published messages
    message_cache_size = 10

    # force skipping this task when remove_output is set to "all" mode
    skip_output_removal = False

    exclude_index = True
    exclude_params_req: set[str] = set()
    exclude_params_repr: set[str] = set()
    exclude_params_repr_empty: set[str] = set()

    @classmethod
    def req_params(  # type: ignore[override]
        cls,
        inst: Task,
        _exclude: str | Sequence[str] | set[str] | None = None,
        _prefer_cli: str | Sequence[str] | set[str] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        _exclude = set() if _exclude is None else make_set(_exclude)

        # always exclude interactive parameters
        _exclude |= set(inst.interactive_params)

        return super().req_params(inst, _exclude=_exclude, _prefer_cli=_prefer_cli, **kwargs)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # cache for messages published to the scheduler
        self._message_cache: list[str] = []

        # cache for the last progress published to the scheduler
        self._last_progress_percentage: int | None = None

    @property
    def default_log_file(self) -> str | pathlib.Path | LocalFileTarget:
        return "-"

    def is_root_task(self) -> bool:
        return root_task() == self

    def publish_message(
        self,
        msg: Any,
        stdout: TextIO = sys.stdout,
        scheduler: bool = True,
        **kwargs,
    ) -> None:
        msg = str(msg)

        # write to stdout
        if stdout:
            stdout.write(f"{msg}\n")
            stdout.flush()

        # publish to the scheduler
        if scheduler:
            self._publish_message(msg, **kwargs)

    def _publish_message(self, msg: Any, flush_cache: bool = False, silent: bool = False) -> None:
        msg = uncolored(str(msg))

        # flush the message cache?
        if flush_cache:
            del self._message_cache[:]

        # add to message cache and handle overflow
        self._message_cache.append(msg)
        if self.message_cache_size >= 0:
            end = max(len(self._message_cache) - self.message_cache_size, 0)
            del self._message_cache[:end]

        # set status message based on the full, current message cache
        if callable(getattr(self, "set_status_message", None)):
            self.set_status_message("\n".join(self._message_cache))
        elif not silent:
            logger.warning("set_status_message not set, cannot send task message to scheduler")

    def _create_message_stream(self, *args, **kwargs) -> TaskMessageStream:
        return TaskMessageStream(self, *args, **kwargs)

    def _create_logger(self, name: str, level: str | int | None = None, **kwargs) -> logging.Logger:
        return setup_logger(
            name,
            level=level,
            add_console_handler={
                "handler_kwargs": {"stream": self._create_message_stream(**kwargs)},
            },
        )

    @contextlib.contextmanager
    def publish_step(
        self,
        msg: Any,
        success_message: str = "done",
        fail_message: str = "failed",
        runtime: bool = True,
        scheduler: bool = True,
        flush_cache: bool = False,
    ) -> Iterator[None]:
        self.publish_message(msg, scheduler=scheduler, flush_cache=flush_cache)
        success = False
        t0 = time.perf_counter()
        try:
            yield
            success = True
        finally:
            msg = success_message if success else fail_message
            if runtime:
                diff = time.perf_counter() - t0
                msg = f"{msg} (took {human_duration(seconds=diff)})"
            self.publish_message(msg, scheduler=scheduler, flush_cache=flush_cache)

    def publish_progress(self, percentage: int | float, precision: int = 1):
        percentage = int(round_discrete(percentage, precision, "floor"))
        if percentage != self._last_progress_percentage:
            self._last_progress_percentage = percentage

            if callable(getattr(self, "set_progress_percentage", None)):
                self.set_progress_percentage(percentage)
            else:
                logger.warning(
                    "set_progress_percentage not set, cannot send task progress to scheduler",
                )

    def create_progress_callback(
        self,
        n_total: int,
        reach: tuple[int, int] = (0, 100),
        precision: int = 1,
    ) -> Callable[[int], None]:
        def make_callback(n, start, end) -> Callable[[int], None]:
            def callback(i: int) -> None:
                self.publish_progress(start + (i + 1) / float(n) * (end - start), precision)
            return callback

        if isinstance(n_total, (list, tuple)):
            width = 100.0 / len(n_total)
            reaches = [(width * i, width * (i + 1)) for i in range(len(n_total))]
            return n_total.__class__(make_callback(n, *r) for n, r in zip(n_total, reaches))

        return make_callback(n_total, *reach)

    def iter_progress(
        self,
        iterable: Iterable[T],
        n_total: int,
        reach: tuple[int, int] = (0, 100),
        precision: int = 1,
        msg: Any | None = None,
    ) -> Iterator[T]:
        # create a progress callback with all arguments
        progress_callback = self.create_progress_callback(n_total, reach=reach, precision=precision)

        # when msg is set, place the iteration in an outer context
        context = (lambda: self.publish_step(msg)) if msg else empty_context

        # iterate and invoke the callback
        with context():  # type: ignore[operator]
            for i, val in enumerate(iterable):
                yield val
                progress_callback(i)

    def cli_args(
        self,
        exclude: str | Sequence[str] | set[str] | None = None,
        replace: dict[str, Any] | None = None,
        skip_empty_bools: bool = True,
    ) -> dict[str, str]:
        exclude = set() if exclude is None else make_set(exclude)

        # always exclude interactive parameters
        exclude |= set(self.interactive_params)

        return super().cli_args(exclude=exclude, replace=replace, skip_empty_bools=skip_empty_bools)

    def __repr__(self) -> str:
        color = Config.instance().get_expanded_bool("task", "colored_repr")
        return self.repr(color=color)

    def __str__(self) -> str:
        color = Config.instance().get_expanded_bool("task", "colored_str")
        return self.repr(color=color)

    def repr(
        self,
        all_params: bool = False,
        color: bool | None = None,
        **kwargs,
    ) -> str:
        if color is None:
            color = Config.instance().get_expanded_bool("task", "colored_repr")

        family = self._repr_family(self.get_task_family(), color=color, **kwargs)

        parts = [
            self._repr_param(name, value, color=color, **kwargs)
            for name, value in self._repr_params(all_params=all_params).items()
        ] + [
            self._repr_flag(flag, color=color, **kwargs)
            for flag in self._repr_flags()
        ]

        return f"{family}({', '.join(parts)})"

    def _repr_params(self, all_params: bool = False) -> dict[str, Any]:
        # determine parameters to exclude
        exclude = set()
        if not all_params:
            exclude |= self.exclude_params_repr
            exclude |= set(self.interactive_params)

        # build a map "name -> value" for all significant parameters
        params = {}
        for name, param in self.get_params():
            value = getattr(self, name)
            include = (
                param.significant and
                not multi_match(name, exclude) and
                (value not in (None, "NO_STR", ()) or name not in self.exclude_params_repr_empty)
            )
            if include:
                params[name] = value

        return params

    def _repr_flags(self) -> list[str]:
        return []

    def _repr_family(self, family: str, color: bool = False, **kwargs) -> str:
        return colored(family, "green") if color else family

    def _repr_param(
        self,
        name: str,
        value: Any,
        color: bool = False,
        serialize: bool = True,
        **kwargs,
    ) -> str:
        # try to serialize first unless explicitly disabled
        if serialize:
            param = getattr(self.__class__, name, no_value)
            if param != no_value:
                value = param.serialize(value) if isinstance(param, luigi.Parameter) else param

        name_repr = colored(name, color="blue", style="bright") if color else name
        return f"{name_repr}={value}"

    def _repr_flag(self, name: str, color: bool = False, **kwargs) -> str:
        return colored(name, color="magenta") if color else name

    def _print_deps(self, args: tuple) -> None:
        return print_task_deps(self, *args)

    def _print_status(self, args: tuple) -> None:
        return print_task_status(self, *args)

    def _print_output(self, args: tuple) -> None:
        return print_task_output(self, *args)

    def _remove_output(self, args: tuple) -> bool:
        return remove_task_output(self, *args)

    def _fetch_output(self, args: tuple) -> None:
        return fetch_task_output(self, *args)

    @classmethod
    def _law_run_inst(
        cls,
        inst: Task,
        _exclude: str | Sequence[str] | set[str] | None = None,
        _replace: dict[str, Any] | None = None,
        _global_args: str | Sequence[str] | set[str] | None = None,
        _run_kwargs: dict[str, Any] | None = None,
    ) -> int:
        # get the cli arguments
        args = inst.cli_args(exclude=_exclude, replace=_replace)

        # prepend a space to values starting with "-"
        for key, value in args.items():
            if value.startswith("-"):
                args[key] = f" {value}"

        # flatten them
        flat_args = sum(map(make_list, args.items()), [])

        # add global parameters when given
        if _global_args:
            flat_args.extend([str(arg) for arg in make_list(_global_args)])

        # build the full command
        cmd = [cls.get_task_family()] + flat_args

        # run it
        return law_run(cmd, **(_run_kwargs or {}))

    @classmethod
    def law_run_inst(
        cls,
        _exclude: str | Sequence[str] | set[str] | None = None,
        _replace: dict[str, Any] | None = None,
        _global_args: str | Sequence[str] | set[str] | None = None,
        _run_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> int:
        # create a new instance
        inst = cls(**kwargs)

        return cls._law_run_inst(
            inst,
            _exclude=_exclude,
            _replace=_replace,
            _global_args=_global_args,
            _run_kwargs=_run_kwargs,
        )

    def law_run(
        self,
        _exclude: str | Sequence[str] | set[str] | None = None,
        _replace: dict[str, Any] | None = None,
        _global_args: str | Sequence[str] | set[str] | None = None,
        _run_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> int:
        # when kwargs are given, create a new instance
        inst: Task = self.req(self, **kwargs) if kwargs else self  # type: ignore[assignment]

        return self._law_run_inst(
            inst,
            _exclude=_exclude,
            _replace=_replace,
            _global_args=_global_args,
            _run_kwargs=_run_kwargs,
        )

    def localize_input(self, *args, **kwargs) -> Generator[Any, None, None]:
        return localize_file_targets(self.input(), *args, **kwargs)  # type: ignore[return-value]

    def localize_output(self, *args, **kwargs) -> Generator[Any, None, None]:
        return localize_file_targets(self.output(), *args, **kwargs)  # type: ignore[return-value]


class WrapperTask(Task):
    """
    Use for tasks that only wrap other tasks and that by definition are done
    if all their requirements exist.
    """

    exclude_index = True

    def _repr_flags(self) -> list[str]:
        return super()._repr_flags() + ["wrapper"]

    def complete(self) -> bool:
        # get potentially cached requirements
        if self.cache_requirements:
            if self._cached_requirements is no_value:
                self._cached_requirements = self.requires()
            reqs = self._cached_requirements
        else:
            reqs = self.requires()

        return all(task.complete() for task in flatten(reqs))

    def output(self) -> Any:
        inputs = self.input()
        return mask_struct(map_struct(bool, inputs), inputs) or []

    def run(self) -> None | Iterator[Any]:
        return None


class ExternalTask(Task):

    exclude_index = True

    run = None  # type: ignore[assignment]

    def _repr_flags(self) -> list[str]:
        return super()._repr_flags() + ["external"]


class TaskMessageStream(BaseStream):

    def __init__(
        self,
        task: Task,
        stdout: TextIO = sys.stdout,
        scheduler: bool = True,
        flush_cache: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.task = task
        self.stdout = stdout
        self.scheduler = scheduler
        self.flush_cache = flush_cache

    def _write(self, msg: Any) -> None:
        # foward to publish_message
        self.task.publish_message(
            str(msg).rstrip("\n"),
            stdout=self.stdout,
            scheduler=self.scheduler,
            flush_cache=self.flush_cache,
            silent=True,
        )


# trailing imports
from law.task.interactive import (
    print_task_deps, print_task_status, print_task_output, remove_task_output, fetch_task_output,
)
