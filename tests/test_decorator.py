# mypy: disable-error-code="call-arg"
from __future__ import annotations

__all__ = ["TestDecorator"]

import io
import os
import pathlib
import sys

import luigi
import pytest

import law
from law.decorator import factory

# collects calls of the tracking decorator below
_track_calls: list[tuple[str, int]] = []


@factory(accept_generator=True)
def _track(fn, opts, task, *args, **kwargs):
    def before_call():
        _track_calls.append(("before", task.n))
        return task.n

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        _track_calls.append(("after", state))

    return before_call, call, after_call


class LawTestDecoratorDep(luigi.Task):

    k = luigi.IntParameter()

    def complete(self) -> bool:
        return True


class LawTestDecoratorGenTask(law.Task):

    out_dir = luigi.Parameter()
    n = luigi.IntParameter()

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, f"gen_{self.n}"))

    @_track
    def run(self):
        yield LawTestDecoratorDep(k=self.n)
        self.output().touch()


class DecoratorTestCase:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)


class TestDecorator(DecoratorTestCase):

    def test_factory_plain(self) -> None:
        @factory(digits=2)
        def double(fn, opts, task, *args, **kwargs):
            return 2 * fn(task, *args, **kwargs) + opts["digits"]

        class Obj:
            @double
            def method(self, x):
                return x

            @double(digits=10)
            def method2(self, x):
                return x

        assert Obj().method(3) == 8
        assert Obj().method2(3) == 16
        # the decorator stack can be skipped
        assert Obj().method(3, skip_decorators=True) == 3

    def test_factory_rejects_generators(self) -> None:
        @factory()
        def plain(fn, opts, task, *args, **kwargs):
            return fn(task, *args, **kwargs)

        with pytest.raises(TypeError, match=r"not configured to decorate a generator function"):
            class Obj:
                @plain
                def run(self):
                    yield 1

    def test_factory_generator_callbacks(self) -> None:
        events: list[str] = []

        @factory(accept_generator=True)
        def deco(fn, opts, task, *args, **kwargs):
            def before_call():
                events.append("before")
                return 42

            def call(state):
                events.append(f"call {state}")
                return fn(task, *args, **kwargs)

            def after_call(state):
                events.append(f"after {state}")

            return before_call, call, after_call

        class Obj:
            @deco
            def method(self):
                return "result"

        # plain functions are called serially
        assert Obj().method() == "result"
        assert events == ["before", "call 42", "after 42"]

    def test_factory_on_error(self) -> None:
        @factory(swallow=True, accept_generator=True)
        def deco(fn, opts, task, *args, **kwargs):
            def before_call():
                return None

            def call(state):
                return fn(task, *args, **kwargs)

            def after_call(state):
                return None

            def on_error(error, state):
                return opts["swallow"]

            return before_call, call, after_call, on_error

        class Obj:
            @deco
            def fail(self):
                raise RuntimeError("fail")

            @deco(swallow=False)
            def fail2(self):
                raise RuntimeError("fail")

        assert Obj().fail() is None
        with pytest.raises(RuntimeError, match="fail"):
            Obj().fail2()

    def test_factory_invalid_callbacks(self) -> None:
        @factory(accept_generator=True)
        def deco(fn, opts, task, *args, **kwargs):
            return (None, None)

        class Obj:
            @deco
            def method(self):
                pass

        with pytest.raises(TypeError, match=r"must return 3 or 4 callbacks"):
            Obj().method()

    def test_factory_generator_state_per_run(self) -> None:
        _track_calls.clear()
        tasks = [LawTestDecoratorGenTask(out_dir=self.tmp, n=n) for n in (1, 2)]
        assert luigi.build(tasks, local_scheduler=True, log_level="ERROR")
        assert _track_calls == [("before", 1), ("after", 1), ("before", 2), ("after", 2)]

    def test_safe_output(self) -> None:
        out_dir = self.tmp

        class LawTestSafeOutputTask(law.Task):
            optional = luigi.BoolParameter(default=True)

            def output(self):
                return {
                    "a": law.LocalFileTarget(os.path.join(out_dir, "a.txt")),
                    "b": law.LocalFileTarget(os.path.join(out_dir, "b.txt"), optional=True),
                }

            @law.decorator.safe_output
            def run(self):
                for t in self.output().values():
                    t.touch()
                raise RuntimeError("fail")

            @law.decorator.safe_output(skip=KeyError)
            def run_skip(self):
                for t in self.output().values():
                    t.touch()
                raise KeyError("fail")

            @law.decorator.safe_output(optional=False)
            def run_keep_optional(self):
                for t in self.output().values():
                    t.touch()
                raise RuntimeError("fail")

        task = LawTestSafeOutputTask()

        # outputs are removed on errors
        with pytest.raises(RuntimeError, match="fail"):
            task.run()
        assert not any(t.exists() for t in task.output().values())

        # skipped exceptions do not remove outputs
        with pytest.raises(KeyError, match="fail"):
            task.run_skip()
        assert all(t.exists() for t in task.output().values())

        # optional outputs are kept when requested
        with pytest.raises(RuntimeError, match="fail"):
            task.run_keep_optional()
        assert not task.output()["a"].exists()
        assert task.output()["b"].exists()

    def test_delay(self) -> None:
        class Obj:
            @law.decorator.delay(t=0.01)
            def method(self):
                return 1

            @law.decorator.delay(t=0.01, stddev=0.001, pdf="unknown")
            def method_invalid(self):
                return 1

        assert Obj().method() == 1
        with pytest.raises(ValueError, match=r"unknown delay decorator pdf"):
            Obj().method_invalid()

    def test_timeit(self) -> None:
        messages: list[str] = []

        class Logger:
            def info(self, msg):
                messages.append(msg)

        class Obj:
            logger = Logger()

            @law.decorator.timeit
            def method(self):
                return 1

            @law.decorator.timeit
            def fail(self):
                raise RuntimeError("fail")

        assert Obj().method() == 1
        assert len(messages) == 1
        assert messages[0].startswith("runtime: ")

        # the runtime is also logged on errors
        with pytest.raises(RuntimeError, match="fail"):
            Obj().fail()
        assert len(messages) == 2

    def test_log(self) -> None:
        log_path = os.path.join(self.tmp, "logs", "log.txt")

        class LawTestLogTask(law.Task):
            def run(self):
                print("hello log")

        task = LawTestLogTask(log_file=log_path)
        stdout, stderr = sys.stdout, sys.stderr
        try:
            law.decorator.log(LawTestLogTask.run)(task)
        finally:
            sys.stdout, sys.stderr = stdout, stderr
        with open(log_path, encoding="utf-8") as f:
            assert f.read() == "hello log\n"

        # no redirection with "-"
        task = LawTestLogTask(log_file="-")
        assert law.decorator.log(LawTestLogTask.run)(task) is None

    def test_log_target(self) -> None:
        log_target = law.LocalFileTarget(os.path.join(self.tmp, "log.txt"))

        class LawTestLogTargetTask(law.Task):
            @property
            def default_log_file(self):
                return log_target

            def run(self):
                print("hello log")

        stdout, stderr = sys.stdout, sys.stderr
        try:
            law.decorator.log(LawTestLogTargetTask.run)(LawTestLogTargetTask())
        finally:
            sys.stdout, sys.stderr = stdout, stderr
        assert log_target.load(formatter="text") == "hello log\n"

    def test_log_restores_streams(self) -> None:
        log_path = os.path.join(self.tmp, "log.txt")

        class LawTestLogStreamsTask(law.Task):
            def run(self):
                pass

        stdout, stderr = sys.stdout, sys.stderr
        custom_out, custom_err = io.StringIO(), io.StringIO()
        sys.stdout, sys.stderr = custom_out, custom_err
        try:
            law.decorator.log(LawTestLogStreamsTask.run)(LawTestLogStreamsTask(log_file=log_path))
            restored = (sys.stdout, sys.stderr)
        finally:
            sys.stdout, sys.stderr = stdout, stderr
        assert restored == (custom_out, custom_err)

    def test_notify(self) -> None:
        notifications: list[tuple] = []

        def notify_func(title, content, **kwargs):
            notifications.append((title, content))

        class LawTestNotifyTask(law.Task):
            notify_custom = law.NotifyCustomParameter(notify_func=notify_func, significant=False)
            fail = luigi.BoolParameter(default=False)

            @law.decorator.notify
            def run(self):
                if self.fail:
                    raise RuntimeError("fail")

        # no notification when the parameter is not set
        LawTestNotifyTask().run()
        assert notifications == []

        # notification on success
        LawTestNotifyTask(notify_custom=True).run()
        assert len(notifications) == 1
        title, content = notifications[0]
        assert title == "Task LawTestNotifyTask succeeded!"
        assert set(content) == {"Task", "Host", "Duration", "Last message"}

        # notification on failure, including the traceback
        with pytest.raises(RuntimeError, match="fail"):
            LawTestNotifyTask(notify_custom=True, fail=True).run()
        assert len(notifications) == 2
        title, content = notifications[1]
        assert title == "Task LawTestNotifyTask failed!"
        assert "RuntimeError: fail" in content["Traceback"]

    def test_notify_on_success_failure(self) -> None:
        notifications: list[str] = []

        def notify_func(title, content, **kwargs):
            notifications.append(title)

        class LawTestNotifyFilterTask(law.Task):
            notify_custom = law.NotifyCustomParameter(notify_func=notify_func, significant=False)
            fail = luigi.BoolParameter(default=False)

            @law.decorator.notify(on_success=False)
            def run(self):
                if self.fail:
                    raise RuntimeError("fail")

        LawTestNotifyFilterTask(notify_custom=True).run()
        assert notifications == []
        with pytest.raises(RuntimeError, match="fail"):
            LawTestNotifyFilterTask(notify_custom=True, fail=True).run()
        assert notifications == ["Task LawTestNotifyFilterTask failed!"]

    def test_notify_custom_func_signature(self) -> None:
        notifications: list[str] = []

        def notify_func(title, content):
            notifications.append(title)

        class LawTestNotifySignatureTask(law.Task):
            notify_custom = law.NotifyCustomParameter(notify_func=notify_func, significant=False)

            @law.decorator.notify
            def run(self):
                pass

        LawTestNotifySignatureTask(notify_custom=True).run()
        assert notifications == ["Task LawTestNotifySignatureTask succeeded!"]

    def test_localize(self) -> None:
        out_dir = self.tmp

        class LawTestLocalizeInput(law.ExternalTask):
            def output(self):
                return law.LocalFileTarget(os.path.join(out_dir, "input.txt"))

        class LawTestLocalizeTask(law.Task):
            def requires(self):
                return LawTestLocalizeInput()

            def output(self):
                return law.LocalFileTarget(os.path.join(out_dir, "output.txt"))

            @law.decorator.localize
            def run(self):
                # local inputs are read in place, outputs are written to temporary targets first
                assert self.input().path == self.input_unlocalized().path  # type: ignore[attr-defined]
                assert self.output().path != self.output_unlocalized().path  # type: ignore[attr-defined]
                assert not self.output_unlocalized().exists()  # type: ignore[attr-defined]
                self.output().dump(self.input().load() + "!")

        LawTestLocalizeInput().output().dump("data")
        task = LawTestLocalizeTask()
        task.run()
        assert task.output().load() == "data!"
        # the original methods are restored
        assert task.output().path == os.path.join(out_dir, "output.txt")
        assert not hasattr(task, "output_unlocalized")

    def test_require_sandbox(self) -> None:
        class Obj:
            @law.decorator.require_sandbox
            def method(self):
                pass

        with pytest.raises(TypeError, match=r"can only be used to decorate methods of tasks"):
            Obj().method()
