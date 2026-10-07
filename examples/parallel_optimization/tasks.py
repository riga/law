from __future__ import annotations

import os

import luigi
import luigi.util

import law

law.contrib.load("matplotlib")


class Task(law.Task):
    """
    Base task that provides some convenience methods to create local file and
    directory targets at the default data path.
    """

    def local_path(self, *path):
        # ANALYSIS_DATA_PATH is defined in setup.sh
        parts = (os.getenv("ANALYSIS_DATA_PATH"), self.__class__.__name__, *path)
        return os.path.join(*parts)

    def local_target(self, *path, **kwargs):
        return law.LocalFileTarget(self.local_path(*path), **kwargs)


class Optimizer(Task, law.LocalWorkflow):
    """
    Workflow that runs optimization.
    """

    iterations = luigi.IntParameter(default=10, description="number of iterations; default: 10")
    n_parallel = luigi.IntParameter(default=4, description="number of parallel evaluations; default: 4")
    n_initial_points = luigi.IntParameter(
        default=10,
        description="number of randomly sampled points before starting the optimization; default: 10",
    )

    def create_branch_map(self):
        return list(range(self.iterations))

    def requires(self):
        if self.branch == 0:
            return None
        return Optimizer.req(self, branch=self.branch - 1)

    def output(self):
        return self.local_target(f"optimizer_{self.branch}.pkl")

    def run(self):
        import skopt
        optimizer = self.input().load() if self.branch != 0 else skopt.Optimizer(
            dimensions=[skopt.space.Real(-5.0, 10.0), skopt.space.Real(0.0, 15.0)],
            random_state=1,
            n_initial_points=self.n_initial_points,
        )

        x = optimizer.ask(n_points=self.n_parallel)

        output = yield Objective.req(self, x=x, iteration=self.branch, branch=-1)

        y = [f.load()["y"] for f in output["collection"].targets.values()]

        optimizer.tell(x, y)

        print(f"minimum after {self.branch + 1} iterations: {min(optimizer.yi)}")

        with self.output().localize("w") as tmp:
            tmp.dump(optimizer)


@luigi.util.inherits(Optimizer)
class OptimizerPlot(Task, law.LocalWorkflow):
    """
    Workflow that runs optimization and plots results.
    """

    plot_objective = luigi.BoolParameter(
        default=True,
        description="plot the objective, which can be expensive for high dimensional inputs; default: True",
    )

    def create_branch_map(self):
        return list(range(self.iterations))

    def requires(self):
        return Optimizer.req(self)

    def has_fitted_model(self):
        return self.plot_objective and (self.branch + 1) * self.n_parallel >= self.n_initial_points

    def output(self):
        collection = {
            "evaluations": self.local_target(f"evaluations_{self.branch}.pdf"),
            "convergence": self.local_target(f"convergence_{self.branch}.pdf"),
        }

        if self.has_fitted_model():
            collection["objective"] = self.local_target(f"objective_{self.branch}.pdf")

        return law.SiblingFileCollection(collection)

    def run(self):
        import matplotlib.pyplot as plt
        from skopt.plots import plot_convergence, plot_evaluations, plot_objective

        result = self.input().load().run(None, 0)
        output = self.output()

        with output.targets["convergence"].localize("w") as tmp:
            plot_convergence(result)
            tmp.dump(plt.gcf(), bbox_inches="tight")
        plt.close()
        with output.targets["evaluations"].localize("w") as tmp:
            plot_evaluations(result, bins=10)
            tmp.dump(plt.gcf(), bbox_inches="tight")
        plt.close()
        if self.has_fitted_model():
            with output.targets["objective"].localize("w") as tmp:
                plot_objective(result)
                tmp.dump(plt.gcf(), bbox_inches="tight")
            plt.close()


class Objective(Task, law.LocalWorkflow):
    """
    Objective to optimize.

    This workflow will evaluate the branin function for given values `x`.
    In a real world example this will likely be an expensive to compute function like a
    neural network training or other computational demanding task.
    The workflow can be easily extended as a remote workflow to submit evaluation jobs
    to a batch system in order to run calculations in parallel.
    """
    x = luigi.ListParameter()
    iteration = luigi.IntParameter()

    def create_branch_map(self):
        return dict(enumerate(self.x))

    def output(self):
        return self.local_target(f"x_{self.iteration}_{self.branch}.json")

    def run(self):
        from skopt.benchmarks import branin
        with self.output().localize("w") as tmp:
            tmp.dump({"x": self.branch_data, "y": branin(self.branch_data)})
