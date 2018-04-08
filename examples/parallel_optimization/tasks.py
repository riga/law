# -*- coding: utf-8 -*-

import os

import luigi
import law
law.contrib.load("matplotlib")


class Task(law.Task):
    """
    Base that provides some convenience methods to create local file and
    directory targets at the default data path.
    """

    def local_path(self, *path):
        # ANALYSIS_DATA_PATH is defined in setup.sh
        parts = (os.getenv("ANALYSIS_DATA_PATH"), self.__class__.__name__) + path
        return os.path.join(*parts)

    def local_target(self, *path, **kwargs):
        return law.LocalFileTarget(self.local_path(*path), **kwargs)


class Optimizer(Task, law.LocalWorkflow):
    """
    Workflow that runs optimization.
    """

    iterations = luigi.IntParameter(default=10, description="Number of iterations")
    n_parallel = luigi.IntParameter(default=4, description="Number of parallel evaluations")
    n_initial_points = luigi.IntParameter(default=10, description="Number of random sampled values \
        before starting optimizations")

    def create_branch_map(self):
        return list(range(self.iterations))

    def requires(self):
        if self.branch == 0:
            return None
        return Optimizer.req(self, branch=self.branch - 1)

    def output(self):
        return self.local_target("optimizer_{}.pkl".format(self.branch))

    def run(self):
        import skopt
        optimizer = self.input().load() if self.branch != 0 else skopt.Optimizer(
            dimensions=[skopt.space.Real(-5.0, 10.0), skopt.space.Real(0.0, 15.0)],
            random_state=1, n_initial_points=self.n_initial_points
        )

        x = optimizer.ask(n_points=self.n_parallel)

        output = yield Objective.req(self, x=x, iteration=self.branch, branch=-1)

        y = [f.load()["y"] for f in output["collection"].targets.values()]

        optimizer.tell(x, y)

        print("minimum after {} iterations: {}".format(self.branch + 1, min(optimizer.yi)))

        with self.output().localize("w") as tmp:
            tmp.dump(optimizer)


@luigi.util.inherits(Optimizer)
class OptimizerPlot(Task, law.LocalWorkflow):
    """
    Workflow that runs optimization and plots results.
    """

    plot_objective = luigi.BoolParameter(default=True, description="Plot objective. \
        Can be expensive to evaluate for high dimensional input")

    def create_branch_map(self):
        return list(range(self.iterations))

    def requires(self):
        return Optimizer.req(self)

    def has_fitted_model(self):
        return self.plot_objective and (self.branch + 1) * self.n_parallel >= self.n_initial_points

    def output(self):
        collection = {
            "evaluations": self.local_target("evaluations_{}.pdf".format(self.branch)),
            "convergence": self.local_target("convergence_{}.pdf".format(self.branch))
        }

        if self.has_fitted_model():
            collection["objective"] = self.local_target("objective_{}.pdf".format(self.branch))

        return law.SiblingFileCollection(collection)

    def run(self):
        from skopt.plots import plot_objective, plot_evaluations, plot_convergence
        import matplotlib.pyplot as plt

        result = self.input().load().run(None, 0)
        output = self.output()
        output.dir.touch()

        with output.targets["convergence"].localize("w") as tmp:
            plot_convergence(result)
            tmp.dump(plt.gcf(), bbox_inches="tight")
        plt.close()
        with output.targets["evaluations"].localize("w") as tmp:
            plot_evaluations(result, bins=10)
            tmp.dump(plt.gcf(), bbox_inches="tight")
        plt.close()
        if self.has_fitted_model():
            plot_objective(result)
            with output.targets["objective"].localize("w") as tmp:
                tmp.dump(plt.gcf(), bbox_inches="tight")
            plt.close()


class Objective(Task, law.LocalWorkflow):
    """
    Objective to optimize.

    This workflow will evaluate the branin function for given values `x`.
    In a real world example this will likely be a expensive to compute function like a
    neural network training or other computational demanding task.
    The workflow can be easily extended as a remote workflow to submit evaluation jobs
    to a batch system in order to run calculations in parallel.
    """
    x = luigi.ListParameter()
    iteration = luigi.IntParameter()

    def create_branch_map(self):
        return {i: x for i, x in enumerate(self.x)}

    def output(self):
        return self.local_target("x_{}_{}.json".format(self.iteration, self.branch))

    def run(self):
        from skopt.benchmarks import branin
        with self.output().localize("w") as tmp:
            tmp.dump({"x": self.branch_data, "y": branin(self.branch_data)})
