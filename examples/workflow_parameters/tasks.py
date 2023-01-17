# coding: utf-8

import itertools as it
import os

import luigi
import law
from law.parameter import CSVParameter
from law.util import no_value


class WorkflowParameter(CSVParameter):
    def __init__(self, *args, **kwargs):
        # force an empty default value, disable single values being wrapped by tuples, and declare
        # the parameter as insignificant as they only act as a convenient branch lookup interface
        if "default" in kwargs or "force_tuple" in kwargs or "significant" in kwargs:
            raise ValueError("Cannot pass default/force_tuple/significant to WorkflowParameter")

        kwargs["default"] = no_value
        kwargs["force_tuple"] = True
        kwargs["significant"] = False

        super(WorkflowParameter, self).__init__(*args, **kwargs)

        # linearize the default
        self._default = no_value

    def parse(self, inp):
        """"""
        if not inp:
            return no_value

        return super(WorkflowParameter, self).parse(inp)

    def serialize(self, value):
        """"""
        if value == no_value:
            return ""

        return super(WorkflowParameter, self).serialize(value)

    def allows(self, value, set_value):
        """
        Checks if the user requested "value" to be run by passing "set_value" for this parameter.
        This could be used, for instance, to implement a parameter as a range ("run all mass points
        between 1 and 2TeV)".
        """
        return value in set_value

    def is_set(self, set_value):
        """Checks if the parameter has been set by the user if its value is set_value."""
        return set_value is not no_value


class AutomaticWorkflow(law.Task):
    def __init__(self, *args, **kwargs):
        # Fill in parameter values etc.
        super().__init__(*args, **kwargs)

        # Handle translation from workflow parameters to branch(es)
        workflow_params = [
            (name, param) for name, param in self.__class__.get_params()
            if isinstance(param, WorkflowParameter)
        ]
        present_params = list(filter(lambda t: t[1].is_set(self.param_kwargs[t[0]]),
                                     workflow_params))

        # Forbidden combo
        if present_params and self.branches:
            raise ValueError("Cannot use branches and workflow parameters in the same task")


        if self.branches:
            # Normal branch semantics
            pass

        elif self.branch == -1 and not self.branches:
            # Using workflow parameter semantics
            # Remove workflow parameters from the task, so create_branch_map()
            # cannot use them. The branch map depend on automatic parameters,
            # otherwise two workflows with different parameters wouldn't
            # execute properly in parallel.
            param_kwargs = self.param_kwargs.copy()  # Save it, we'll need it shortly
            for name, _ in present_params:
                setattr(self, name, no_value)
                self.param_kwargs[name] = no_value

            # Find branches to be executed
            # FIXME This calls create_branch_map() before the subclass is fully initialized
            all_branches = self.create_branch_map()
            self.branches = []
            for i, params in all_branches.items():
                requested = True
                for name, p in present_params:
                    if not p.allows(params[name], param_kwargs[name]):
                        requested = False
                        break
                if requested:
                    self.branches.append(i)

            if not self.branches:
                # Don't run everything...
                # FIXME Support empty workflows in the core?
                raise ValueError("No branch left after taking workflow params into account")

        elif self.branch_data:
            # Running a single branch
            # Regular branch parameters take precedence over workflow parameters:
            # set workflow parameters to values from the branch
            # FIXME Requiring branch_data to be a map kinda breaks the API
            #       (I'm mostly using tuples or numbers as branch data)
            # FIXME Do this lower in the stack, when setting branch_data?
            self.param_kwargs.update(self.branch_data)
            for name, value in self.branch_data.items():
                setattr(self, name, value)


class Task(AutomaticWorkflow):

    def store_parts(self):
        return (self.__class__.__name__,)

    def local_path(self, *path):
        parts = (os.getenv("WORKFLOWEXAMPLE_DATA_PATH"),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class CreateChars(Task, law.LocalWorkflow):

    num = WorkflowParameter(
        cls=luigi.IntParameter,
    )
    upper_case = WorkflowParameter(
        cls=luigi.BoolParameter,
    )

    #@classmethod
    def create_branch_map(self):
        return {
            i: {"num": num, "upper_case": upper_case}
            for i, (num, upper_case) in enumerate(
                it.product(range(97, 122 + 1), [True, False]))
        }

    def output(self):
        return self.local_target("output_{}_{}.json".format(self.num, self.upper_case))

    def run(self):
        num = self.num + (26 if self.upper_case else 0)
        char = chr(num)
        self.output().dump({"num": num, "char": char})


class CreateAlphabet(Task):

    def requires(self):
        return CreateChars.req(self)

    def output(self):
        return self.local_target("alphabet.txt")

    def run(self):
        alphabet = "".join(
            inp.load()["char"]
            for inp in self.input()["collection"].targets.values()
        )
        self.output().dump(alphabet + "\n")
        alphabet = "".join(law.util.colored(c, color="random") for c in alphabet)
        self.publish_message("\nbuilt alphabet: {}\n".format(alphabet))
