"""
Example showing workflow parameters.

The payload is the same as in the "workflows" example, but CreateChars creates both lower and upper case characters.
Its branches are not selected via ``--branch`` or ``--branches``, but through the *num* and *upper_case* workflow
parameters which are looked up in the branch map.
"""

from __future__ import annotations

import os

import luigi

import law


class Task(law.Task):
    """
    Base task that provides some convenience methods to create local file targets at the default data path, as defined
    in the setup.sh.
    """

    def store_parts(self):
        return (self.__class__.__name__,)

    def local_path(self, *path):
        # WORKFLOWEXAMPLE_DATA_PATH is defined in setup.sh
        parts = ("$WORKFLOWEXAMPLE_DATA_PATH", *self.store_parts(), *path)
        return os.path.join(*map(str, parts))

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class CreateChars(Task, law.LocalWorkflow):
    """
    Workflow with 52 branches, each converting an ascii number into a lower or upper case character.

    *num* and *upper_case* are workflow parameters. When set on a workflow, only branches whose data matches all set
    values are processed. When set on a branch task, they are filled with the values of its branch data. Examples:

        - ``--upper-case`` selects the 26 branches creating upper case characters,
        - ``--num 97`` selects the two branches creating "a" and "A", and
        - ``--num 97 --upper-case`` selects the single branch creating "A".
    """

    num = law.WorkflowParameter(
        cls=luigi.IntParameter,
        description="ascii number of the character to create; default: all",
    )
    upper_case = law.WorkflowParameter(
        cls=luigi.BoolParameter,
        description="whether to create upper case characters; default: both",
    )

    @classmethod
    def create_branch_map(cls, params):
        # the branch map must be created before the task is instantiated in order to look up branches from workflow
        # parameters, so this method is a classmethod that receives all task parameters in a dictionary
        return [
            {"num": num, "upper_case": upper_case}
            for upper_case in [False, True]
            for num in range(97, 122 + 1)
        ]

    def output(self):
        return self.local_target("output_{}_{}.json".format(
            self.num,
            "uc" if self.upper_case else "lc",
        ))

    def run(self):
        # workflow parameters are set to the values of the branch data
        char = chr(self.num)
        if self.upper_case:
            char = char.upper()
        self.output().dump({"num": self.num, "char": char})


class CreateAlphabet(Task):
    """
    Requires the CreateChars workflow and writes the alphabet in either lower or upper case into a text file.
    """

    upper_case = luigi.BoolParameter(default=False, description="create the upper case alphabet; default: False")

    def requires(self):
        # the upper_case value is forwarded to the CreateChars workflow, which then selects the matching 26 branches
        return CreateChars.req(self)

    def output(self):
        return self.local_target("alphabet_{}.txt".format("uc" if self.upper_case else "lc"))

    def run(self):
        alphabet = "".join(
            inp.load()["char"]
            for inp in self.input()["collection"].targets.values()
        )
        self.output().dump(alphabet + "\n")
        alphabet = "".join(law.util.colored(c, color="random") for c in alphabet)
        self.publish_message(f"\nbuilt alphabet: {alphabet}\n")
