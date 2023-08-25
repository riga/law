# coding: utf-8


import os

import luigi
import law


class Task(law.Task):

    def store_parts(self):
        return (self.__class__.__name__,)

    def local_path(self, *path):
        parts = ("$WORKFLOWEXAMPLE_DATA_PATH",) + self.store_parts() + path
        return os.path.join(*map(str, parts))

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class CreateChars(Task, law.LocalWorkflow):

    num = law.WorkflowParameter(
        cls=luigi.IntParameter,
    )
    upper_case = law.WorkflowParameter(
        cls=luigi.BoolParameter,
    )

    @classmethod
    def create_branch_map(cls, params):
        return [
            {"num": num, "upper_case": upper_case}
            for upper_case in [True, False]
            for num in range(97, 122 + 1)
        ]

    def output(self):
        return self.local_target("output_{}_{}.json".format(
            self.num,
            "uc" if self.upper_case else "lc",
        ))

    def run(self):
        char = chr(self.num)
        if self.upper_case:
            char = char.upper()
        self.output().dump({"num": self.num, "char": char})


class CreateAlphabet(Task):

    upper_case = luigi.BoolParameter(default=False)

    def requires(self):
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
        self.publish_message("\nbuilt alphabet: {}\n".format(alphabet))
