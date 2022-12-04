# coding: utf-8


import os

import luigi
import law


class Task(law.Task):

    def store_parts(self):
        return (self.__class__.__name__,)

    def local_path(self, *path):
        parts = (os.getenv("WORKFLOWEXAMPLE_DATA_PATH"),) + self.store_parts() + path
        return os.path.join(*parts)

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
            for num in range(97, 122 + 1)
            for upper_case in [True, False]
        ]

    def output(self):
        return self.local_target("output_{}_{}.json".format(self.num, self.upper_case))

    def run(self):
        num = self.branch_data
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
