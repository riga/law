"""
Example tasks whose command line outputs are shown on the cli page of the documentation, see
generate_cli_outputs.py.
"""

import law


class CreateNumbers(law.LocalWorkflow):

    def create_branch_map(self):
        return [1, 2, 3]

    def output(self):
        return law.LocalFileTarget(f"$DATA_PATH/numbers_{self.branch}.json")

    def run(self):
        self.output().dump({"number": self.branch_data}, formatter="json")


class SumNumbers(law.Task):

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return law.LocalFileTarget("$DATA_PATH/sum.json")

    def run(self):
        inputs = self.input()["collection"].targets.values()
        total = sum(inp.load(formatter="json")["number"] for inp in inputs)
        self.output().dump({"sum": total}, formatter="json")


class PlotSum(law.Task):

    def requires(self):
        return SumNumbers.req(self)

    def output(self):
        return law.LocalFileTarget("$DATA_PATH/plot.txt")

    def run(self):
        total = self.input().load(formatter="json")["sum"]
        self.output().dump(f"sum: {total}", formatter="text")
