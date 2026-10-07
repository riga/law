# Merging outputs

Workflows often produce many small outputs that need to be combined into one, e.g. histograms of many input files.

## Merging specific file types

Some contrib packages provide functions that implement the actual merging for common file types:

- {py:func}`law.root.hadd_task <law.root.hadd_task>` merges ROOT files using `hadd`, optionally in multiple steps to limit the length of the command line.
- {py:func}`law.pyarrow.merge_parquet_task <law.pyarrow.merge_parquet_task>` merges parquet files.

```python
law.contrib.load("root")


class MergeHistograms(Task):

    def requires(self) -> Any:
        return CreateHistograms.req(self)

    def output(self) -> Any:
        return self.local_target("histograms.root")

    def run(self) -> None:
        inputs = list(self.input()["collection"].targets.values())
        law.root.hadd_task(self, inputs, self.output(), local=True)
```

## Further reading

- The {doc}`../contrib/root` and {doc}`../contrib/pyarrow` packages document the merging functions.
