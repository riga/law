# Software in jobs and sandboxes

Tasks that run in {doc}`remote jobs <../remote_workflows>` or {doc}`sandboxes <../sandboxing>` need the same software as locally, including law, luigi and the project code itself.
How this is achieved depends on whether the remote environment can see the local file system.

## Shared file systems

Many batch systems, such as HTCondor at CERN or Slurm on HPC clusters, mount the same file systems on all nodes.
In this case, the simplest approach is a bootstrap file that sources the {doc}`setup script <project_structure>` of the project:

```shell
#!/usr/bin/env bash

action() {
    source "{{analysis_path}}/setup.sh" ""
}
action "$@"
```

The variable `{{analysis_path}}` is replaced when the job starts, using the render variables of the job config:

```python
class MyHTCondorWorkflow(law.htcondor.HTCondorWorkflow):

    def htcondor_bootstrap_file(
        self,
    ) -> str | pathlib.Path | law.LocalFileTarget | law.JobInputFile | None:
        return law.JobInputFile(
            law.util.rel_path(__file__, "bootstrap.sh"),
            share=True,
            render_job=True,
        )

    def htcondor_job_config(
        self,
        config: law.htcondor.HTCondorJobFileFactory.Config,
        job_num: int | list[int],
        branches: list[int] | list[list[int]],
    ) -> law.htcondor.HTCondorJobFileFactory.Config:
        config.render_variables["analysis_path"] = os.environ["ANALYSIS_PATH"]
        return config
```

Bash and venv sandboxes have direct access to the local file system anyway, and their setup scripts or environments are used as they are.

## Shipping software

When jobs run on machines without access to the local file system, e.g. on the WLCG, the software must be shipped.
A common approach consists of three steps:

1. Bundle the software into archives, e.g. with the bundle tasks of the {doc}`../contrib/git`, {doc}`../contrib/mercurial` and {doc}`../contrib/cms` packages, whose outputs contain a checksum of the bundled content.
2. Upload the archives to remote storage, which can be done by overwriting the output of the bundle task with a {ref}`remote target <remote-targets>`.
3. Require the bundle tasks in `workflow_requires()`, and download and unpack the archives in the bootstrap file of the jobs.

```python
law.contrib.load("git")


class BundleRepo(law.git.BundleGitRepository):

    def get_repo_path(self) -> str | pathlib.Path | law.FileSystemFileTarget:
        return os.environ["ANALYSIS_PATH"]

    def output(self) -> Any:
        return law.wlcg.WLCGFileTarget(f"/software/repo.{self.checksum}.tgz")
```

The bootstrap file then fetches the archives, e.g. with `gfal-copy`, and unpacks them before setting up the environment.

Container sandboxes forward the installed version of law automatically, see {doc}`../sandboxing`.

## Further reading

- {doc}`../remote_workflows` describes bootstrap files and job input files.
- {doc}`../sandboxing` describes the available sandbox types.
- The {doc}`../contrib/git` and {doc}`../contrib/cms` packages provide tasks for bundling software.
