(remote-workflows)=

# Remote workflows and jobs

Remote workflows process the branches of a {doc}`workflow <workflows>` as jobs on a batch system, such as HTCondor or Slurm.
The workflow submits the jobs, monitors them until they are finished, and resubmits failed ones.
Since branch tasks run the same way locally and in jobs, the same task can run either way, selected via `--workflow`.

## How it works

When a remote workflow runs, it goes through the following steps:

1. **Requirements:** The requirements returned by {py:meth}`workflow_requires() <law.workflow.base.BaseWorkflow.workflow_requires>` are resolved locally, before any job is submitted.
2. **Grouping:** Branches are grouped into jobs, each processing `--tasks-per-job` branches, which defaults to one.
   Jobs whose branches are all complete already are skipped.
3. **Submission:** For each job, a job file is created and submitted to the batch system.
   The job ids and further data are stored in the *jobs file*, a json file in the output directory of the workflow.
4. **Polling:** The workflow queries the status of all jobs every `--poll-interval` and prints a status line.
   Failed jobs are resubmitted up to `--retries` times.
5. **Completion:** The workflow succeeds when enough branches are complete, as defined by `--acceptance`, and fails when more jobs failed than `--tolerance` allows.

Inside each job, the law job script `law_job.sh` runs the branches of that job one after another via `law run <Task> --branch <b>`, using the same parameters as the workflow.

```{important}
Branch requirements are resolved again inside the job, and incomplete ones are run within the job as well.
This can be intended, e.g. in {ref}`pilot mode <pilot-mode>`, where branches of an upstream workflow deliberately run inside the jobs of the downstream workflow instead of being submitted separately.

However, jobs should never run while requirements of the workflow itself are incomplete.
Declare everything that must exist before jobs start, such as software bundles, in `workflow_requires()`.
```

## Setting up a remote workflow

Batch systems differ a lot between sites, so law does not try to configure them automatically.
Instead, a project usually defines its own base class once, which inherits from the workflow class of the batch system and implements a few hooks.
The example below is adapted from the [HTCondor at CERN example](https://github.com/riga/law/tree/master/examples/htcondor_at_cern):

```python
import os
import pathlib

import law

law.contrib.load("htcondor")


class MyHTCondorWorkflow(law.htcondor.HTCondorWorkflow):

    def htcondor_output_directory(
        self,
    ) -> str | pathlib.Path | law.FileSystemDirectoryTarget:
        # directory where the jobs file and (optionally) logs are stored
        return law.LocalDirectoryTarget(f"$DATA_PATH/jobs/{self.task_family}")

    def htcondor_bootstrap_file(
        self,
    ) -> str | pathlib.Path | law.LocalFileTarget | law.JobInputFile | None:
        # script executed in each job before the tasks run, e.g. to set up software
        bootstrap_file = law.util.rel_path(__file__, "bootstrap.sh")
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def htcondor_job_config(
        self,
        config: law.htcondor.HTCondorJobFileFactory.Config,
        job_num: int | list[int],
        branches: list[int] | list[list[int]],
    ) -> law.htcondor.HTCondorJobFileFactory.Config:
        # variables rendered into all files sent with the job, e.g. {{analysis_path}}
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")
        # additional, site-specific settings for the submission file
        config.custom_content.append(("getenv", "true"))
        return config
```

Tasks then inherit from this class, and optionally from {py:class}`law.LocalWorkflow <law.workflow.local.LocalWorkflow>` to also allow local processing:

```python
class ConvertFiles(MyHTCondorWorkflow, law.LocalWorkflow):
    ...
```

### Hooks

All hook methods are prefixed with the type of the workflow, e.g. `htcondor_` or `slurm_`.
The most common ones are:

| Hook | Description |
| --- | --- |
| `<type>_output_directory()` | Directory of the jobs file. Must be implemented. |
| `<type>_bootstrap_file()` | Script that is sourced in each job before the tasks run, typically to set up the software environment. |
| `<type>_stageout_file()` | Script that is executed in each job after the tasks ran. |
| `<type>_job_config(config, job_num, branches)` | Modifies the configuration of the job file factory before the job file is written, e.g. to set resources or site-specific options. |
| `<type>_job_resources(job_num, branches)` | Resources used by a job, which are taken into account by the central scheduler. |
| `<type>_workflow_requires()` | Additional requirements that only apply when the workflow runs as this workflow type. |
| `<type>_poll_callback(poll_data)` | Called after each status query, e.g. to adjust the number of parallel jobs. Returning *False* stops the polling. |
| `<type>_check_job_completeness()` | Whether the outputs of branches are checked when their job is reported as finished. |
| `<type>_destination_info(info)` | Information about the submission destination shown in status lines. |

See the workflow class of each batch system in the table below for the full list.

### Job input files

Files that are sent with jobs, such as the bootstrap file, can be wrapped in a {py:class}`law.JobInputFile <law.job.base.JobInputFile>` to control how they are handled:

- `share=True` copies the file only once into the submission directory instead of once per job.
- `render_job=True` replaces variables such as `{{analysis_path}}` inside the job, using the `render_variables` of the job config.
- `render_local=True` replaces them already during submission.

Further input files are added in the `<type>_job_config()` hook via the `input_files` dictionary of the config.
Their keys are added to the render variables, holding the path of the file as seen by the job, so that other rendered files can refer to them:

```python
import os
import pathlib

import law

law.contrib.load("slurm")


class MySlurmWorkflow(law.slurm.SlurmWorkflow):

    def slurm_output_directory(
        self,
    ) -> str | pathlib.Path | law.FileSystemDirectoryTarget:
        return law.LocalDirectoryTarget(f"$DATA_PATH/jobs/{self.task_family}")

    def slurm_bootstrap_file(
        self,
    ) -> str | pathlib.Path | law.LocalFileTarget | law.JobInputFile | None:
        return law.JobInputFile(
            law.util.rel_path(__file__, "bootstrap.sh"),
            render_job=True,
        )

    def slurm_job_config(
        self,
        config: law.slurm.SlurmJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> law.slurm.SlurmJobFileFactory.Config:
        # a setup script shared by all jobs, copied only once
        config.input_files["setup_file"] = law.JobInputFile(
            law.util.rel_path(__file__, "setup.sh"),
            share=True,
        )
        # a job specific configuration file, with variables replaced during submission
        config.input_files["job_config"] = law.JobInputFile(
            law.util.rel_path(__file__, "job_config.yaml"),
            render_local=True,
        )
        config.render_variables["analysis_path"] = os.environ["ANALYSIS_PATH"]
        # additional settings for the submission file
        config.custom_content.append(("mem", "4G"))
        return config
```

The bootstrap file can then source the setup script via the `setup_file` variable:

```shell
#!/usr/bin/env bash

action() {
    source "{{setup_file}}" ""
    export JOB_CONFIG="{{job_config}}"
}
action "$@"
```

Within the job, the paths of all input files are available, and the variables listed in the header of `law_job.sh`, such as `LAW_JOB_HOME`, are set.

## Controlling jobs from the command line

Remote workflows add parameters that control the submission and polling:

| Parameter | Description |
| --- | --- |
| `--tasks-per-job` | Number of branches processed per job, defaults to 1. |
| `--parallel-jobs` | Maximum number of jobs running at the same time, unlimited by default. |
| `--retries` | Number of automatic resubmissions per job, defaults to 5. |
| `--walltime` | Maximum runtime of jobs, after which they are considered failed. The default unit is hours. |
| `--poll-interval` | Time between two status queries. The default unit is minutes. |
| `--job-workers` | Number of branches processed in parallel within a job. |
| `--no-poll` | Only submit jobs and exit without polling. |
| `--ignore-submission` | Ignore an existing jobs file and submit new jobs. |
| `--transfer-logs` | Transfer job logs to the output directory. |
| `--cancel-jobs` | Cancel the jobs stored in the jobs file. |
| `--cleanup-jobs` | Clean up the jobs stored in the jobs file. |

When a workflow runs again while its jobs file exists, e.g. after it was interrupted or when `--no-poll` was used, it does not submit new jobs.
Instead, it resumes polling the jobs stored in the file.
Use `--ignore-submission` to start over with new jobs.

## Supported batch systems

| Package | Workflow class | Batch system |
| --- | --- | --- |
| {doc}`contrib/htcondor` | {py:class}`law.htcondor.HTCondorWorkflow` | [HTCondor](https://htcondor.org) |
| {doc}`contrib/slurm` | {py:class}`law.slurm.SlurmWorkflow` | [Slurm](https://slurm.schedmd.com) |
| {doc}`contrib/lsf` | {py:class}`law.lsf.LSFWorkflow` | [IBM Spectrum LSF](https://www.ibm.com/products/hpc-workload-management) |
| {doc}`contrib/arc` | {py:class}`law.arc.ARCWorkflow` | [ARC](http://www.nordugrid.org/arc) computing elements of the WLCG |
| {doc}`contrib/glite` | {py:class}`law.glite.GLiteWorkflow` | gLite CREAM computing elements of the WLCG |
| {doc}`contrib/cms` | {py:class}`law.cms.CrabWorkflow` | The CMS CRAB system (without CMSSW constraint) |

The examples for [HTCondor at CERN](https://github.com/riga/law/tree/master/examples/htcondor_at_cern), [HTCondor at NAF](https://github.com/riga/law/tree/master/examples/htcondor_at_naf), [LSF at CERN](https://github.com/riga/law/tree/master/examples/lsf_at_cern) and [Slurm at Maxwell](https://github.com/riga/law/tree/master/examples/slurm_at_maxwell) show complete setups.

## Behind the scenes

Remote workflows are built from a few components that can be customized when needed:

- The *job manager*, a {py:class}`~law.job.base.BaseJobManager`, talks to the batch system to submit, query, cancel and clean up jobs.
- The *job file factory*, a {py:class}`~law.job.base.BaseJobFileFactory`, creates the job files that are submitted, in a directory configured in the {ref}`[job] <job-section>` config section.
- The *job dashboard*, a {py:class}`~law.job.dashboard.BaseJobDashboard`, can optionally publish job status updates to external monitoring services.

The job script reports failures through exit codes, e.g. 20 when the bootstrap file failed and 60 when one of the tasks failed.
The full list is documented in the header of `law_job.sh`.

## Further reading

- {doc}`workflows` describes workflows in general.
- {doc}`practices/software` explains how software is made available in jobs.
- {doc}`practices/workflow_optimizations` explains how to avoid separate job submissions for chained workflows.
- The [sequential HTCondor at CERN example](https://github.com/riga/law/tree/master/examples/sequential_htcondor_at_cern) shows jobs that start as soon as their requirements are complete.
- The {ref}`[job] <job-section>` config section lists all job options.
