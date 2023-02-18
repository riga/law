<center>
  <a href="https://github.com/riga/law">
    <img src="https://raw.githubusercontent.com/riga/law/master/logo.png" />
  </a>
</center>


<!-- marker-after-logo -->


[![Build status](https://github.com/riga/law/workflows/Lint%20and%20test/badge.svg)](https://github.com/riga/law/actions?query=workflow%3A%22Lint+and+test%22)
[![Docker images](https://github.com/riga/law/workflows/Deploy%20images/badge.svg)](https://github.com/riga/law/actions?query=workflow%3A%22Deploy+images%22)
[![Documentation status](https://readthedocs.org/projects/law/badge/?version=latest)](http://law.readthedocs.io/en/latest)
[![Package version](https://img.shields.io/pypi/v/law.svg?style=flat)](https://pypi.python.org/pypi/law)
[![License](https://img.shields.io/github/license/riga/law.svg)](https://github.com/riga/law/blob/master/LICENSE)
[![DOI](https://zenodo.org/badge/75482295.svg)](https://zenodo.org/badge/latestdoi/75482295)


**Note**: This project is currently under development.
Version 1.0.0 will be the first, fully documented beta release, targetted for mid 2023.

Use law to build complex and large-scale task workflows.
It is build on top of [luigi](https://github.com/spotify/luigi) and adds abstractions for **run locations**, **storage locations** and **software environments**.
Law strictly disentangles these building blocks and ensures they remain interchangeable and resource-opportunistic.

Key features:

- CLI with auto-completion and interactive status and dependency inspection.
- Remote targets with automatic retries and local caching
  - WebDAV, HTTP, Dropbox, SFTP, all WLCG protocols (srm, xrootd, dcap, gsiftp, webdav, ...)
- Automatic submission to batch systems from within tasks
  - HTCondor, LSF, gLite, ARC, Slurm
- Environment sandboxing, configurable on task level
  - Docker, Singularity, Sub-Shells, Virutal envs


<!-- marker-after-header -->


## Contents

<!-- marker-after-contents-heading -->

- [First steps](#first-steps)
   - [Installation and dependencies](#installation-and-dependencies)
   - [Usage at CERN](#usage-at-cern)
   - [Overcomplete example config](#overcomplete-example-config)
- [Projects using law](#projects-using-law)
- [Examples](#examples)
- [Further topics](#further-topics)
   - [Auto completion on the command-line](#auto-completion-on-the-command-line)
   - [Tests](#tests)
   - [Contributors](#contributors)
   - [Development](#development)


<!-- marker-before-body -->


## First steps

### Installation and dependencies

Install via [pip](https://pypi.python.org/pypi/law):

```shell
pip install law
```

This command also installs [luigi](https://pypi.python.org/pypi/luigi) and [six](https://pypi.python.org/pypi/six) (Python 2 support will be dropped soon).

If you plan to use remote targets, the (default) implementation also requires [gfal2](https://dmc-docs.web.cern.ch/dmc-docs/gfal2/gfal2.html) and [gfal2-python](https://pypi.python.org/pypi/gfal2-python) (optional) to be installed, either via pip or conda.

```shell
conda install -c conda-forge gfal2 gfal2-util
```


### Usage at CERN

See the [wiki](https://github.com/riga/law/wiki/Usage-at-CERN).


### Overcomplete example config

See [law.cfg.example](https://github.com/riga/law/tree/master/law.cfg.example).


## Projects using law

- CMS Di-Higgs Inference Tools:
  - Basis for statistical analysis for all Di-Higgs searches in CMS, starting at datacard-level
  - [repo](https://gitlab.cern.ch/hh/tools/inference), [docs](https://cms-hh.web.cern.ch/cms-hh/tools/inference/index.html)
- CMS B-Tag SF Measurement:
  - Automated workflow for deriving shape-calibrating b-tag scale factors, starting at MiniAOD-level
  - [repo](https://github.com/cms-btv-pog/jet-tagging-sf)
- CMS Tau POG ML Tools:
  - Preprocessing pipeline for ML trainings in the TAU group
  - [repo](https://github.com/cms-tau-pog/TauMLTools)
- CMS HLT Config Parser:
  - Collects information from various databases (HLT, bril, etc.) and shows menus, triggers paths, filter names for configurable MC datasets or data runs
  - [repo](https://github.com/riga/cms-hlt-parser)
- UHH-CMS Analysis Framework:
  - Python based, fully automated, columnar framework, including job submission, resolution of systematics and ML pipelines, starting at NanoAOD-level with an optimized multi-threaded column reader
  - [repo](https://github.com/uhh-cms/analysis_playground), [docs](http://analysis_playground.readthedocs.io), [task structure](https://github.com/uhh-cms/analysis_playground/issues/25)
- RWTH-CMS Analysis Framework:
  - Basis for multiple CMS analyses ranging from Di-Higgs, to single Higgs and b-tag SF measurements, starting at NanoAOD-level and based on coffea processors
  - [repo](https://git.rwth-aachen.de/3pia/cms_analyses/common/-/tree/master/)
- CIEMAT-CMS Analysis Framework:
  - Python and RDataFrame based framework starting from NanoAOD and targetting multiple CMS analyses
  - [repo](https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/)
- CMS 3D Z+jet 13TeV analysis
  - Analysis workflow management from NTuple production to final plots and fits
  - [repo](https://gitlab.etp.kit.edu/cverstege/zjet-analysis)
- NP-correction derivation tool
  - MC generation with Herwig and analysis of generated events with Rivet
  - [repo](https://github.com/HerrHorizontal/herwig-run)
- CMS SUSY Searches at DESY
  - Analysis framework for CMS SUSY searches going from custom NanoAODs -> NTuple production -> DNN-based inference -> final plots and fits
  - [repo](https://github.com/frengelk/Susy1LeptonAnalysis)

If your project uses law but is not yet listed here, feel free to open a pull request or mention your project details in a new [issue](https://github.com/riga/law/issues/new?assignees=riga&labels=docs&template=register-project.md) and it will be added.


## Examples

All examples can be run either in a Jupyter notebook or a dedicated docker container.
For the latter, do

```shell
docker run -ti riga/law:example <example_name>
```

- [loremipsum](https://github.com/riga/law/tree/master/examples/loremipsum): The *hello world* example of law.
- [workflows](https://github.com/riga/law/tree/master/examples/workflows): Law workflows.
- [notebooks](https://github.com/riga/law/tree/master/examples/notebooks): Examples showing how to use and work with law in notebooks.
- [dropbox_targets](https://github.com/riga/law/tree/master/examples/dropbox_targets): Working with targets that are stored on Dropbox.
- [wlcg_targets](https://github.com/riga/law/tree/master/examples/wlcg_targets): Working with targets that are stored on WLCG storage elements (dCache, EOS, ...). TODO.
- [htcondor_at_vispa](https://github.com/riga/law/tree/master/examples/htcondor_at_vispa): HTCondor workflows at the [VISPA service](https://vispa.physik.rwth-aachen.de).
- [htcondor_at_cern](https://github.com/riga/law/tree/master/examples/htcondor_at_cern): HTCondor workflows at the CERN batch infrastructure.
- [sequential_htcondor_at_cern](https://github.com/riga/law/tree/master/examples/sequential_htcondor_at_cern): Continuation of the [htcondor_at_cern](https://github.com/riga/law/tree/master/examples/htcondor_at_cern) example, showing sequential jobs that eagerly start once jobs running previous requirements succeeded.
- [htcondor_at_naf](https://github.com/riga/law/tree/master/examples/htcondor_at_naf): HTCondor workflows at German [National Analysis Facility (NAF)](https://confluence.desy.de/display/IS/NAF+-+National+Analysis+Facility).
- [slurm_at_maxwell](https://github.com/riga/law/tree/master/examples/slurm_at_maxwell): Slurm workflows at the [Desy Maxwell cluster](https://confluence.desy.de/display/MXW/Maxwell+Cluster).
- [grid_at_cern](https://github.com/riga/law_example_WLCG): Workflows that run jobs and store data on the WLCG.
- [lsf_at_cern](https://github.com/riga/law/tree/master/examples/lsf_at_cern): LSF workflows at the CERN batch infrastructure.
- [docker_sandboxes](https://github.com/riga/law/tree/master/examples/docker_sandboxes): Environment sandboxing using Docker. TODO.
- [singularity_sandboxes](https://github.com/riga/law/tree/master/examples/singularity_sandboxes): Environment sandboxing using Singularity. TODO.
- [subshell_sandboxes](https://github.com/riga/law/tree/master/examples/subshell_sandboxes): Environment sandboxing using Subshells. TODO.
- [parallel_optimization](https://github.com/riga/law/tree/master/examples/parallel_optimization): Parallel optimization using [scikit optimize](https://scikit-optimize.github.io).
- [notifications](https://github.com/riga/law/tree/master/examples/notifications): Demonstration of slack and telegram task status notifications..
- [CMS Single Top Analysis](https://github.com/riga/law_example_CMSSingleTopAnalysis): Simple physics analysis using law.


## Further topics

### Auto completion on the command-line

**bash**

```shell
source "$( law completion )"
```

**zsh**

zsh is able to load and evaluate bash completion scripts via `bashcompinit`.
In order for `bashcompinit` to work, you should run `compinstall` to enable completion scripts:

```shell
autoload -Uz compinstall && compinstall
```

After following the instructions, these lines should be present in your `~/.zshrc`:

```shell
# The following lines were added by compinstall
zstyle :compinstall filename '~/.zshrc'

autoload -Uz +X compinit && compinit
autoload -Uz +X bashcompinit && bashcompinit
# End of lines added by compinstall
```

If this is the case, just source the law completion script (which internally enables `bashcompinit`) and you're good to go:

```shell
source "$( law completion )"
```


### Tests

To run and test law, there are various docker `riga/law` images available on the [DockerHub](https://cloud.docker.com/u/riga/repository/docker/riga/law), corresponding to different OS and Python versions.

|    OS    | Python |                   Tags                   |
| -------- | ------ | ---------------------------------------- |
| Centos 8 |   3.10 | c8-py310, c8-py3, c8, py310, py3, latest |
| Centos 8 |    3.9 | c8-py39, py39                            |
| Centos 8 |    3.8 | c8-py38, py38                            |
| Centos 8 |    3.7 | c8-py37, py37                            |
| Centos 7 |   3.10 | c7-py310                                 |
| Centos 7 |    3.9 | c7-py39, c7-py3, c7                      |
| Centos 7 |    3.8 | c7-py38                                  |
| Centos 7 |    3.7 | c7-py37                                  |
| Centos 7 |    3.6 | c7-py36, py36                            |
| Centos 7 |    2.7 | c7-py27, c7-py2, py27, py2               |

```shell
docker run -ti riga/law:latest
```

### Contributors

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://marcelrieger.com"><img src="https://avatars.githubusercontent.com/u/1908734?v=4?s=100" width="100px;" alt="Marcel R."/><br /><sub><b>Marcel R.</b></sub></a><br /><a href="https://github.com/riga/law/commits?author=riga" title="Code">💻</a> <a href="https://github.com/riga/law/pulls?q=is%3Apr+reviewed-by%3Ariga" title="Reviewed Pull Requests">👀</a> <a href="#maintenance-riga" title="Maintenance">🚧</a> <a href="https://github.com/riga/law/commits?author=riga" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->


### Development

- Source hosted at [GitHub](https://github.com/riga/law)
- Report issues, questions, feature requests on [GitHub Issues](https://github.com/riga/law/issues)


<!-- marker-after-body -->
