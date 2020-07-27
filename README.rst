.. figure:: https://raw.githubusercontent.com/riga/law/master/logo.png
   :target: https://github.com/riga/law
   :align: center
   :alt: law logo

.. image:: https://github.com/riga/law/workflows/Lint%20and%20test/badge.svg
   :target: https://github.com/riga/law/actions?query=workflow%3A%22Lint+and+test%22
   :alt: Build status

.. image:: https://readthedocs.org/projects/law/badge/?version=latest
   :target: http://law.readthedocs.io/en/latest
   :alt: Documentation status

.. image:: https://img.shields.io/pypi/v/law.svg?style=flat
   :target: https://pypi.python.org/pypi/law
   :alt: Package version

.. image:: https://img.shields.io/github/license/riga/law.svg
   :target: https://github.com/riga/law/blob/master/LICENSE
   :alt: License

.. image:: https://zenodo.org/badge/75482295.svg
   :target: https://zenodo.org/badge/latestdoi/75482295
   :alt: DOI


**Note**: This project is currently under development. Version 0.1.0 will be the first, fully documented beta release, targetted for early 2020.

Use law to build complex and large-scale task workflows. It is build on top of `luigi <https://github.com/spotify/luigi>`__ and adds abstractions for **run locations**, **storage locations** and **software environments**. Law strictly disentangles these building blocks and ensures they remain interchangeable and resource-opportunistic.

Key features:

- CLI with auto-completion and interactive status and dependency inspection.
- Remote targets with automatic retries and local caching
   - WebDAV, HTTP, Dropbox, SFTP, all WLCG protocols (srm, xrootd, rfio, dcap, gsiftp, ...)
- Automatic submission to batch systems from within tasks
   - HTCondor, LSF, gLite, ARC
- Environment sandboxing, configurable on task level
   - Docker, Singularity, Sub-Shells


.. marker-after-header


First steps
===========

Installation and Dependencies
-----------------------------

Install via `pip <https://pypi.python.org/pypi/law>`__:

.. code-block:: bash

   pip install law

This command also installs `luigi <https://pypi.python.org/pypi/luigi>`__ and `six <https://pypi.python.org/pypi/six>`__.

Remote targets also require `gfal2 <http://dmc.web.cern.ch/projects/gfal-2/home>`__ and `gfal2-python <https://pypi.python.org/pypi/gfal2-python>`__ (optional, also via pip) to be installed.


Docker Images
-------------

To run and test law, there are three docker images available on the `DockerHub <https://cloud.docker.com/u/riga/repository/docker/riga/law>`__, corresponding to Python versions 2.7, 3.7 and 3.8. They are based on CentOS 7 and ship with the dependencies listed above, including gfal2.

.. code-block:: bash

   docker run -ti riga/law:latest


Tags:

- ``latest``, ``py3``, ``py38``: Latest Python 3.8
- ``py37``: Latest Python 3.7
- ``py2``, ``py27``: Python 2.7.5
- ``example``: Example runner, based on ``latest`` (see `below <#examples>`__)


`Usage at CERN <https://github.com/riga/law/wiki/Usage-at-CERN>`__
------------------------------------------------------------------


`Overcomplete example config <https://github.com/riga/law/tree/master/law.cfg.example>`__
-----------------------------------------------------------------------------------------


Examples
========

All examples can be run either in a Jupyter notebook or a dedicated docker container. For the latter, do

.. code-block:: bash

   docker run -ti riga/law:example <example_name>

- `loremipsum <https://github.com/riga/law/tree/master/examples/loremipsum>`__: The *hello world* example of law.
- `workflows <https://github.com/riga/law/tree/master/examples/workflows>`__: Law workflows.
- `dropbox_targets <https://github.com/riga/law/tree/master/examples/dropbox_targets>`__: Working with targets that are stored on Dropbox.
- `wlcg_targets <https://github.com/riga/law/tree/master/examples/wlcg_targets>`__: Working with targets that are stored on WLCG storage elements (dCache, EOS, ...). TODO.
- `htcondor_at_vispa <https://github.com/riga/law/tree/master/examples/htcondor_at_vispa>`__: HTCondor workflows at the `VISPA service <https://vispa.physik.rwth-aachen.de>`__.
- `htcondor_at_cern <https://github.com/riga/law/tree/master/examples/htcondor_at_cern>`__: HTCondor workflows at the CERN batch infrastructure.
- `grid_at_cern <https://github.com/riga/law_example_WLCG>`__: Workflows that run jobs and store data on the WLCG.
- `lsf_at_cern <https://github.com/riga/law/tree/master/examples/lsf_at_cern>`__: LSF workflows at the CERN batch infrastructure.
- `docker_sandboxes <https://github.com/riga/law/tree/master/examples/docker_sandboxes>`__: Environment sandboxing using Docker. TODO.
- `singularity_sandboxes <https://github.com/riga/law/tree/master/examples/singularity_sandboxes>`__: Environment sandboxing using Singularity. TODO.
- `subshell_sandboxes <https://github.com/riga/law/tree/master/examples/subshell_sandboxes>`__: Environment sandboxing using Subshells. TODO.
- `parallel_optimization <https://github.com/riga/law/tree/master/examples/parallel_optimization>`__: Parallel optimization using `scikit optimize <https://scikit-optimize.github.io>`__.
- `notifications <https://github.com/riga/law/tree/master/examples/notifications>`__: Demonstration of slack and telegram task status notifications..
- `CMS Single Top Analysis <https://github.com/riga/law_example_CMSSingleTopAnalysis>`__: Simple physics analysis using law.


Auto completion on the command-line
===================================

bash
----

.. code-block:: shell

   source "$( law completion )"


zsh
---

zsh is able to load and evaluate bash completion scripts via ``bashcompinit``. In order for ``bashcompinit`` to work, you should run ``compinstall`` to enable completion scripts:

.. code-block:: shell

   autoload -Uz compinstall && compinstall

After following the instructions, these lines should be present in your ~/.zshrc:

.. code-block:: shell

   # The following lines were added by compinstall
   zstyle :compinstall filename '~/.zshrc'

   autoload -Uz compinit
   compinit
   # End of lines added by compinstall

If this is the case, just source the law completion script (which internally enables ``bashcompinit``) and you're good to go:

.. code-block:: shell

   source "$( law completion )"


Development
===========

- Source hosted at `GitHub <https://github.com/riga/law>`__
- Report issues, questions, feature requests on `GitHub Issues <https://github.com/riga/law/issues>`__


.. marker-after-body

