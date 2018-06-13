.. figure:: https://raw.githubusercontent.com/riga/law/master/logo.png
   :target: https://github.com/riga/law
   :align: center
   :alt: law logo

.. image:: https://img.shields.io/travis/riga/law/master.svg?style=flat
   :target: https://travis-ci.org/riga/law
   :alt: Build status

.. image:: https://readthedocs.org/projects/law/badge/?version=latest
   :target: http://law.readthedocs.io/en/latest
   :alt: Documentation status

.. image:: https://img.shields.io/pypi/v/law.svg?style=flat
   :target: https://pypi.python.org/pypi/law
   :alt: Package version

.. image:: https://img.shields.io/pypi/l/law.svg?style=flat
   :target: https://pypi.python.org/pypi/law
   :alt: License


**Note**: This project is currently under development. Version 0.1.0 will be the first, fully documented alpha release.

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


Examples
========

- `loremipsum <https://github.com/riga/law/tree/master/examples/loremipsum>`__: The *hello world* of law.
- `workflows <https://github.com/riga/law/tree/master/examples/workflows>`__: Law workflows.
- `dropbox_targets <https://github.com/riga/law/tree/master/examples/dropbox_targets>`__: Working with targets that are stored on Dropbox.
- `wlcg_targets <https://github.com/riga/law/tree/master/examples/wlcg_targets>`__: Working with targets that are stored on WLCG storage elements (dCache, EOS, ...). TODO.
- `htcondor_at_vispa <https://github.com/riga/law/tree/master/examples/htcondor_at_vispa>`__: HTCondor workflows at the `VISPA service <https://vispa.physik.rwth-aachen.de>`__.
- `htcondor_at_cern <https://github.com/riga/law/tree/master/examples/htcondor_at_cern>`__: HTCondor workflows at the CERN batch infrastructure.
- `grid_at_cern <https://github.com/riga/law/tree/master/examples/grid_at_cern>`__: Workflows that run on the WLCG. TODO.
- `lsf_at_cern <https://github.com/riga/law/tree/master/examples/lsf_at_cern>`__: LSF workflows at the CERN batch infrastructure.
- `docker_sandboxes <https://github.com/riga/law/tree/master/examples/docker_sandboxes>`__: Environment sandboxing using Docker. TODO.
- `singularity_sandboxes <https://github.com/riga/law/tree/master/examples/singularity_sandboxes>`__: Environment sandboxing using Singularity. TODO.
- `subshell_sandboxes <https://github.com/riga/law/tree/master/examples/subshell_sandboxes>`__: Environment sandboxing using Subshells. TODO.
- `parallel_optimization <https://github.com/riga/law/tree/master/examples/parallel_optimization>`__: Parallel optimization using `scikit optimize <https://scikit-optimize.github.io>`__.
- `CMS Single Top Analysis <https://github.com/riga/law_example_CMSSingleTopAnalysis>`__: Simple physics analysis using law.


Development
===========

- Source hosted at `GitHub <https://github.com/riga/law>`__
- Report issues, questions, feature requests on `GitHub Issues <https://github.com/riga/law/issues>`__


.. marker-after-body


Authors
=======

- `Marcel R. <https://github.com/riga>`__


License
=======

The MIT License (MIT)

Copyright (c) 2016-2018 Marcel R.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
