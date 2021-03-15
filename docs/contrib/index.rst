**contrib** Packages
====================

Law contains a collection of so-called *contrib* packages that can be loaded manually when needed instead of being automatically loaded on each ``import law`` call.

The following example shows how a package (e.g. the :py:mod:`~law.docker` package for working with containers) is loaded via :py:func:`~law.contrib.load`.

.. code-block:: python

   import law

   law.contrib.load("docker")
   law.docker.DockerSandbox(...)

   # the following is similar but does not add the law.docker shorthand
   import law.contrib.docker
   law.contrib.docker.DockerSandbox(...)


.. automodule:: law.contrib
   :members:


**Available pacakges**:

.. toctree::
   :maxdepth: 1

   arc
   cms
   coffea
   docker
   dropbox
   gfal
   git
   glite
   hdf5
   htcondor
   ipython
   keras
   lsf
   matplotlib
   mercurial
   numpy
   profiling
   rich
   root
   singularity
   slack
   tasks
   telegram
   tensorflow
   wlcg
