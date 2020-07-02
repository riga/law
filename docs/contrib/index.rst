**contrib** Packages
====================

.. automodule:: law.contrib

To use on of the following packages in your code, you must import them explicitly, e.g.

.. code-block:: python

   import law

   import law.contrib.docker
   law.contrib.docker.DockerSandbox(...)

   # or (recommended)
   law.contrib.load("docker")
   law.docker.DockerSandbox(...)


.. toctree::
   :maxdepth: 1

   arc
   cms
   coffea
   docker
   dropbox
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
   rich
   root
   singularity
   slack
   tasks
   telegram
   tensorflow
   wlcg
