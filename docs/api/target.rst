law.target
==========

.. automodule:: law.target


.. contents::


Module ``law.target.base``
--------------------------

.. automodule:: law.target.base


Class ``Target``
^^^^^^^^^^^^^^^^

.. autoclass:: Target
   :members:


Module ``law.target.file``
--------------------------

.. automodule:: law.target.file


Class ``FileSystem``
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: FileSystem
   :members:


Class ``FileSystemTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: FileSystemTarget
   :members:


Class ``FileSystemFileTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: FileSystemFileTarget
   :members:


Class ``FileSystemDirectoryTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: FileSystemDirectoryTarget
   :members:


Functions
^^^^^^^^^

.. autofunction:: get_path

.. autofunction:: get_scheme

.. autofunction:: has_scheme

.. autofunction:: add_scheme

.. autofunction:: remove_scheme


Module ``law.target.local``
---------------------------

.. automodule:: law.target.local


Class ``LocalFileSystem``
^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: LocalFileSystem
   :members:


Class ``LocalTarget``
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: LocalTarget
   :members:


Class ``LocalFileTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: LocalFileTarget
   :members:


Class ``LocalDirectoryTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: LocalDirectoryTarget
   :members:


Module ``law.target.remote``
----------------------------

.. automodule:: law.target.remote


Class ``RemoteFileSystem``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RemoteFileSystem
   :members:


Class ``RemoteTarget``
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RemoteTarget
   :members:


Class ``RemoteFileTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RemoteFileTarget
   :members:


Class ``RemoteDirectoryTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RemoteDirectoryTarget
   :members:


Class ``RemoteCache``
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RemoteCache
   :members:


Class ``GFALInterface``
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: GFALInterface
   :members:


Module ``law.target.collection``
--------------------------------

.. automodule:: law.target.collection


Class ``TargetCollection``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TargetCollection
   :members:


Class ``SiblingFileCollection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: SiblingFileCollection
   :members:


Module ``law.target.dcache``
----------------------------

.. automodule:: law.target.dcache


Class ``DCacheFileSystem``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DCacheFileSystem
   :members:


Class ``DCacheTarget``
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DCacheTarget
   :members:


Class ``DCacheFileTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DCacheFileTarget
   :members:


Class ``DCacheDirectoryTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DCacheDirectoryTarget
   :members:


Module ``law.target.dropbox``
-----------------------------

.. automodule:: law.target.dropbox


Class ``DropboxFileSystem``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DropboxFileSystem
   :members:


Class ``DropboxTarget``
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DropboxTarget
   :members:


Class ``DropboxFileTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DropboxFileTarget
   :members:


Class ``DropboxDirectoryTarget``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DropboxDirectoryTarget
   :members:


Module ``law.target.formatter``
-------------------------------

.. automodule:: law.target.formatter


Class ``FormatterRegister``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: FormatterRegister
   :members:


Class ``Formatter``
^^^^^^^^^^^^^^^^^^^

.. autoclass:: Formatter
   :members:


Functions
^^^^^^^^^

.. autofunction:: get_formatter

.. autofunction:: find_formatters


Formatters
^^^^^^^^^^

Class ``TextFormatter``
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TextFormatter
   :members:


Class ``JSONFormatter``
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: JSONFormatter
   :members:


Class ``YAMLFormatter``
^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: YAMLFormatter
   :members:


Class ``ZipFormatter``
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ZipFormatter
   :members:


Class ``TarFormatter``
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TarFormatter
   :members:


Class ``NumpyFormatter``
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: NumpyFormatter
   :members:
