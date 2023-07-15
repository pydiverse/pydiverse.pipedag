API
===

.. automodule:: pydiverse.pipedag
    :members:
    :inherited-members:
    :exclude-members: Flow, Stage, materialize

    .. autoclass:: Flow
        :members:
        :inherited-members:
        :special-members: __getitem__
    .. autoclass:: Stage
        :members:
        :inherited-members:
        :special-members: __getitem__
    .. autodecorator:: materialize

Backend Classes
===============

Table Store
-----------
.. autoclass:: pydiverse.pipedag.backend.table.SQLTableStore

SQLTableStore Dialects
^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: pydiverse.pipedag.backend.table.sql.dialects.PostgresTableStore
.. autoclass:: pydiverse.pipedag.backend.table.sql.dialects.DuckDBTableStore
.. autoclass:: pydiverse.pipedag.backend.table.sql.dialects.MSSqlTableStore
.. autoclass:: pydiverse.pipedag.backend.table.sql.dialects.IBMDB2TableStore

Local Table Cache
^^^^^^^^^^^^^^^^^
.. autoclass:: pydiverse.pipedag.backend.table.cache.ParquetTableCache

Blob Store
----------
.. autoclass:: pydiverse.pipedag.backend.blob.FileBlobStore

Lock Manager
------------
.. autoclass:: pydiverse.pipedag.backend.lock.DatabaseLockManager
.. autoclass:: pydiverse.pipedag.backend.lock.ZooKeeperLockManager
.. autoclass:: pydiverse.pipedag.backend.lock.FileLockManager
.. autoclass:: pydiverse.pipedag.backend.lock.NoLockManager

Orchestration Engine
--------------------
.. autoclass:: pydiverse.pipedag.engine.SequentialEngine
.. autoclass:: pydiverse.pipedag.engine.DaskEngine

.. py:class:: PrefectEngine
   :canonical: pydiverse.pipedag.engine.PrefectEngine

   Alias for either
   :class:`PrefectOneEngine <pydiverse.pipedag.engine.PrefectOneEngine>` or
   :class:`PrefectTwoEngine <pydiverse.pipedag.engine.PrefectTwoEngine>`
   depending on the version of Prefect that is installed.

.. autoclass:: pydiverse.pipedag.engine.PrefectOneEngine
.. autoclass:: pydiverse.pipedag.engine.PrefectTwoEngine
