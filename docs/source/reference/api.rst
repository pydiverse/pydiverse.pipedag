API
===

.. automodule:: pydiverse.pipedag
    :exclude-members: Flow, Stage

    .. autoclass:: Flow
        :special-members: __getitem__
    .. autoclass:: Stage
        :special-members: __getitem__

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
