Pipedag Config File
===================

The `pipedag.toml` file is used to configure pipedag.

When executing a flow, pipedag searches for the config file in the following directories:

- The current working directory
- Any parent directories of the working directory
- The user folder

Alternatively you can also specify a config file path using the `PIPEDAG_CONFIG` environment variable.


name
----

The name of the project. **Optional**

Currently, the main purpose of the project name is as a namespace for the lock manager.
That way you can use the same manager (e.g. Zookeeper) for two projects that run on different databases without causing any issues.


network_interface
-----------------

The network interface to use for communicating with the parent process. **Optional**

If no value is specified, `127.0.0.1` is used.
To specify a IPv6 address, you must surround it in square brackets.


auto_table
----------

A list of tables classes.
If a materializing task returns an instance of any class in this list, it automatically gets materialized to the table store.
**Optional**

For example, if you want to store all pandas dataframes and pydiverse transform table objects in the table store, you would specify it like this:

.. code-block:: toml

    auto_table = [
        "pandas.DataFrame",
        "pydiverse.transform.Table",
    ]


auto_blob
---------

The same as `auto_table` just for blobs. **Optional**


table_store
-----------

This section describes the table store to use. **Required**

The `class` key/value is used to define which class to use as a the table store.
Any other key/value pairs in this section are backend specific and either get passed to the classes `__init__` or `_init_conf_` method.

.. code-block:: toml

    [table_store]
    class = "pydiverse.pipedag.backend.table.SQLTableStore"
    engine = "postgresql://postgres:pipedag@127.0.0.1/pipedag"


blob_store
----------

This section describes which blob store to use. **Required**

It is structured the same way as the `table_store` section.

.. code-block:: toml

    [blob_store]
    class = "pydiverse.pipedag.backend.blob.FileBlobStore"
    base_path = "/tmp/pipedag/blobs"


lock_manager
------------

This section describes the lock manager to use. **Required**

It is structured the same way as the `table_store` section.
If you are the only person working on a project, you can choose not to use a lock manager at all (*not recommended for production*),
in which case you set `class = "pydiverse.pipedag.backend.lock.NoLockManager"`.

.. code-block:: toml

    [lock_manager]
    class = "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
    hosts = "localhost:2181"


engine
------

This section describes the default engine that should be used to execute a flow. **Optional**

Once again, this section is structured the same way as the `table_store` section.
If you don't specify this section, you must pass an Engine object to the `flow.run()` method.

.. code-block:: toml

    [engine]
    class = "pydiverse.pipedag.engine.PrefectEngine"