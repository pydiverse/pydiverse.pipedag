*******************
Pipedag Config File
*******************

introduction
============

The `pipedag.yaml` file is used to configure pipedag.

When executing a flow, pipedag searches for the config file in the following directories:

- The current working directory
- Any parent directories of the working directory
- The user folder

**Attention: pipedag config files may be used for code execution under a different user ID.**
Pipedag may be instructed to read yaml files with key-value pairs and send content to arbitrary
hosts. It can also be instructed to send content of environment variables (i.e. security tokens).
Furthermore it can be instructed to load python classes in the python environment and the constructor
be fed with arbitrary keyword arguments.

Alternatively you can also specify a config file path using the `PIPEDAG_CONFIG` environment variable.

One of the main goals of pipedag is to make it easy to keep multiple instances of a data processing DAG up and running
at the same time and maximize development efficiency. Thus it is important to allow running the same flow with full
input data, a sampled version, and a bare minimum data set for quick smoke tests of newly written code. We would call
setups for those three input data sets 'pipedag instances' and they could get names like `full, midi, mini`.

There is no hard binding of data processing code called *flow* and a pipedag configuration file. The idea, however,
is rather a one-to-one relationship. For each flow, the pipedag configuration file offers configuration options for
multiple pipedag instances. So the following parameters can either be set per instance:

.. code-block::

    instances:
        myinstance:
            <parameter>: <value>

per flow:

.. code-block::

    flows:
        myflow:
            <parameter>: <value>

or per flow per instance:

.. code-block::

    flows:
        myflow:
            instances:
                myinstance:
                    <parameter>: <value>

The `instances:` section would be interpreted first, then it can be overwritten by the `flows: xxx: instances:` section,
and finally it can again by overwritten by the top level attributes in the `flows: xxx:` section.

There is a special `__any__` instance and `__any__` flow which are base attributes which are inherited by all custom
named instances or flows respectively. When given

simple example configuration
---------------------

.. code-block:: yaml

    name: pipedag_tests

    instances:
      __any__:
        auto_table: ["pandas.DataFrame", "sqlalchemy.Table"]

        instance_id: pipedag_default
        table_store:
          class: "pydiverse.pipedag.backend.table.SQLTableStore"
          url: "postgresql://sa:Pydiverse23@127.0.0.1:6543/{instance_id}"
          create_database_if_not_exists: True

        blob_store:
          class: "pydiverse.pipedag.backend.blob.FileBlobStore"
          base_path: "/tmp/pipedag/blobs"

        lock_manager:
          class: "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
          hosts: "localhost:2181"

        orchestration:
          class: "pydiverse.pipedag.engine.SequentialEngine"


top level attributes
====================


name
----

The name of the pipedag configuration. It is also used as the default name for a flow connected with this configuration.

strict_instance_lookup
----------------------

`strict_instance_lookup=true` (default) means that a lookup for an instance that was not explicitly specified in the
pipedag config will fail. A lookup for `instance=__any__` will succeed if such a base instance exists.

instance/flow level attributes
=========================

Instance or flow level attributes can be placed in the following positions and will be overwritten in this order:

.. code-block:: yaml

    instances:
        __any__:
            attribute: value
    instances:
        <xxx>:
            attribute: value
    flows:
        __any__:
            instances:
                __any__:
                    attribute: value
    flows:
        __any__:
            instances:
                <xxx>:
                    attribute: value
    flows:
        __any__:
            attribute: value
    flows:
        <yyy>:
            instances:
                <xxx>:
                    attribute: value
    flows:
        <yyy>:
            attribute: value

Between each of those overwrite steps, meta-attributes like `technical_setup`_, `table_store_connection`_ and
`blob_store_connection`_ are resolved before the attributes from the same section are applied.

instance_id
-----------

default: name of flow (defaults to `name`_ if not provided when generating Flow object)

An ID for identifying a particular pipedag instance. **Optional**

Its purpose is to be used in table_store and blob_store
configurations for ensuring that different pipedag instances don't overwrite each other's tables, schemas, files
or folders. Please note that PipedagConfig.get(per_user=True) will modify instance_id such that it is unique for every
user ID as taken from environment variables.

The instance_id will also be used by the locking manager (i.e. Zookeeper) together with the stage name to ensure that
different runs on the same instance_id will not mess with identically named schemas. The goal is that flows / pipedag
instances can be run from IDE, Continuous Integration, and the Orchestration Engine UI without collisions, automatically
ensuring cache validity the running code commit in the moment of transactionally committing a stage result.

per_user_template
-----------------

default: {id}_{username}

In case a run config is generated with `PipedagConfig.get(per_user=True)`, the user name is injected
into instance_id before it is used for lookups in table_store or blob_store configurations.

With `per_user_template`_ it is possible to control whether username will be used as prefix or suffix.
Therefore, it must include both placeholders `{id}` and `{username}`:

.. code-block:: yaml

    per_user_template: "{username}__{id}"

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

.. code-block:: yaml

    auto_table: ["pandas.DataFrame", "sqlalchemy.Table", "pydiverse.transform.Table",]


auto_blob
---------

The same as `auto_table` just for blobs. **Optional**


fail_fast
---------

default: false

When true, will provide nicer stack traces for debugging but will make it harder to monitor an execution graph where
occasional errors are expected.


strict_result_get_locking
-------------------------

default: true

When true, check that `Result.get()` is only called within `with StageLockContext(...)` statement.
This does not allow a flow to change result outputs before they are fetched. The defautl is a good
choice when (potentially) running tests in parallel. For interactive debugging it might be handy to
disable this check.

table_store
-----------

This section describes the table store to use. **Required**

The `class` key/value is used to define which class to use as a the table store.
Any other key/value pairs in this section are backend specific and either get passed to the classes `__init__` or `_init_conf_` method.

Fields `schema_prefix` and `schema_suffix` are optional. They are particularly useful for use with SQL Server database.
SQL Server can query multiple databases within one query. So the database becomes effectively a part of the schema
(also in the view of sqlalchemy). If `schema_prefix` includes a dot (i.e. ``"flow_db."``), we always prefix a
specific database as part of the schema. If `schema_suffix` includes a dot, we use databases instead of schemas.
``schema_suffix=".dbo"`` is the most common usecase for this. Never put a dot in both `schema_prefix` and `schema_suffix`.

.. code-block:: yaml

    table_store:
        class: "pydiverse.pipedag.backend.table.SQLTableStore"
        url: "postgresql://{username}:{password}@127.0.0.1/{instance_id}"
        url_attrs_file: "~/.pipedag/{name}_{instance_id}.yaml"
        # schema_prefix: "myflow_"
        # schema_suffix: "_flow01"

table_store_connection
^^^^^^^^^^^^^^^^^^^^^^

This is an attribute within `table_store`_ section which allows referencing a block of attributes from
`table_store_connections`_ section:

.. code-block:: yaml

    table_store_connections:
        postgres:
            url: "postgresql://postgres:pipedag@127.0.0.1/{instance_id}"
            schema_prefix: "myflow_"

    table_store:
        class: "pydiverse.pipedag.backend.table.SQLTableStore"
        table_store_connection: postgres

class: pydiverse.pipedag.backend.table.SQLTableStore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Database backend for storing tables and working with tables based on hand-written or programatically created SQL.

url
"""

Sqlalchemy engine URL for referencing a database connection including user name and password. Placeholders like
{name} and {instance_id} may be used. Further placeholders can be defined in a yaml file referenced by `url_attrs_file`_
(i.e. {username}, {password}, {host}, {port}).

Attention: `PipedagConfig.get(per_user=true)` modifies `instance_id`_ before it is used here.

.. code-block:: yaml

        url: "postgresql://{username}:{password}@127.0.0.1/{instance_id}"

The URL may also reference environment variables:

.. code-block:: yaml

        url: "postgresql://defaultuser:{$POSTGRES_PASSWORD}@127.0.0.1/{instance_id}"

Environment variables may include non-environment variable placeholders.

url_attrs_file
""""""""""""""

Filename of a yaml file which is read shortly before rendering the final sqlalchemy engine URL and which is used to
replace custom placeholders in `url`_. The filename itself may include placeholders like {name} and {instance_id}.

Attention: `PipedagConfig.get(per_user=true)` modifies `instance_id`_ before it is used here.

.. code-block:: yaml

        url_attrs_file: "~/.pipedag/{name}_{instance_id}.yaml"

The filename may also reference environment variables:

.. code-block:: yaml

        url_attrs_file: "{$PIPEDAG_PASSWORD_FILE}"

Environment variables may include non-environment variable placeholders.

schema_prefix
"""""""""""""

When accessing tables via a database connection, sqlalchemy offers a `schema=` attribute. This schema is assembled
as `schema_prefix`_ + `stage.name` + `schema_suffix`_. For `dialect=mssql`, sqlalchemy best supports the use of
databases as schemas. In this case one of `schema_prefix`_ or `schema_suffix`_ must include a dot, so that the
resulting schema name looks like `schema="database_<stage_schema>.dbo"`:

Attention: `PipedagConfig.get(per_user=true)` modifies `instance_id`_ before it is used here.

.. code-block:: yaml

        schema_prefix: "{instance_id}_"
        schema_suffix: ".dbo"

schema_suffix
"""""""""""""

See `schema_prefix`_.

class: pydiverse.pipedag.backend.table.DictTableStore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rather used for fast testing. It stores dataframes in a dictionary. Not intended for productive use.

blob_store
----------

This section describes which blob store to use. **Required**

It is structured the same way as the `table_store` section.

.. code-block:: yaml

    blob_store:
        class: "pydiverse.pipedag.backend.blob.FileBlobStore"
        base_path: "/tmp/pipedag/blobs"

blob_store_connection
^^^^^^^^^^^^^^^

This is an attribute within `blob_store`_ section which allows referencing a block of attributes from
`blob_store_connections`_ section:

.. code-block:: yaml

    blob_store_connections:
        tmp:
            base_path: "/tmp/pipedag/blobs"

    table_store:
        class: "pydiverse.pipedag.backend.table.SQLTableStore"
        blob_store_connection: tmp

class: pydiverse.pipedag.backend.blob.FileBlobStore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Store blobs as files on the filesystem (might be mounted network drive)

base_name
"""""""""

The directory under which blobs are stored. Directories are created based on `instance_id`_.

Attention: `PipedagConfig.get(per_user=true)` modifies `instance_id`_ before it is used here.

lock_manager
------------

This section describes the lock manager to use. **Required**

It is structured the same way as the `table_store` section.
If you are the only person working on a project, you can choose not to use a lock manager at all
(*not recommended for production*),
in which case you set `class = "pydiverse.pipedag.backend.lock.NoLockManager"`.

.. code-block:: yaml

    lock_manager:
        class: "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
        hosts: "localhost:2181"

class: FileLockManager
^^^^^^^^^^^^^^^^^^^^^^

Use lock files on the filesystem.

Attention: sometimes mounted network drives have unreliable locking

base_name
"""""""""

The directory under which lock files are stored. Directories are created based on `instance_id`_.

Attention: `PipedagConfig.get(per_user=true)` modifies `instance_id`_ before it is used here.


class: pydiverse.pipedag.backend.lock.ZooKeeperLockManager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

all attributes besides `class` are given as keyword
arguments to https://kazoo.readthedocs.io/en/latest/api/client.html

An excerpt of most needed attributes:

hosts
"""""

Comma separated list of hosts to connect.

keyfile
"""""""

SSL keyfile to use for authentication.

The filename may also reference environment variables and use placeholders like {name} and {instance_id}:

.. code-block:: yaml

        keyfile: "{$ZOOKEEPER_AUTH_DIR}/{instance_id}.yaml"

use_ssl
"""""""

Argument to control whether SSL is used or not (default: false).

class: pydiverse.pipedag.backend.lock.NoLockManager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Disables locking between different runs of the flow.

Attention: This may lead to corruption in databases or files on disk. Especially stage commit transactionality may
be compromised.

orchestration
-------------

This section describes the default orchestration engine that should be used to execute a flow. **Optional**

Once again, this section is structured the same way as the `table_store` section.
If you don't specify this section, you must pass an Engine object to the `flow.run()` method.

.. code-block:: yaml

    orchestration:
        class = "pydiverse.pipedag.engine.PrefectEngine"

Currently supported orchestration engines:

class: pydiverse.pipedag.engine.PrefectEngine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Hands over orchestration of pipedag flow execution to prefect.
It supports both prefect 1.3.x and 2.y depending on which version is installed in the python environment.

Prefect also supports caching features, but we don't use them with pipedag. But we actually like about prefect that it
can also be used as a thin layer for executing pieces of code. It is also important that it has a UI that you can
keep running while adding a project for monitoring runs of a newly created flow.

Version 2.y is a radical change of principles which don't just have positive effects for using it as a pipedag
orchestration engine. For example, the radar view is pretty ill-suited for rather linear flows which is how most data
pipelines look on a higher level.

* For prefect 1.3.x, see: https://docs-v1.prefect.io/
* For prefect 2.y, see: https://docs.prefect.io/

pydiverse.pipedag.engine.SequentialEngine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Simple choice to just execute the pipedag flow. Flow runs are not recorded anywhere and there is not UI for monitoring
them.

example configuration
---------------------

.. code-block:: yaml

    name: pipedag_tests
    strict_instance_lookup: true  # default value: true
    table_store_connections:
      postgres:
        url: "postgresql://{username}:{password}@127.0.0.1:6543/{instance_id}"
        url_attrs_file: "~/.pipedag/{name}_{instance_id}.yaml"

      mssql:
        url: "mssql+pyodbc://{username}:{password}@127.0.0.1:1433/master?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
        url_attrs_file: "~/.pipedag/mssql.yaml"
        schema_prefix: "{instance_id}_"  # SQL Server needs database.schema (uncomment only on of prefix and suffix)
        schema_suffix: ".dbo"   # Alternatively SQL Server databases can be used as schemas with .dbo default schema

    blob_store_connections:
      file:
        base_path: "/tmp/pipedag/blobs"

    technical_setups:
      default:
        network_interface: "127.0.0.1"
        auto_table: ["pandas.DataFrame", "sqlalchemy.Table"]
        fail_fast: true

        instance_id: pipedag_default
        table_store:
          class: "pydiverse.pipedag.backend.table.SQLTableStore"

          table_store_connection: postgres
          create_database_if_not_exists: True

          print_materialize: true
          print_sql: true

        blob_store:
          class: "pydiverse.pipedag.backend.blob.FileBlobStore"
          blob_store_connection: file

        lock_manager:
          class: "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
          hosts: "localhost:2181"

        orchestration:
          class: "pydiverse.pipedag.engine.SequentialEngine"
          ## Activate this class to work either with prefect 1.3.0 or prefect 2.0
          # class: "pydiverse.pipedag.engine.PrefectEngine"

    instances:
      __any__:
        technical_setup: default
        # The following Attributes are handed over to the flow implementation (pipedag does not care)
        attrs:
          # by default we load source data and not a sampled version of a loaded database
          copy_filtered_input: false

      full:
        # pipedag instance for full dataset scheduled by CI
        instance_id: pipedag_full
        # Run this instance under @pytest.mark.slow5 (pydiverse.pipetest will read tags from here)
        tags: pytest_mark_slow5

      midi:
        # pipedag instance for medium size input with some code coverage
        instance_id: pipedag_midi
        attrs:
          # copy filtered input from full instance
          copy_filtered_input: true
          copy_source: full
          copy_per_user: false
          sample_cnt: 2  # this is just dummy input where we sample 2 rows

        # Run this instance under @pytest.mark.slow4 (pydiverse.pipetest will read tags from here)
        tags: pytest_mark_slow4
        # Run only stage_2 under @pytest.mark.slow3 (pydiverse.pipetest will read stage_tags from here)
        stage_tags:
          pytest_mark_slow3:
            - simple_flow_stage2

      mini:
        # pipedag instance for tiny input just for smoke test development
        instance_id: pipedag_mini
        attrs:
          copy_filtered_input: true
          copy_source: full
          copy_per_user: false
          sample_cnt: 1  # this is just dummy input where we sample 1 row

        # Run this instance under @pytest.mark.slow2
        tags: pytest_mark_slow2
        # Run only stage_2 under @pytest.mark.slow1
        stage_tags:
          pytest_mark_slow1:
            - simple_flow_stage2

      mssql:
        # Full dataset is using default database connection and schemas
        table_store:
          <<: *db_mssql

    flows:
    #  __any__:
    #    instances:
    #      # it would be equivalent to move everything in "instances:" to here
      test_instance_selection:
        instances:
          full:
            table_store:
              schema_suffix: "_full"
        table_store:
          schema_prefix: "instance_selection_"

example configuration with anchor syntax
----------------------------------------

Keys beginning with underscore don't have any specific meaning. They are just used for defining an anchor section
which then can be later referenced
(see https://www.howtogeek.com/devops/how-to-simplify-docker-compose-files-with-yaml-anchors-and-extensions/).

.. code-block:: yaml

    name: pipedag_tests
    strict_instance_lookup: true  # default value: true
    _table_store_connections:
      postgres: &db_postgres
        url: "postgresql://{username}:{password}@127.0.0.1:6543/{instance_id}"
        url_attrs_file: "~/.pipedag/{name}_{instance_id}.yaml"

      mssql: &db_mssql
        url: "mssql+pyodbc://{username}:{password}@127.0.0.1:1433/master?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
        url_attrs_file: "~/.pipedag/mssql.yaml"
        schema_prefix: "{instance_id}_"  # SQL Server needs database.schema (uncomment only on of prefix and suffix)
        schema_suffix: ".dbo"   # Alternatively SQL Server databases can be used as schemas with .dbo default schema

    _blob_store_connections:
      file: &blob_file
        base_path: "/tmp/pipedag/blobs"

    _technical_setups:
      default: &technical_setup_default
        network_interface: "127.0.0.1"
        auto_table: ["pandas.DataFrame", "sqlalchemy.Table"]
        fail_fast: true

        instance_id: pipedag_default
        table_store:
          # Postgres:
          <<: *db_postgres
          create_database_if_not_exists: True

          class: "pydiverse.pipedag.backend.table.SQLTableStore"

          print_materialize: true
          print_sql: true

        blob_store:
          class: "pydiverse.pipedag.backend.blob.FileBlobStore"
          <<: *blob_file

        lock_manager:
          class: "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
          hosts: "localhost:2181"

        orchestration:
          class: "pydiverse.pipedag.engine.SequentialEngine"

    _instances: &instances
      __any__:
        <<: *technical_setup_default
        # The following Attributes are handed over to the flow implementation (pipedag does not care)
        attrs:
          # by default we load source data and not a sampled version of a loaded database
          copy_filtered_input: false

      full:
        # pipedag instance for full dataset scheduled by CI
        instance_id: pipedag_full
        # Run this instance under @pytest.mark.slow5 (pydiverse.pipetest will read tags from here)
        tags: pytest_mark_slow5

      midi:
        # pipedag instance for medium size input with some code coverage
        instance_id: pipedag_midi
        attrs:
          # copy filtered input from full instance
          copy_filtered_input: true
          copy_source: full
          copy_per_user: false
          sample_cnt: 2  # this is just dummy input where we sample 2 rows from each table

        # Run this instance under @pytest.mark.slow4 (pydiverse.pipetest will read tags from here)
        tags: pytest_mark_slow4
        # Run only stage_2 under @pytest.mark.slow3 (pydiverse.pipetest will read stage_tags from here)
        stage_tags:
          pytest_mark_slow3:
            - simple_flow_stage2

      mini:
        # pipedag instance for tiny input just for smoke test development
        instance_id: pipedag_mini
        attrs:
          copy_filtered_input: true
          copy_source: full
          copy_per_user: false
          sample_cnt: 1  # this is just dummy input where we sample 1 row from each table

        # Run this instance under @pytest.mark.slow2
        tags: pytest_mark_slow2
        # Run only stage_2 under @pytest.mark.slow1
        stage_tags:
          pytest_mark_slow1:
            - simple_flow_stage2

      mssql:
        table_store:
          <<: *db_mssql

    flows:
      __any__:
        instances: *instances
      test_instance_selection:
        instances:
          full:
            table_store:
              schema_suffix: "_full"
        table_store:
          schema_prefix: "instance_selection_"

example code for loading configuration
--------------------------------------

.. code-block:: python

    flow = create_flow1()
    flow.run()  # will internally run cfg=PipedagConfig.load().get(flow_name=flow.name)

    cfg=PipedagConfig.load().get() # will load instance=__any__, flow_name=cfg.get_pipedag_name()
    flow = create_flow2(cfg.flow_name, cfg.attrs)
    flow.run(cfg)

    with PipedagConfig.load().get(): # will load instance=__any__, flow_name=cfg.get_pipedag_name()
      flow = create_flow3()  # can get ConfigContext.get().get_pipedag_name() or ConfigContext.get().attrs
      flow.run()  # will work with cfg=ConfigContext.get()

    cfg=PipedagConfig.load().get(flow_name="foo") # will load instance=__any__
    flow = create_flow4(cfg.flow_name, cfg.attrs)
    flow.run(cfg)