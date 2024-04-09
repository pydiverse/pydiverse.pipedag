# Pipedag Config File

```{note}
technical_setups is missing. Do we actually need this?
```

The `pipedag.yaml` file is used to configure pipedag.
When executing a flow, pipedag searches for the config file in the following directories:

- The current working directory
- Any parent directories of the working directory
- The user folder

Alternatively you can also specify a config file path using the {envvar}`PIPEDAG_CONFIG` environment variable.

```{attention}
Pipedag config files may be used for code execution under a different user ID.

Pipedag may be instructed to read yaml files with key-value pairs and send content to arbitrary
hosts. It can also be instructed to send content of environment variables (i.e. security tokens).
Furthermore it can be instructed to load python classes in the python environment and the constructor
be fed with arbitrary keyword arguments.
```

## Specifying instances and flows

One of the main goals of pipedag is to make it easy to keep multiple instances of a data processing DAG up and running at the same time and maximize development efficiency. 
Thus, it is important to allow running the same flow with full input data, a sampled version, and a bare minimum data set for quick smoke tests of newly written code.
We would call setups for those three input data sets 'pipedag instances' and they could get names like `full`, `midi` or `mini`.

There is no hard binding between a flow and a pipedag configuration file. 
For each flow, the pipedag configuration file offers configuration options for multiple pipedag instances. 

There are three main places in the config file where you can configure individual instances and flows:

```yaml
instances:
  my_instance:
    # Configuration for instance `my_instance`
    <parameter>: <value>

flows:
  my_flow:
    # Configuration for flow `my_flow`
    <parameter>: <value>
    
    instances:
      my_flow:
        # Configuration for the combination of 
        # flow `my_flow` with instance `my_instance`
        <parameter>: <value>
```

In this case, if you were to run the flow `my_flow` on the instance `my_instance`, 
we combine all appropriate config parameters in the `instances`, `flows` and `flows > instances` sections,
where values found in `flows > instances` replace values found in `flows`,
and values found in `flows` replace values found in the `instances` section.

Additionally, there are is a special `__any__` instance and `__any__` flow which provide the base attributes which are inherited by all other named instances and flows respectively.

## Simple Example Config

```yaml
name: pipedag_tests
instances:
  __any__:
    instance_id: pipedag_default
    auto_table: 
      - pandas.DataFrame
      - sqlalchemy.sql.expression.TextClause
      - sqlalchemy.sql.expression.Selectable 
    
    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"
      args:
        url: "postgresql://user:password@127.0.0.1:5432/{instance_id}"
        create_database_if_not_exists: true
    
    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      args:
        base_path: "/tmp/pipedag/blobs"

    lock_manager:
      class: "pydiverse.pipedag.backend.lock.DatabaseLockManager"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"
```


* * *


## Specification

### Top Level Attributes

(config-name)=
name
: The name of the pipedag configuration. 
  It is also used as the default name for a flow connected with this configuration.

strict_instance_lookup
: If set to true, looking up an instance that was not explicitly defined in the config will fail.
  
  (default `True`)


### Instance / Flow level attributes

Instances of flow level attributes can be placed in the following places, and will be overwritten in the following order (more specific values replace less specific ones):

```yaml
instances:
  __any__:
    attribute: value

instances:
  <xxx>:
    attribute: value

flows:
  __any__:
    attribute: value

flows:
  __any__:
    instances:
      <xxx>:
        attribute: value

flows:
  <yyy>:
    attribute: value
    
flows:
  <yyy>:
    instances:
      <xxx>:
        attribute: value
```

(instance_id)=
instance_id 
: An ID for identifying a particular pipedag instance. *Optional*

  Its purpose is to be used in [`table_store`](#section-table_store) and [`blob_store`](#section-blob_store) configurations 
  for ensuring that different pipedag instances don't overwrite each other's tables, schemas, files or folders.
  Please note that `PipedagConfig.get(per_user=True)` will modify instance_id such that it is unique for every user ID as taken from environment variables.
    
  The `instance_id` will also be used by the locking manager together with the stage name 
  to ensure that different runs on the same instance_id will not mess with identically named schemas. 
  The goal is that flows / pipedag instances can be run from IDE, Continuous Integration, and the Orchestration Engine UI without collisions, 
  automatically ensuring cache validity the running code commit in the moment of transactionally committing a stage result.
     
  (default: name of flow)

stage_commit_technique
: We want to prepare the whole output of a `Stage` before we make it visible to an explorative user looking in the table_store / database.
There should never be a time when he sees a mix of new and old tables of that schema and the switch (stage commit) should happen in an instance. 
We don't use database transactionality features because of expected slowdowns, and we do want to look at partial output for debugging.

  In order to commit stages, we currently offer the following techniques:

  schema_swap
  : We prepare output in a `<stage>__tmp` schema and then swap schemas for `<stage>` and `<stage>__tmp` with three rename operations.
    
  read_views
  : We use two schemas, `<stage>__odd` and `<stage>__even`, and fill schema `<stage>` just with views to one of those schemas.
    
  ```{list-table} Support for different commit techniques
  :widths: 50 25 25
  :header-rows: 1
    
  *   - Database
      - `schema_swap`
      - `read_views`
  *   - PostgreSQL
      - yes
      - yes
  *   - DuckDB
      - no
      - yes
  *   - Microsoft SQL Server
      - yes
      - yes
  *   - IBM Db2
      - no
      - yes
  ```

  (default: `schema_swap`)

(per_user_template)=
per_user_template
: In case the config is generated with `PipedagConfig.get(per_user=True)`{l=python}, 
  the current user's name gets injected into [`instance_id`](#instance_id).
  This new per-user instance_id is them used wherever the old instance_id was used.

  To customize how this per-user instance_id is constructed, you can provide the `per_user_templace` argument, 
  which must include the template placeholders `{username}` and `{id}`. 
    
  ```yaml
  per_user_templace: "{username}_{id}"
  ```

  (default: `{id}_{username}`)
  
network_interface
: The network interface to use for communicating with the parent process. *Optional*

  This should be the IP address of the computer on which the flow is being executed.
  Unless you are running the flow in a distributed manner across multiple computer, you can leave this value blank.

  To specify a IPv6 address, you must surround it in square brackets.

  (default: `127.0.0.1`)

disable_kroki
: If set to `True`, [Kroki](https://kroki.io) URL will not be displayed at end of executing flow. Kroki URLs are a really nice way 
of visualizing the flow execution without the need to install graphviz and to worry about how to display the result. 
The graph is sent to the kroki_url only once you click the link. The whole graph is encoded in the URL. However, since
there is the risk to expose sensitive information by sending stage and task names to a public server, it is disabled by default.
You can also [self-host](https://docs.kroki.io/kroki/setup/install/) a kroki service and set the kroki_url to your own 
service.

  (default: `True`)

(kroki_url)=
kroki_url
: A url that points to a [Kroki](https://kroki.io) instance. *Optional*

  Pipedag uses a free service called Kroki to visualize flows (see [](Flow.visualize_url) and [](Result.visualize_url)).
  If you want to self-host a Kroki instance (for example, if your flow contains sensitive information), you can specify a custom url that pipedag should use.

  (default: `https://kroki.io`)

(auto_table)=
auto_table
: A list of tables classes.
  If a materializing task returns an instance of any class in this list, it automatically gets materialized to the table store. *Optional*
    
  For example, if you automatically want to store all pandas dataframes, pydiverse transform tables and sql alchemy queries in the table store, you would specify it like this:
  ```yaml
  auto_table: 
    - pandas.DataFrame
    - pydiverse.transform.Table
    - sqlalchemy.sql.expression.TextClause
    - sqlalchemy.sql.expression.Selectable
  ```

(auto_blob)=
auto_blob
: Same as [`auto_table`](#auto_table) just for [blobs](project:#pydiverse.pipedag.Blob). *Optional*

fail_fast
: When set to `True`, and an exception occurs during execution of a flow, the flow will abort execution and reraise the exception.

  (default: `False`)

(strict_result_get_locking)= 
strict_result_get_locking
: When set to `True`, check that [`Result.get()`](#pydiverse.pipedag.Result.get) is only called within a [`with StageLockContext(...)`](#StageLockContext) statement.
  
  This prevents a different flow from overwriting the results before they get fetched. 
  The default is a good choice when (potentially) running tests in parallel.
  For interactive debugging it might be handy to disable this check.

  (default: `True`)

cache_validation
: See [](#section-cache_validation). *Optional*
  
table_store
: See [](#section-table_store). *Required*

blob_store
: See [](#section-blob_store). *Required*

lock_manager
: See [](#section-lock_manager). *Required*

orchestration
: See [](#section-orchestration). *Required*

(config-attrs)=
attrs
: A place to put an arbitrary, user specific yaml mapping.
  During flow execution you will be able to access these `attrs` using [`ConfigContext.get().attrs`](#ConfigContext.attrs)

(section-cache_validation)=
### Cache Validation options

mode
: Choose a mode of cache invalidation. 

  Supported values:
  - `normal`: Normal cache invalidation.
  - `assert_no_fresh_input`: Same as `ignore_fresh_input` and additionally fail if tasks having
      a cache function would still be executed (change in version or lazy query).
  - `ignore_fresh_input`: Ignore the output of cache functions that help determine the availability of fresh input.
        With `disable_cache_function=False`, it still calls cache functions, so cache invalidation works interchangeably 
        between `ignore_fresh_input` and `normal`.
  - `force_fresh_input`: Consider all cache function outputs as different and thus make source tasks cache invalid.
  - `force_caches_invalid`: Disable caching and thus force all tasks as cache invalid.
        This option implies `force_fresh_input`.

  (default: `normal`) 

disable_cache_function
: When set to `True`, cache functions are not called. This is not compatible with `mode=normal`. The difference to 
`ignore_fresh_input` is that in case mode is set back to `normal`, the cache becomes invalid if disable_cache_function 
was set to `True` during last run.

  (default: `False`)

ignore_task_version
: When set to `True`, tasks that specify an explicit version for cache invalidation will always be considered cache invalid. 
  This might be useful for instances with short execution time during rapid development cycles when manually bumping version numbers becomes cumbersome.

  (default: `False`)




(section-table_store)=
### Table Store Config

This section (labeled `table_store`) specifies the table store to use.

(table_store_connection)=
table_store_connection
: This attribute allows referencing a block of attributes from the top level `table_store_connections` section.

  As an example, this config

  ```yaml
  table_store_connections:
    postgres:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"
      args:
        url: "postgresql://postgres:pipedag@127.0.0.1/{instance_id}"
  
  instances:
    __any__:
      table_store:
        table_store_connection: postgres
        args:
          schema_prefix: "foo"
  ```

  is, after parsing, equivalent to the following config:

  ```yaml
  instances:
    __any__:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"
      args:
        url: "postgresql://postgres:pipedag@127.0.0.1/{instance_id}"
        schema_prefix: "foo"
  ```

class
: The [fully qualified name](<inv:python#qualified name>) of the class to be used as the table store.
  
  Available classes:
  - [](#pydiverse.pipedag.backend.table.SQLTableStore)

args
: Any values in this subsection will be passed as arguments to the `__init__` or, if available, the `_init_conf_` method of the table store class.
  For a list of available options, look at the `__init__` method of the table store you are using.

hook_args
: This subsection allows passing custom config arguments to the different table hooks to influence how tables get materialized and retrieved.
  The builtin hooks respect the following options:

  pandas
  : dtype_backend
    : The default [dtype backend](https://pandas.pydata.org/docs/reference/arrays.html) to use.
    
    : Supported values:
      - `numpy`: Use pandas' nullable extension dtypes for numpy.
      - `arrow`: Use pyarrow backed dataframes.


local_table_cache
: See [](#section-local_table_cache). *Optional*


(section-local_table_cache)=
#### Local Table Cache

The section (labeled `local_table_cache`) inside the [`table_store`](#section-table_store) section specifies a cache for storing tables locally.
Such a local table cache may help speed up local development significantly as tables don't need to be retrieved from the (potentially slower) table store.

class
: The [fully qualified name](<inv:python#qualified name>) of the class to be used as the local table cache.

  Available classes:
  - [](#pydiverse.pipedag.backend.table.cache.ParquetTableCache)

attrs
: Any values in this subsection will be passed as arguments to the `__init__` or, if available, the `_init_conf_` method of the local table cache class.
  For a list of available options, look at the `__init__` method of the table cache you are using.

store_input
: If true, input dataframes are cached after reading from the table store. 
  This can significantly speed up the retrieval.

  (default: `True`)

store_output
: If true, output dataframes are stored before writing to table store. 
  This is mainly useful for inspecting / using the cache during debugging.

  (default: `False`)

use_stored_input_as_cache
: If true, input dataframes are read from cache instead of table store if cache is valid. 

  (default: `True`)



(section-blob_store)=
### Blob Store Config

This section (labeled `blob_store`) specifies the blob store to use.
It is structured very similarly to `table_store` and provides the following options:

blob_store_connection
: This attribute allows referencing a block of attributes from the top level `blob_store_connections` section.
  For more detail, refer to the documentation of [`table_store_connection`](#table_store_connection).

class
: The [fully qualified name](<inv:python#qualified name>) of the class to be used as the blob store.
  
  Available classes:
  - [](#pydiverse.pipedag.backend.blob.FileBlobStore)

args
: Any values in this subsection will be passed as arguments to the `__init__` or, if available, the `_init_conf_` method of the blob store class.
  For a list of available options, look at the `__init__` method of the blob store you are using.


(section-lock_manager)=
### Lock Manager Config

This section (labeled `lock_manager`) specifies the lock manager to use.

class
: The [fully qualified name](<inv:python#qualified name>) of the class to be used as the lock manager.

  Available classes:
  - [](#pydiverse.pipedag.backend.lock.DatabaseLockManager)
  - [](#pydiverse.pipedag.backend.lock.ZooKeeperLockManager)
  - [](#pydiverse.pipedag.backend.lock.FileLockManager)
  - [](#pydiverse.pipedag.backend.lock.NoLockManager)

args
: Any values in this subsection will be passed as arguments to the `__init__` or, if available, the `_init_conf_` method of the lock manager class.
  For a list of available options, look at the `__init__` method of the lock manager you are using.



(section-orchestration)=
### Orchestration Engine Config

This section (labeled `orchestration`) specifies which orchestration engine should be used for executing a flow.

It is structured very similarly to `table_store` and provides the following options:

class
: The [fully qualified name](<inv:python#qualified name>) of the class to be used as the orchestration engine.

  Available classes:
  - [](#pydiverse.pipedag.engine.SequentialEngine)
  - [](#pydiverse.pipedag.engine.DaskEngine)
  - [](#pydiverse.pipedag.engine.prefect.PrefectEngine)
    
args
: Any values in this subsection will be passed as arguments to the `__init__` or, if available, the `_init_conf_` method of the orchestration engine class.
  For a list of available options, look at the `__init__` method of the orchestration engine you are using.


* * *


## Complex Example Config

```yaml
name: pipedag_tests
strict_instance_lookup: true  # default value: true
table_store_connections:
  postgres:
    args:
      # Postgres: this can be used after running `docker-compose up`
      url: "postgresql://{$POSTGRES_USERNAME}:{$POSTGRES_PASSWORD}@127.0.0.1:6543/{instance_id}"

  postgres2:
    args:
      url: "postgresql://{username}:{password}@{host}:{port}/{instance_id}"
      url_attrs_file: "{$POSTGRES_PASSWORD_CFG}"

  mssql:
    args:
      # SQL Server: this can be used after running `docker-compose up`
      url: "mssql+pyodbc://{$MSSQL_USERNAME}:{$MSSQL_PASSWORD}@127.0.0.1:1433/master?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
      # schema_prefix: "master."  # SQL Server needs database.schema (exactly one of prefix and suffix must include a dot)
      schema_prefix: "{instance_id}_"  # SQL Server needs database.schema
      schema_suffix: ".dbo"   # Alternatively SQL Server databases can be used as schemas with .dbo default schema

blob_store_connections:
  file:
    args:
      base_path: "/tmp/pipedag/blobs"

technical_setups:
  default:
    # listen-interface for pipedag context server which synchronizes some task state during DAG execution
    network_interface: "127.0.0.1"
    # classes to be materialized to table store even without pipedag Table wrapper (we have loose coupling between
    # pipedag and pydiverse.transform, so consider adding 'pydiverse.transform.Table' in your config)
    auto_table: ["pandas.DataFrame", "sqlalchemy.sql.expression.TextClause", "sqlalchemy.sql.expression.Selectable"]
    # abort as fast a possible on task failure and print most readable stack trace
    fail_fast: true

    # Attention: For disable_kroki: false, stage and task names might be sent to the kroki_url.
    #   You can self-host kroki if you like:
    #   https://docs.kroki.io/kroki/setup/install/
    disable_kroki: true
    kroki_url: "https://kroki.io"

    instance_id: pipedag_default
    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"

      # Postgres: this can be used after running `docker-compose up`
      table_store_connection: postgres

      ## SQL Server: this can be used after running `docker-compose up`
      #table_store_connection: mssql

      args:
        create_database_if_not_exists: True
        # print select statements before being encapsualted in materialize expressions and tables before writing to
        # database
        print_materialize: true
        # print final sql statements
        print_sql: true

    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      blob_store_connection: file

    lock_manager:
      class: "pydiverse.pipedag.backend.lock.DatabaseLockManager"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"
      ## Activate this class to work either with prefect 1.x or prefect 2.y
      # class: "pydiverse.pipedag.engine.prefect.PrefectEngine"

instances:
  __any__:
    technical_setup: default
    # The following Attributes are handed over to the flow implementation (pipedag does not care)
    attrs:
      # by default we load source data and not a sampled version of a loaded database
      copy_filtered_input: false

  full:
    # Full dataset is using default database connection and schemas
    instance_id: pipedag_full
    table_store:
      table_store_connection: postgres2
    # Run this instance under @pytest.mark.slow5
    tags: pytest_mark_slow5

  midi:
    # Full dataset is using default database connection and schemas
    instance_id: pipedag_midi
    attrs:
      # copy filtered input from full instance
      copy_filtered_input: true
      copy_source: full
      copy_per_user: false
      copy_filter_cnt: 2  # this is just dummy input where we sample 2 rows

    # Run this instance under @pytest.mark.slow4
    tags: pytest_mark_slow4
    # Run only stage_2 under @pytest.mark.slow3
    stage_tags:
      pytest_mark_slow3:
        - simple_flow_stage2

  mini:
    # Full dataset is using default database connection and schemas
    instance_id: pipedag_mini
    attrs:
      copy_filtered_input: true
      copy_source: full
      copy_per_user: false
      copy_filter_cnt: 1  # this is just dummy input where we sample 1 row

    # Run this instance under @pytest.mark.slow2
    tags: pytest_mark_slow2
    # Run only stage_2 under @pytest.mark.slow1
    stage_tags:
      pytest_mark_slow1:
        - simple_flow_stage2

  mssql:
    # Full dataset is using default database connection and schemas
    table_store:
      table_store_connection: mssql

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
```