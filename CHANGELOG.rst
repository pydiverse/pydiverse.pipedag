.. Versioning follows semantic versioning, see also
https://semver.org/spec/v2.0.0.html. The most important bits are:
* Update the major if you break the public API and major > 0
* Update the minor if you add new functionality
* Update the patch if you fixed a bug

Changelog
=========

0.5.0 (2023-06-28)
------------------
- add support for DuckDB
- add support for pyarrow backed pandas dataframes
- support execution of subflow
- store final state of task in flow result object
- tasks now have a `position_hash` associated with them to identify them purely based on their position (e.g. stage, name and input wiring) inside a flow.
- breaking change to metadata: added position_hash to `tasks` metadata table and change type of hash columns from String(32) to String(20).
- `Flow`, `Subflow`, and `Result` objects now provide additional options for visualizing them
- added `unlogged_tables` flag to SQLTableStore for creating UNLOGGED tables with Postgres.
- created `pipedag-manage` command line utility with `clear-metadata` command to help with migrating between different pipedag metadata versions.

0.4.1 (2023-06-17)
------------------
- implement DaskEngine: orchestration engine for running multiple tasks in parallel
- implement DatabaseLockManager: lock manager based on locking mechanism provided by database

0.4.0 (2023-06-08)
------------------
- update public interface
- encrypt IPC communication
- remove preemptive os.makedirs from ParquetTableCache
- improve logging and provide structlog utilities

0.3.0 (2023-05-25)
------------------
- breaking change to pipedag.yaml: introduced `args` subsections for arguments
  that are passed to backend classes
- fix ibm_db_sa bug when copying dataframes from cache: uppercase table names by default
- nicer readable SQL queries: use automatic aliases for inputs of SQLAlchemy tasks
- implement option ignore_task_version: disable eager task caching for some instances to
    reduce overhead from task version bumping
- implement local table cache: store input/output of dataframe tasks in parquet files
    and allow using it as cache to avoid rereading from database

0.2.4 (2023-05-05)
------------------
- fix errors by increasing output_json length in metadata table
- fix cache invalidation: query normalization before checking for changes
- add rudimentary support for ibis tasks (postgres + mssql)
- add rudimentary support for polars + tidypolars tasks
- implemented pandas type mapping to avoid row wise type checks of object columns
- support pandas 2.0 (no arrow features used that)
- support sqlalchemy 2.0 (except for with polars)

0.2.3 (2023-04-17)
------------------
- fixed python 3.9 compatibility (traceback.format_exception syntax changed)
- fixed deferred table copy when task is invalid (introduced with 0.2.2)
- fixed mssql to not reflect full schema while renamings happen
- fixed clearing of metadata tables for lazy tables and raw sql tables
- fixed mssql synonym resolution when reading input table for pandas task
- initial implementation of issue #62: make query canonical before hashing
- retry some DB calls in case they are aborted as deadlock victim

0.2.2 (2023-03-31)
------------------
- added option avoid_drop_create_schema to table store configuration
- improve performance when working with IBM DB2 dialect (i.e. table locking)
- prevent table copying and schema swapping for 100% cache valid stages

0.2.1 (2023-01-15)
------------------
- removed contextvars dependency (not needed for python >= 3.7 and broke conda-forge build)

0.2.0 (2023-01-14)
------------------
- SQLTableStore: support for Microsoft SQL Server and IBM DB2 (Linux) database connection strings
- Support primary keys and indexes (can be configured with Table object and used in custom RawSql code)
- RawSql: support additional return type for @materialize tasks which allows to emit raw SQL string including multiple
   create statements (currently, views/functions/procedures are only supported for dialect mssql). This feature should
   only be used for getting legacy code running in pipedag before converting it to programatically generated or manual
   SELECT statements.
- Support pytsql library for executing raw SQL scripts with dialect=mssql (i.e. supports PRINT capture)
- Manual Cache Invalidation for source nodes: @materialize(cache=f) parameter can take an arbitrary function that gets
   the same arguments as the task function and returns a hash. If the hash is different than for the previous run, the
   task is considered cache invalid.
- New configuration file format pipedag.yaml can be used to configure multiple pipedag instances:
   see docs/reference/config.rst

0.1.0 (2022-09-01)
------------------
Initial release.
- @materialize annotations
- flow definition with nestable stages
- zookeeper synchronization
- postgres database backend
- Prefect 1.x and 2.x support
- multi-processing/multi-node support for state exchange between @materialize tasks
- support materialization for: pandas, sqlalchemy, raw sql text, pydiverse.transform
