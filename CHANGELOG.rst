.. Versioning follows semantic versioning, see also
https://semver.org/spec/v2.0.0.html. The most important bits are:
* Update the major if you break the public API and major > 0
* Update the minor if you add new functionality
* Update the patch if you fixed a bug

Changelog
=========

0.2.4 (YYYY-MM-DD)
------------------
- fix errors by increasing output_json length in metadata table
- fix cache invalidation: query normalization before checking for changes
- implemented pandas type mapping to avoid rowwise type checks of object columns
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