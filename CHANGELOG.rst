.. Versioning follows semantic versioning, see also
https://semver.org/spec/v2.0.0.html. The most important bits are:
* Update the major if you break the public API
* Update the minor if you add new functionality
* Update the patch if you fixed a bug

Changelog
=========

0.2.0 (2022-MM-DD)
------------------

- SQLTableStore: support for Microsoft SQL Server database connection strings
- RawSql: support additional return type for @materialize tasks which allows to emit raw SQL string including multiple
   create statements (currently, views/functions/procedures are only supported for dialect mssql). This feature should
   only be used for getting legacy code running in pipedag before converting it to programatically generated or manual
   SELECT statements.
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