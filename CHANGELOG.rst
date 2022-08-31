.. Versioning follows semantic versioning, see also
   https://semver.org/spec/v2.0.0.html. The most important bits are:
   * Update the major if you break the public API
   * Update the minor if you add new functionality
   * Update the patch if you fixed a bug

Changelog
=========

0.1.1 (2022-MM-DD)
------------------

- SQLTableStore: support for Microsoft SQL Server database connection strings

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