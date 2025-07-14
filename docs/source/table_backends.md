from pydiverse.pipedag.backend.table.base import CanResult

# Table Backends

We currently only support one table backend battle tested:

- [](#pydiverse.pipedag.backend.table.SQLTableStore)

## [](#pydiverse.pipedag.backend.table.SQLTableStore)

This backend is highly flexible in terms of database dialects and task implementation styles for which it can
materialize/dematerialize tables. Internally, this is abstracted as Hooks like:

```python
@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
```

Which need to implement the following functions:

```python
def can_materialize(cls, type_) -> CanResult:
def can_retrieve(cls, type_) -> bool:
def materialize(cls, store: SQLTableStore, table: Table, stage_name):
def retrieve(cls, store, table, stage_name, as_type: type, limit: int | None = None):
def lazy_query_str(cls, store, obj) -> str:
```

The SQLTableStore currently supports the following SQL databases/dialects:

- Postgres
- Snowflake
- Microsoft SQL Server/TSQL
- IBM DB2 (LUW)
- DuckDB (rather used for testing so far)
- Every dialect unknown to pipedag will be treated like a postgres database (issues are likely)

Example connection strings:
- Postgres: `postgresql://user:password@localhost:5432/{instance_id}`
- Snowflake: `snowflake://{$SNOWFLAKE_USER}:{$SNOWFLAKE_PASSWORD}@{$SNOWFLAKE_ACCOUNT}/database_name/DBO?warehouse=warehouse_name&role=access_role`
- Microsoft SQL Server: `mssql+pyodbc://user:password@127.0.0.1:1433/{instance_id}?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no`
- IBM DB2: `db2+ibm_db://db2inst1:password@localhost:50000/testdb`, `schema_prefix: "{instance_id}_"`
- DuckDB: `duckdb:////tmp/pipedag/{instance_id}/db.duckdb`

For Postgres, it is recommended to install psycopg2 and adbc-driver-postgresql. When using pip, it may be easier to
install psycopg2-binary as opposed to psycopg2.

For Microsoft SQL Server, it is recommended to install unixodbc, arrow-odbc and mssqlkit (proprietary) or bcpandas.
See [below](#section-mssql_odbc) for how to install mssql odbc driver.

See [Database Testing](database_testing.md) for an example how to spin up a database for testing.

SQLTableStore supports the following `input_type` arguments to the {py:func}`@materialize <pydiverse.pipedag.materialize>`
decorator out-of-the-box:

- `sqlalchemy.Table` (see [https://www.sqlalchemy.org/](https://www.sqlalchemy.org/); recommended with `lazy=True`;
  can also be used for composing handwritten SQL strings)
- `pydiverse.transform.eager.PandasTableImpl` (see
  [https://pydiversetransform.readthedocs.io/en/latest/](https://pydiversetransform.readthedocs.io/en/latest/);
  recommended with manual version bumping and `version="X.Y.Z"`)
- `pydiverse.transform.lazy.SQLTableImpl` (
  see [https://pydiversetransform.readthedocs.io/en/latest/](https://pydiversetransform.readthedocs.io/en/latest/);
  recommended with `lazy=True`)
- `ibis.Table` (see [https://ibis-project.org/](https://ibis-project.org/); recommended with `lazy=True`)
- `tidypolars.Tibble` (see [https://github.com/markfairbanks/tidypolars](https://github.com/markfairbanks/tidypolars);
  recommended with `lazy=True`)
- `pandas.DataFrame` (see [https://pandas.pydata.org/](https://pandas.pydata.org/); recommended with manual version
  bumping and `version="X.Y.Z"`)
- `polars.DataFrame` (see [https://pola.rs/](https://pola.rs/); recommended with manual version bumping
  and `version="X.Y.Z"`)
- `polars.LazyFrame` (see [https://pola.rs/](https://pola.rs/); recommended with `version=AUTO_VERSION`)

(section-mssql_odbc)=
### Installing mssql odbc driver for macOS and Linux

Install via Microsoft's
instructions for [Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
or [macOS](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos).

In one Linux installation case, `odbcinst -j` revealed that it installed the configuration in `/etc/unixODBC/*`.
But conda installed pyodbc brings its own `odbcinst` executable and that shows odbc config files are expected in
`/etc/*`. Symlinks were enough to fix the problem. Try `pixi run python -c 'import pyodbc;print(pyodbc.drivers())'`
and see whether you get more than an empty list.

Same happened for MacOS. The driver was installed in `/opt/homebrew/etc/odbcinst.ini` but pyodbc expected it in
`/etc/odbcinst.ini`. This can also be solved by `sudo ln -s /opt/homebrew/etc/odbcinst.ini /etc/odbcinst.ini`.

Furthermore, make sure you use 127.0.0.1 instead of localhost. It seems that /etc/hosts is ignored.
