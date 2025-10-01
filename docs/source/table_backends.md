from pydiverse.pipedag.backend.table.base import CanResult

# Table Backends

We currently support two table backends:

- [](#pydiverse.pipedag.backend.table.SQLTableStore)
- [](#pydiverse.pipedag.backend.table.ParquetTableStore)

Support for in-memory storage based on [Apache Arrow](https://arrow.apache.org/) is planned.

## [](#pydiverse.pipedag.backend.table.SQLTableStore)

This backend is highly flexible in terms of database dialects and task implementation styles for which it can
materialize/dematerialize tables. Internally, this is abstracted as Hooks like:

```python
@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
```

Which need to implement the following functions:

```python
def can_materialize(cls, type_) -> CanMatResult:
def can_retrieve(cls, type_) -> CanRetResult:
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
  can also be used for composing handwritten SQL strings; please note that tables are given
  as sqlalchemy.Alias to the task in order to produce more concise SQL queries -- the only
  downside is that getting schema for `tbl: sa.Alias` requires `tbl.original.schema` instead
  of `tbl.schema`)
- `pydiverse.transform.SqlAlchemy` (
  see [https://pydiversetransform.readthedocs.io/en/latest/](https://pydiversetransform.readthedocs.io/en/latest/);
  recommended with `lazy=True`)
- `pydiverse.transform.Polars` (
  see [https://pydiversetransform.readthedocs.io/en/latest/](https://pydiversetransform.readthedocs.io/en/latest/);
  recommended with `version=AUTO_VERSION`)
- `polars.LazyFrame` (see [https://pola.rs/](https://pola.rs/); recommended with `version=AUTO_VERSION`)
- `polars.DataFrame` (see [https://pola.rs/](https://pola.rs/); recommended with manual version bumping
  and `version="X.Y.Z"`)
- `pandas.DataFrame` (see [https://pandas.pydata.org/](https://pandas.pydata.org/); recommended with manual version
  bumping and `version="X.Y.Z"`)
- `ibis.Table` (see [https://ibis-project.org/](https://ibis-project.org/); recommended with `lazy=True`)
- `tidypolars.Tibble` (see [https://github.com/markfairbanks/tidypolars](https://github.com/markfairbanks/tidypolars);
  recommended with `lazy=True`)

## [](#pydiverse.pipedag.backend.table.ParquetTableStore)

The ParquetTableStore is derived from the [SQLTableStore](#pydiverse.pipedag.backend.table.SQLTableStore)
with flavor DuckDB. It can be used in much the same way. Just the performance for `input_type`
`pl.DataFrame/pl.LazyFrame/pd.DataFrame` should be much better since they directly read from the
parquet files.

The ParquetTableStore has one additional `args` attribute:
- `parquet_base_path`: Defines the fs_spec base location where parquet files are placed.
It can be both a regular file system location or a cloud storage location.
For regular file locations, directories are created automatically. For cloud storage
locations the bucket must already exist and schemas are represented as key prefixes.
The instance_id is still added as a directory/key prefix level below `parquet_base_path`.

The usage of ParquetTableStore is described [here](parquet_s3.md) in more detail.

(section-examples)=
## Example pipelines for getting started

The following examples can all be run with [pixi](https://pixi.sh/latest/installation/). They require running
`pixi run docker-compose up` in one shell and once the database is up, you can run `pixi run python <script>.py`.
Each includes `run_pipeline.py`. Some also include other pipelines like
[`realistic_pipeline.py`](examples/realistic_pipeline.md) or [`run_pipeline_simple.py`](database_testing.md).

The examples are all part of the pipedag repository and can be executed from there for example with:
```bash
git clone https://github.com/pydiverse/pydiverse.pipedag.git
cd pydiverse.pipedag/example_postgres
pixi run docker-compose up
```
and in a separate shell:
```bash
cd pydiverse.pipedag/example_postgres
pixi run python run_pipeline.py
```

A specific example to get started with one database technology can also be found as zip file (identical content):
- [Postgres example](examples/zip/example_postgres.zip)
- [MSSQL example](examples/zip/example_mssql.zip)
- [IBM DB2 example](examples/zip/example_ibm_db2.zip)
- [Parquet S3 example](examples/zip/example_parquet_s3.zip)

The Parquet S3 example uses minio container as S3 compatible storage and is described
[here](parquet_s3.md) in more detail.

(section-mssql_odbc)=
## Installing mssql odbc driver for macOS and Linux

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
