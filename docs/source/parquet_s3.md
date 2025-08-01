# Parquet files as table store (incl. S3 support)

Lately, the dataframe based world with parquet as the de-facto standard storage format on disk
developed more dynamically (or enthusiastically) than relational databases. Two tools for data
processing particularly stand out: [Polars](https://www.pola.rs/) and [DuckDB](https://duckdb.org/).
Polars offers extremely fast and highly parallelized processing of dataframes (also much more type-safe
than pandas), and DuckDB is one of the fastest SQL engines available. Both natively can operate on
parquet files. ([Apache Arrow](https://arrow.apache.org/) would also allow in-memory exchange between
polars and duckdb. Pipedag support for this is planned in the future.)

Pydiverse pipedag supports all techniques even with exactly the same user code (only configuration changes:
see {ref}`table_backends <section-examples>`).
Postgres is actually the nicest behaving SQL database and extremely fast for developing with small test data.
When being able to use cloud databases, [Snowflake](https://www.snowflake.com/) and
[BigQuery](https://cloud.google.com/bigquery) are extremely fast and scalable choices based on SQL paradigm
(Snowflake and BigQuery support is planned for
[pydiverse transform](https://pydiversetransform.readthedocs.io/en/latest/) and pipedag).
If you like to base your data pipeline on Polars as much as possible and only use duckdb for a few join
operations where it is faster than polars, you can use the ParquetTableStore (pandas also works but polars
is the more realistic contender to SQL due to type stability, speed, and lazy expression evaluation).

You can find an example how to get started with ParquetTableStore as [zip file](examples/zip/example_parquet_s3.zip)
or you can get it from the pipedag repository as follows:

```bash
git clone https://github.com/pydiverse/pydiverse.pipedag.git
cd pydiverse.pipedag/example_parquet_s3
pixi run docker-compose up
```
and in a separate shell:
```bash
cd pydiverse.pipedag/example_parquet_s3
pixi run python run_pipeline.py
```

The slightly more [realistic pipeline](examples/realistic_pipeline.md) is available in the same directory:
```bash
cd pydiverse.pipedag/example_parquet_s3
pixi run python realistic_pipeline.py
```

You can look into the minio container to see the parquet files with a browser at the url: `http://localhost:9001/`.
username and password are both `minioadmin` and does not need to be changed when playing with toy data.

The example will still produce a duckdb file called `/tmp/pipedag/parquet_duckdb/pipedag_default.duckdb`.
You can open it with the Database UI of your choice and still execute SQL commands that will work on the
parquet files which reside in the minio container. Pipedag places views in the correct schemas.
This query should work after running `run_pipeline.py`: `SELECT * FROM stage_1.dfa`

The main problem with working with the duckdb file is that duckdb recently introduced extremely strict locking.
As a consequence, the Database UI cannot keep a connection open while running the pipeline. One way to solve this is
to copy the duckdb file for interactive use. A good UI (like Jetbrains DataGrip or PyCharm Professional) will also
offer configuration parameters to make this smooth:

<p align="center">
  <img src="_images/datagrip_s3_options01.png" width="45%" />
  <img src="_images/datagrip_s3_options02.png" width="45%" />
</p>

The example uses the following configuration file `pipedag.yaml`:

```yaml
instances:
  __any__:
    network_interface: "127.0.0.1"
    auto_table:
      # When a Task returns an object of these classes, they will automatically be wrapped
      # in a pipedag Table object and materialized with the table store.
      - "pandas.DataFrame"
      - "polars.DataFrame"
      - "polars.LazyFrame"
      - "sqlalchemy.sql.expression.TextClause"
      - "sqlalchemy.sql.expression.Selectable"
      - "pydiverse.transform.Table"

    fail_fast: true  # this provides better stack traces but less fault tolerance
    instance_id: pipedag_default

    # Attention: For disable_kroki: false, stage and task names might be sent to the kroki_url.
    #   You can self-host kroki if you like:
    #   https://docs.kroki.io/kroki/setup/install/
    #   You need to install optional dependency 'pydot' for any visualization
    #   URL to appear.
    disable_kroki: true
    kroki_url: "https://kroki.io"

    table_store:
      class: "pydiverse.pipedag.backend.table.parquet.ParquetTableStore"
      args:
        # This is the main location where the ParquetTableStore will store tables.
        parquet_base_path: "s3://pipedag-test-bucket/table_store/"
        # There is still a duckdb file which keeps read views to all the parquet files.
        # This database file can also be used with a SQL UI to access the parquet files
        # associated with a specific pipeline instance.
        url: "duckdb:////tmp/pipedag/parquet_duckdb/{instance_id}.duckdb"
        create_database_if_not_exists: true
        print_materialize: true
        print_sql: true

      hook_args:
        sql:
          # This controls when temporary tables created by ColSpec will be cleaned up
          cleanup_annotation_action_on_success: false
          cleanup_annotation_action_intermediate_state: false

    # duckdb does not support schema renaming so this is mandatory:
    stage_commit_technique: READ_VIEWS

    lock_manager:
      # consider using FileLockManager or NoLockManager if you don't like to launch a ZooKeeper instance
      class: "pydiverse.pipedag.backend.lock.ZooKeeperLockManager"
      args:
        hosts: "localhost:2181"

    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      args:
        base_path: "/tmp/pipedag/blobs"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"
```

The most important change to relational database configurations is:
```yaml
    table_store:
      class: "pydiverse.pipedag.backend.table.parquet.ParquetTableStore"
```

The ParquetTableStore is based on the SQLTableStore for duckdb, so you still need to give
a duckdb file location as `args: url`.
Don't configure a local_table_cache with ParquetTableStore. It might work, but doesn't make much sense.

The main change that ParquetTableStore implemented is that `CREATE TABLE as SELECT` statements are
turned into `COPY (SELECT ...) TO <fs_spec file location> WITH (FORMAT PARQUET)`. Additionally,
it creates views to the the parquet files in the duckdb file with
`CREATE VIEW ... FROM read_parquet(<file location>)`. In order not to move files, it uses ODD/EVEN schemas
for stage level transactionality. For example if stage is called "stage_1", then the parquet files would
either reside in the schema `stage_1_odd` or `stage_1_even`. In the duckdb file, there will also be schema
`stage_1` with views on the currently active transaction schema. The duckdb file also includes metadata in
table pipedag_metadata.stages to keep track of the current active schema for each stage.

When using `input_type` `pl.DataFrame/pl.LazyFrame/pd.DataFrame`, the parquet files are read and
written directly and not via duckdb.

Currently, there is only one file used per table. This might change in the future by using partitioning
features of polars and duckdb.
