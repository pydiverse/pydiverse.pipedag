# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import polars as pl
import sqlalchemy as sa
from upath import UPath

from pydiverse.common.util.structlog import setup_logging
from pydiverse.pipedag import ExternalTableReference, Stage, Table
from pydiverse.pipedag.backend import ParquetTableStore, SQLTableStore


def main():
    # This is *not* the recommended way of processing the output of pipedag tasks.
    # But in case one simply wants to download tables from an SQL database, this should work
    # using pipedag dematerialization/materialization hooks.

    instance_id = "pipedag_default"
    parquet_path = UPath("/tmp/pipedag/")
    mssql_url = (
        f"mssql+pyodbc://sa:PydiQuant27@127.0.0.1:1433/{instance_id}?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
    )
    mssql_store = SQLTableStore(mssql_url)
    parquet_store = ParquetTableStore(
        "duckdb://", parquet_base_path=parquet_path, force_transaction_suffix="", allow_overwrite=True
    )
    parquet_store.set_instance_id("dump_parquet_files")
    parquet_store.disable_caching = True

    with mssql_store.engine_connect() as conn:
        schemas = conn.execute(sa.text("SELECT name FROM sys.schemas")).fetchall()
    schemas = [
        s[0]
        for s in schemas
        if s[0] not in ("sys", "INFORMATION_SCHEMA", "guest", "dbo") and not s[0].startswith("db_")
    ]
    for schema in schemas:
        stage = Stage(schema, force_committed=True)
        table_names = mssql_store.get_table_objects_in_stage(stage)
        print(f"{schema}:{table_names}")
        parquet_store.init_stage(stage)
        for table_name in table_names:
            table = Table(ExternalTableReference(name=table_name, schema=schema))
            tbl = Table(mssql_store.retrieve_table_obj(table, as_type=pl.LazyFrame), table_name)
            tbl.stage = stage
            print(f"{table_name}:{tbl.obj.collect_schema()}")
            parquet_store.store_table(tbl, task=None)


if __name__ == "__main__":
    setup_logging()  # you can setup the logging and/or structlog libraries as you wish

    # see https://pydiversepipedag.readthedocs.io/en/latest/table_backends.html#installing-mssql-odbc-driver-for-macos-and-linux
    # for how to install the ODBC driver for MSSQL on macOS and Linux.

    # Run docker-compose in separate shell to launch Microsoft SQL Server container:
    # ```shell
    # pixi run docker-compose up
    # ```

    # Run this pipeline with (might take a bit longer on first run in pixi environment):
    # ```shell
    # pixi run python run_pipeline.py
    # ```

    main()
