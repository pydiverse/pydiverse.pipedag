# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import time
from typing import Literal

import polars as pl
import sqlalchemy as sa

from pydiverse.pipedag.backend.table.sql import hooks
from pydiverse.pipedag.backend.table.sql.hooks import (
    IbisTableHook,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.optional_dependency.ibis import ibis
from pydiverse.pipedag.optional_dependency.snowflake import snowflake


class SnowflakeTableStore(SQLTableStore):
    """
    SQLTableStore that supports `Snowflake`_.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

    _dialect_name = "snowflake"

    def _default_isolation_level(self) -> str | None:
        return None  # "READ UNCOMMITTED" does not exist in Snowflake

    def optional_pause_for_db_transactionality(
        self,
        prev_action: Literal[
            "table_drop",
            "table_create",
            "schema_drop",
            "schema_create",
            "schema_rename",
        ],
    ):
        _ = prev_action
        # The snowflake backend has transactionality problems with very quick
        # DROP/CREATE or RENAME activities for both schemas and tables
        # which happen in testing.
        time.sleep(2)

    def _init_database(self):
        create_database = self.engine_url.database.split("/")[0]
        with self.engine.connect() as conn:
            if not [
                x.name
                for x in conn.exec_driver_sql("SHOW DATABASES").mappings().all()
                if x.name.upper() == create_database.upper()
            ]:
                self._init_database_with_database(
                    "snowflake",
                    disable_exists_check=True,
                    create_database=create_database,
                )


@SnowflakeTableStore.register_table(snowflake)
class PolarsTableHook(hooks.PolarsTableHook):
    @classmethod
    def dialect_supports_connectorx(cls):
        # ConnectorX (used by Polars read_database_uri) does not support Snowflake.
        return False

    @classmethod
    def dialect_wrong_polars_column_names(cls):
        # for Snowflake, polars returns uppercase column names by default
        return True

    @classmethod
    def adbc_write_database(
        cls, df: pl.DataFrame, engine: sa.Engine, schema_name: str, table_name: str, if_table_exists="append"
    ) -> int:
        import adbc_driver_snowflake.dbapi as adbc

        # polars does not really handle snowflake well (we use PAT token as SNOWFLAKE_PASSWORD)
        conn = adbc.connect(
            db_kwargs={
                "adbc.snowflake.sql.account": engine.url.host,
                "adbc.snowflake.sql.db": engine.url.database.split("/")[0],  # remove schema
                "adbc.snowflake.sql.schema": schema_name,
                "adbc.snowflake.sql.auth_type": "auth_pat",
                "adbc.snowflake.sql.client_option.auth_token": engine.url.password,
            }
        )

        # try using ADBC, first
        return df.write_database(
            table_name,  # schema see above
            connection=conn,
            if_table_exists=if_table_exists,
            engine="adbc",
        )


@SnowflakeTableStore.register_table(ibis.api.Table)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: SnowflakeTableStore):
        return ibis.snowflake._from_url(store.engine_url)
