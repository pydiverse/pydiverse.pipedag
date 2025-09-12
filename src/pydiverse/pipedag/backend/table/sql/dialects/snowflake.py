# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import time
from typing import Literal

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
        # ConnectorX (used by Polars read_database_uri) supports Snowflake.
        return True


@SnowflakeTableStore.register_table(ibis.api.Table)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: SnowflakeTableStore):
        return ibis.snowflake._from_url(store.engine_url)
