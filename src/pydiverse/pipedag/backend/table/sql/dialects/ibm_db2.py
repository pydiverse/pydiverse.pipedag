from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc

from pydiverse.pipedag.backend.table.sql.ddl import (
    AddPrimaryKey,
    ChangeColumnNullable,
    CreateTableWithSuffix,
    Schema,
)
from pydiverse.pipedag.backend.table.sql.hooks import PandasTableHook
from pydiverse.pipedag.backend.table.sql.reflection import PipedagDB2Reflection
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.backend.table.util import DType
from pydiverse.pipedag.materialize import Table
from pydiverse.pipedag.materialize.details import (
    BaseMaterializationDetails,
    resolve_materialization_details_label,
)

_TABLE_SPACE_KEYWORD_MAP = {
    "table_space_data": "IN",
    "table_space_index": "INDEX IN",
    "table_space_long": "LONG IN",
}


class IBMDB2CompressionTypes(str, Enum):
    NO_COMPRESSION = ""
    ROW_COMPRESSION = "COMPRESS YES"
    STATIC_ROW_COMPRESSION = "COMPRESS YES STATIC"
    ADAPTIVE_ROW_COMPRESSION = "COMPRESS YES ADAPTIVE"
    VALUE_COMPRESSION = "VALUE COMPRESSION"


@dataclass(frozen=True)
class IBMDB2MaterializationDetails(BaseMaterializationDetails):
    """
    :param compression: Specify the compression methods to be applied to the table.
        Possible values include
        compression="COMPRESS YES STATIC"
        compression="COMPRESS YES"
        compression="COMPRESS YES ADAPTIVE"
        compression="VALUE COMPRESSION"
        A list containing combining one of the first three with the last value, e.g.
        compression=["COMPRESS YES ADAPTIVE", "VALUE COMPRESSION"]

        compression="" will result in no compression
    :param table_space_data: The DB2 table space where the data is  stored.
    :param table_space_index: The DB2 table space where the partitioned index is stored.
    :param table_space_long: The DB2 table spaces where the values of any long columns
        are stored.
    """

    def __post_init__(self):
        object.__setattr__(
            self,
            "compression",
            IBMDB2CompressionTypes(self.compression)
            if isinstance(self.compression, str)
            else [IBMDB2CompressionTypes(c) for c in self.compression]
            if self.compression is not None
            else None,
        )

    compression: IBMDB2CompressionTypes | list[IBMDB2CompressionTypes] | None = None
    table_space_data: str | None = None
    table_space_index: str | None = None
    table_space_long: str | list[str] | None = None


class IBMDB2TableStore(SQLTableStore):
    """
    SQLTableStore that supports `IBM Db2 <https://www.ibm.com/products/db2>`_.
    Requires `ibm-db-sa <https://pypi.org/project/ibm-db-sa/>`_ to be installed.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

    _dialect_name = "ibm_db_sa"

    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
        early_not_null_possible: bool = False,
    ):
        if not early_not_null_possible:
            for retry_iteration in range(4):
                # retry operation since it might have been terminated as a
                # deadlock victim
                try:
                    self.execute(
                        ChangeColumnNullable(
                            table_name, schema, key_columns, nullable=False
                        )
                    )
                    break
                except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                    if retry_iteration == 3:
                        raise
                    time.sleep(retry_iteration * retry_iteration * 1.1)
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    def add_indexes(
        self, table: Table, schema: Schema, *, early_not_null_possible: bool = False
    ):
        super().add_indexes(
            table, schema, early_not_null_possible=early_not_null_possible
        )
        table_name = self.engine.dialect.identifier_preparer.quote(table.name)
        schema_name = self.engine.dialect.identifier_preparer.quote_schema(schema.get())
        query = (
            f"CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE {schema_name}.{table_name}"
            f" ON ALL COLUMNS WITH DISTRIBUTION ON ALL COLUMNS AND UNSAMPLED"
            f" DETAILED INDEXES ALL SET PROFILE');"
        )
        self.execute(query)

    def resolve_alias(self, table: Table, stage_name: str) -> tuple[str, str]:
        # The base implementation already takes care of converting Table objects
        # based on ExternalTableReference objects to string table name and schema.
        # For normal Table objects, it needs the stage schema name.
        table_name, schema = super().resolve_alias(table, stage_name)
        return PipedagDB2Reflection.resolve_alias(self.engine, table_name, schema)

    def check_materialization_details_supported(self, label: str | None) -> None:
        _ = label
        return

    def _set_materialization_details(
        self, materialization_details: dict[str, dict[str | list[str]]] | None
    ) -> None:
        self.materialization_details = (
            IBMDB2MaterializationDetails.create_materialization_details_dict(
                materialization_details,
                self.strict_materialization_details,
                self.default_materialization_details,
                self.logger,
            )
        )

    def _get_compression(
        self, materialization_details_label: str | None
    ) -> str | list[str] | None:
        compression: IBMDB2CompressionTypes | list[
            IBMDB2CompressionTypes
        ] | None = IBMDB2MaterializationDetails.get_attribute_from_dict(
            self.materialization_details,
            materialization_details_label,
            self.default_materialization_details,
            "compression",
            self.strict_materialization_details,
            self.logger,
        )
        if isinstance(compression, list):
            return [c.value for c in compression]
        if compression is not None:
            return compression.value

    def _get_table_spaces(
        self, materialization_details_label: str | None
    ) -> dict[str, str]:
        return {
            f"table_space_{st}": IBMDB2MaterializationDetails.get_attribute_from_dict(
                self.materialization_details,
                materialization_details_label,
                self.default_materialization_details,
                f"table_space_{st}",
                self.strict_materialization_details,
                self.logger,
            )
            for st in ("data", "index", "long")
        }

    def get_create_table_suffix(self, materialization_details_label: str | None) -> str:
        table_spaces = self._get_table_spaces(materialization_details_label)
        table_space_suffix = " ".join(
            f"{_TABLE_SPACE_KEYWORD_MAP[stype]} "
            f"{self.engine.dialect.identifier_preparer.quote(sname)}"
            for stype, sname in table_spaces.items()
            if sname
        )
        compression = self._get_compression(materialization_details_label)
        if isinstance(compression, str):
            compression = [compression]
        elif compression is None:
            compression = []
        compression_suffix = " ".join(compression)
        return " ".join((table_space_suffix, compression_suffix))


@IBMDB2TableStore.register_table(pd)
class PandasTableHook(PandasTableHook):
    @classmethod
    def _execute_materialize(
        cls,
        df: pd.DataFrame,
        store: IBMDB2TableStore,
        table: Table[pd.DataFrame],
        schema: Schema,
        dtypes: dict[str, DType],
    ):
        # Default string target is CLOB which can't be used for indexing.
        # -> Convert indexed string columns to VARCHAR(256)
        index_columns = set()
        if indexes := table.indexes:
            index_columns |= {col for index in indexes for col in index}
        if primary_key := table.primary_key:
            index_columns |= set(primary_key)

        dtypes = ({name: dtype.to_sql() for name, dtype in dtypes.items()}) | (
            {
                name: (
                    sa.String(length=256)
                    if name in index_columns
                    else sa.String(length=32_672)
                )
                for name, dtype in dtypes.items()
                if dtype == DType.STRING
            }
        )

        if table.type_map:
            dtypes.update(table.type_map)

        engine = store.engine
        suffix = store.get_create_table_suffix(
            resolve_materialization_details_label(table)
        )
        if suffix:
            store.execute(CreateTableWithSuffix(table.name, schema, dtypes, suffix))

        # noinspection PyTypeChecker
        df.to_sql(
            table.name,
            engine,
            schema=schema.get(),
            index=False,
            dtype=dtypes,
            chunksize=100_000,
            if_exists="append" if suffix else "fail",
        )
