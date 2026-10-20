# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause
import copy
import getpass
import os
import time
from contextlib import contextmanager
from typing import Literal

import pandas as pd
import polars as pl
import sqlalchemy as sa

import pydiverse.pipedag.backend.table.sql.hooks as sql_hooks
from pydiverse.pipedag import Schema, Table
from pydiverse.pipedag.backend.table.sql.ddl import AddIndex
from pydiverse.pipedag.backend.table.sql.hooks import (
    IbisTableHook,
)
from pydiverse.pipedag.backend.table.sql.sql import SQLTableStore
from pydiverse.pipedag.context import ConfigContext, StageCommitTechnique
from pydiverse.pipedag.optional_dependency.ibis import ibis
from pydiverse.pipedag.optional_dependency.snowflake import snowflake


class SnowflakeTableStore(SQLTableStore):
    """
    SQLTableStore that supports `Snowflake`_.

    Takes the same arguments as
    :py:class:`SQLTableStore <pydiverse.pipedag.backend.table.SQLTableStore>`
    """

    _dialect_name = "snowflake"
    # could be made configurable in pipedag.yaml if needed:
    ADBC_MAX_RETRIES = 3
    SQLALCHEMY_PRE_PING = False
    SQLALCHEMY_RECYCLE = 1200
    SQLALCHEMY_LIFO = True

    def _create_engine(self, **kwargs):
        connect_args = dict(kwargs.pop("connect_args", {}) or {})
        connect_args.setdefault("client_session_keep_alive", True)
        session_parameters = dict(connect_args.get("session_parameters", {}) or {})
        session_parameters.setdefault("QUERY_TAG", self.get_adbc_query_tag("|sqa"))
        connect_args["session_parameters"] = session_parameters

        # knobs you can expose/configure:
        kwargs.setdefault("pool_pre_ping", self.SQLALCHEMY_PRE_PING)
        kwargs.setdefault("pool_recycle", self.SQLALCHEMY_RECYCLE)
        kwargs.setdefault("pool_use_lifo", self.SQLALCHEMY_LIFO)
        kwargs["connect_args"] = connect_args

        return super()._create_engine(**kwargs)

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
        time.sleep(0)  # trying with 0 again. This was 2s at some point

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

    # --- connection open/apply-session -----------------------------------------
    def _adbc_open(self):
        import adbc_driver_snowflake.dbapi as adbc

        engine = self.engine
        adbc_query_tag = self.get_adbc_query_tag()
        raw_kwargs = {
            "adbc.snowflake.sql.account": engine.url.host,
            "adbc.snowflake.sql.db": engine.url.database.split("/")[0] if engine.url.database else None,
            "adbc.snowflake.sql.schema": (
                engine.url.database.split("/")[1] if engine.url.database and "/" in engine.url.database else None
            ),
            "adbc.snowflake.sql.warehouse": getattr(self, "warehouse", None) or engine.url.query.get("warehouse"),
            "adbc.snowflake.sql.role": getattr(self, "role", None) or engine.url.query.get("role"),
            "adbc.snowflake.sql.auth_type": "auth_pat",
            "adbc.snowflake.sql.client_option.auth_token": engine.url.password,
            "adbc.snowflake.sql.client_option.use_high_precision": "false",
            "adbc.snowflake.sql.client_option.keep_session_alive": "true",
        }
        db_kwargs = {k: v for k, v in raw_kwargs.items() if v is not None}
        conn = adbc.connect(db_kwargs=db_kwargs)
        with conn.cursor() as cur:
            cur.execute(f"ALTER SESSION SET QUERY_TAG='{adbc_query_tag}'")
            cur.execute("ALTER SESSION SET CLIENT_SESSION_KEEP_ALIVE=TRUE")
            cur.execute("ALTER SESSION SET CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY=900")  # 15 min
            cur.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE=TRUE")  # ADBC append cannot do unquoted names
        self.keep_alive_adbc_connection = conn
        return conn

    def get_adbc_query_tag(self, suffix="") -> str:
        adbc_query_tag = f"pipedag|{self.instance_id}|user:{getpass.getuser()}|pid:{os.getpid()}{suffix}"
        return adbc_query_tag

    def adbc_connect(self):
        if self.keep_alive_db_connections is None:
            return self._adbc_open()
        return self.keep_alive_db_connections

    # --- retry API ---
    def adbc_reconnect(self, state: list) -> bool:
        """
        Gate for the caller's while-loop. Initializes state and returns
        True while we still have attempts left (including the first try).

        Example usage:
        state = []
        while store.adbc_reconnect(state):
            with store.adbc_reconnect_attempt(state) as conn:
                pl.read_database(query, connection=conn)
        """
        if len(state) != 3:
            # [attempt_idx, max_retries, last_exc]
            state[:] = [0, self.ADBC_MAX_RETRIES, None]
            return True

        attempt, max_retries, last_exc = state
        if attempt <= max_retries:
            return last_exc is not None

        # exhausted → re-raise the last captured OperationalError (if any)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("ADBC retries exhausted with no captured exception")

    @contextmanager
    def adbc_reconnect_attempt(self, state: list):
        """
        Yields a live ADBC connection.
        On OperationalError:
          - capture the exception in state[2]
          - increment attempt counter in state[0]
          - reopen sticky connection so next loop has a fresh one
          - suppress the error so the loop can iterate
        """
        assert len(state) == 3
        import adbc_driver_snowflake.dbapi as adbc

        conn = self.adbc_connect()
        try:
            yield conn
        except adbc.OperationalError as exc:
            # store exception
            state[2] = exc
            # bump attempt index
            state[0] += 1
            # teardown & reopen the sticky connection so next loop has a fresh one
            try:
                if self.keep_alive_adbc_connection is not None:
                    self.keep_alive_adbc_connection.close()
            finally:
                self.keep_alive_adbc_connection = None
                self._adbc_open()
            # swallow the error so the while-loop continues
        # let all other exceptions propagate

    def add_index(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        self.logger.warning(
            "Snowflake does not support creating indexes on snowflake tables",
            query_not_executed=str(AddIndex(table_name, schema, index_columns, name)),
        )

    def init_stage(self, stage):
        super().init_stage(stage)
        self._clear_reflection_cache_for_schema(self.get_schema(stage.transaction_name).get())

    def commit_stage(self, stage):
        transaction_schema = self.get_schema(stage.transaction_name).get()
        final_schema = self.get_schema(stage.name).get()
        stage_commit_technique = ConfigContext.get().stage_commit_technique

        super().commit_stage(stage)

        self._clear_reflection_cache_for_schema(final_schema)
        self._replace_reflection_cache(
            transaction_schema,
            final_schema,
            keep_source=stage_commit_technique == StageCommitTechnique.READ_VIEWS,
        )

    def reflect_table(self, table_name: str, schema: str | Schema) -> sa.Table:
        if isinstance(schema, Schema):
            schema = schema.get()

        schema_cache = self.hook_cache.get(self._reflection_cache_key(schema))
        if schema_cache is None:
            schema_cache = self._load_schema_reflection(schema)
            self.hook_cache[self._reflection_cache_key(schema)] = schema_cache

        table_key = self._normalize_reflection_name(table_name)
        column_specs = schema_cache.get(table_key)

        if column_specs is None:
            schema_cache = self._load_schema_reflection(schema)
            self.hook_cache[self._reflection_cache_key(schema)] = schema_cache
            column_specs = schema_cache.get(table_key)

        if column_specs is None:
            raise sa.exc.NoSuchTableError(table_name)

        return self._build_reflected_table(schema, table_name, column_specs)

    def _reflection_cache_key(self, schema: str) -> tuple[type["SnowflakeTableStore"], str, str]:
        return type(self), "snowflake_reflection", schema

    def _clear_reflection_cache_for_schema(self, schema: str):
        self.hook_cache.pop(self._reflection_cache_key(schema), None)

    def _replace_reflection_cache(self, from_schema: str, to_schema: str, *, keep_source: bool):
        from_key = self._reflection_cache_key(from_schema)
        to_key = self._reflection_cache_key(to_schema)

        if from_key == to_key:
            return

        payload = self.hook_cache.get(from_key)
        self.hook_cache.pop(to_key, None)

        if payload is None:
            return

        if keep_source:
            self.hook_cache[to_key] = copy.deepcopy(payload)
        else:
            self.hook_cache[to_key] = self.hook_cache.pop(from_key)

    def _split_reflection_schema(self, schema: str) -> tuple[str | None, str]:
        if schema.count(".") == 1:
            return tuple(schema.split(".", 1))
        return None, schema

    def _quote_identifier(self, identifier: str) -> str:
        return self.engine.dialect.identifier_preparer.quote_identifier(identifier)

    def _normalize_reflection_name(self, name: str) -> str:
        if hasattr(self, "engine") and hasattr(self.engine.dialect, "normalize_name"):
            normalized = self.engine.dialect.normalize_name(name)
            if normalized is not None:
                return normalized
        return name.casefold()

    def _load_schema_reflection(self, schema: str) -> dict[str, list[dict[str, object]]]:
        database, schema_part = self._split_reflection_schema(schema)
        info_schema = "INFORMATION_SCHEMA.COLUMNS"
        if database is not None:
            info_schema = f"{self._quote_identifier(database)}.{info_schema}"

        query = sa.text(
            f"""
            SELECT
                TABLE_NAME,
                COLUMN_NAME,
                ORDINAL_POSITION,
                IS_NULLABLE,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION
            FROM {info_schema}
            WHERE UPPER(TABLE_SCHEMA) = UPPER(:schema_name)
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
        )

        with self.engine.connect() as conn:
            rows = conn.execute(query, {"schema_name": schema_part}).mappings().all()

        schema_cache: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            table_key = self._normalize_reflection_name(row["table_name"])
            schema_cache.setdefault(table_key, []).append(
                {
                    "name": self._normalize_reflection_name(row["column_name"]),
                    "nullable": str(row["is_nullable"]).upper() == "YES",
                    "type": self._sa_type_from_info_schema_row(row),
                }
            )
        return schema_cache

    def _build_reflected_table(self, schema: str, table_name: str, column_specs: list[dict[str, object]]) -> sa.Table:
        columns = [
            sa.Column(
                spec["name"],
                copy.deepcopy(spec["type"]),
                nullable=spec["nullable"],
            )
            for spec in column_specs
        ]
        return sa.Table(table_name, sa.MetaData(), *columns, schema=schema)

    @staticmethod
    def _info_schema_int(value):
        return None if value is None else int(value)

    def _sa_type_from_info_schema_row(self, row) -> sa.types.TypeEngine:
        data_type = str(row["data_type"]).upper()
        length = self._info_schema_int(row["character_maximum_length"])
        precision = self._info_schema_int(row["numeric_precision"])
        scale = self._info_schema_int(row["numeric_scale"])

        if data_type in {"FIXED", "NUMBER", "NUMERIC", "DECIMAL"}:
            kwargs = {}
            if precision is not None:
                kwargs["precision"] = precision
            if scale is not None:
                kwargs["scale"] = scale
            return sa.Numeric(**kwargs)
        if data_type in {"FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL"}:
            return sa.Float() if precision is None else sa.Float(precision=precision)
        if data_type in {"VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER"}:
            return sa.String(length=length)
        if data_type in {"BINARY", "VARBINARY"}:
            return sa.LargeBinary(length=length)
        if data_type == "BOOLEAN":
            return sa.Boolean()
        if data_type == "DATE":
            return sa.Date()
        if data_type == "TIME":
            return sa.Time()
        if data_type == "TIMESTAMP_NTZ":
            return sa.TIMESTAMP(timezone=False)
        if data_type in {"TIMESTAMP_LTZ", "TIMESTAMP_TZ"}:
            return sa.TIMESTAMP(timezone=True)
        if data_type in {"ARRAY", "OBJECT", "VARIANT"}:
            return sa.JSON()
        return sa.NullType()

    def has_table_or_view(self, name: str, schema: Schema | str):
        if isinstance(schema, Schema):
            schema = schema.get()
        if schema.count(".") == 1:
            # sqlalchemy snowflake driver does not support schema="database.schema"
            database, schema_part = schema.split(".")
            with self.engine.connect() as conn:
                conn.execute(sa.text(f"USE DATABASE {database}"))
                has_table = self._has_table_or_view(conn, name, schema_part)
                conn.execute(sa.text(f"USE DATABASE {self.engine.url.database}"))
        else:
            has_table = self._has_table_or_view(self.engine, name, schema)
        return has_table

    @staticmethod
    def _has_table_or_view(conn, name: str, schema: str) -> bool:
        inspector = sa.inspect(conn)
        has_table = inspector.has_table(name, schema=schema)
        # workaround for sqlalchemy backends that fail to find views with has_table
        # in particular DuckDB https://github.com/duckdb/duckdb/issues/10322
        if not has_table:
            has_table = name in inspector.get_view_names(schema=schema)
        return has_table


@SnowflakeTableStore.register_table(snowflake)
class SQLAlchemyTableHook(sql_hooks.SQLAlchemyTableHook):
    @classmethod
    def retrieve(
        cls,
        store: SQLTableStore,
        table: Table,
        stage_name: str | None,
        as_type: type[sa.Table],
        limit: int | None = None,
    ) -> sa.sql.expression.Selectable:
        tbl = cls.__base__.retrieve(store, table, stage_name, as_type, limit)

        # fix integer types
        def bind(c: sa.Column, expr):
            col = sa.Column(c.name, c.type)
            col.table = expr
            return col

        def fix(c: sa.Column) -> sa.Column:
            # convert Numeric(38,0) / DECIMAL(38,0) to BIGINTEGER
            if isinstance(c.type, sa.Numeric) and c.type.precision == 38 and c.type.scale == 0:
                new_c = copy.deepcopy(c)
                new_c.type = sa.BigInteger()
                return new_c
            return c

        try:
            from sqlalchemy.sql.base import ColumnCollection, ReadOnlyColumnCollection

            tbl.c = ReadOnlyColumnCollection(ColumnCollection([(c.name, bind(fix(c), tbl)) for c in tbl.c]))
        except ImportError:
            store.logger.info("Can't fix snowflake sqlalchemy types for sqlalchemy < 2", columns=list(tbl.c))
        return tbl


@SnowflakeTableStore.register_table(snowflake)
class PandasTableHook(sql_hooks.PandasTableHook):
    @classmethod
    def upload_table(
        cls,
        table: Table[pd.DataFrame],
        schema: str,
        dtypes: dict[str, sa.types.TypeEngine],
        store: SQLTableStore,
        early: bool,
    ):
        df = table.obj
        # snowflake driver cannot receive object column with datetime objects
        obj_cols = df.dtypes[df.dtypes == object]  # noqa:E721
        if len(obj_cols) > 0:
            datetime_cols = []
            datetime_ns_cols = []

            def compatible(series: pd.Series, _type: str):
                return (series[~series.isna()].astype(_type) == series[~series.isna()]).all()

            for col in obj_cols.index:
                # detect datetime columns
                try:
                    if compatible(df[col], "datetime64[us]"):
                        datetime_cols.append(col)
                    elif compatible(df[col], "datetime64[ns]"):
                        datetime_ns_cols.append(col)
                except ValueError:
                    pass
            df[datetime_cols] = df[datetime_cols].astype("datetime64[us]")
            df[datetime_ns_cols] = df[datetime_ns_cols].astype("datetime64[ns]")
        super().upload_table(table, schema, dtypes, store, early)


@SnowflakeTableStore.register_table(snowflake)
class PolarsTableHook(sql_hooks.PolarsTableHook):
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
        cls,
        df: pl.DataFrame,
        store: SQLTableStore,
        schema_name: str,
        table_name: str,
        if_table_exists: Literal["replace", "append", "fail"] = "append",
    ) -> int:
        def _quote_ident(name: str) -> str:
            assert '"' not in name, f"Let's not test escaping rules for snowflake with quote in: '{name}'"
            return '"' + name + '"'  # we set session to case-insensitive identifiers above (ADBC cannot do unquoted)

        state: list = []
        while store.adbc_reconnect(state):
            with store.adbc_reconnect_attempt(state) as conn:
                # workaround ADBC/snowflake bug that it cannot transport schema:
                with conn.cursor() as cur:
                    cur.execute(f"USE SCHEMA {_quote_ident(schema_name)}")

                return df.write_database(
                    _quote_ident(table_name),  # schema see above
                    connection=conn,
                    if_table_exists=if_table_exists,
                    engine="adbc",
                )

    @classmethod
    def adbc_read_database(cls, query, store: SQLTableStore) -> pl.DataFrame:
        state: list = []
        while store.adbc_reconnect(state):
            with store.adbc_reconnect_attempt(state) as conn:
                df = pl.read_database(query, connection=conn)
                # TODO: check whether this is still needed
                # # convert Decimal(38,0) to Int64 (snowflake treats them interchangeably)
                # df = df.with_columns(pl.selectors.by_dtype(pl.Decimal(38, 0)).cast(pl.Int64))
                return df


@SnowflakeTableStore.register_table(ibis.api.Table)
class IbisTableHook(IbisTableHook):
    @classmethod
    def _conn(cls, store: SnowflakeTableStore):
        return ibis.snowflake._from_url(store.engine_url)
