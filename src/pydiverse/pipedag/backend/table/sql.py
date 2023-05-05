from __future__ import annotations

import copy
import itertools
import json
import re
import textwrap
import threading
import time
import traceback
import warnings
from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.sql.elements
from sqlalchemy.dialects.mssql import DATETIME2

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.backend.table.util import engine_dispatch
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    AddIndex,
    AddPrimaryKey,
    ChangeColumnNullable,
    ChangeColumnTypes,
    CopyTable,
    CreateAlias,
    CreateDatabase,
    CreateSchema,
    CreateTableAsSelect,
    CreateViewAsSelect,
    DropAlias,
    DropFunction,
    DropProcedure,
    DropSchema,
    DropTable,
    DropView,
    RenameSchema,
    RenameTable,
    Schema,
    ibm_db_sa_fix_name,
    split_ddl_statement,
)
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.context.context import ConfigContext, StageCommitTechnique
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.materialize.util import compute_cache_key
from pydiverse.pipedag.util.ipc import human_thread_id


class SQLTableStore(BaseTableStore):
    """Table store that materializes tables to a SQL database

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.
    """

    METADATA_SCHEMA = "pipedag_metadata"

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        engine_url = config.pop("url")
        return cls(engine_url, **config)

    def __init__(
        self,
        engine_url: str,
        create_database_if_not_exists: bool = False,
        schema_prefix: str = "",
        schema_suffix: str = "",
        avoid_drop_create_schema: bool = False,
        disable_pytsql: bool = False,
        pytsql_isolate_top_level_statements: bool = True,
        print_materialize: bool = False,
        print_sql: bool = False,
        no_db_locking: bool = True,
    ):
        """
        Construct table store.

        :param engine_url:
            URL for SQLAlchemy engine
        :param create_database_if_not_exists:
            whether to create database if it does not exist
        :param schema_prefix:
            prefix string for schemas (dot is interpreted as database.schema)
        :param schema_suffix:
            suffix string for schemas (dot is interpreted as database.schema)
        :param avoid_drop_create_schema
            avoid creating and dropping schemas
        :param disable_pytsql:
            whether to disable the use of pytsql (dialect mssql only)
        :param pytsql_isolate_top_level_statements:
            forward pytsql executes() parameter
        :param print_materialize:
            whether to print select statements before materialization
        :param print_sql:
            whether to print final SQL statements (except for metadata)
        :param no_db_locking:
            speed up database by telling it we will not rely on it's locking mechanisms
        """
        super().__init__()

        self.create_database_if_not_exists = create_database_if_not_exists
        self.schema_prefix = schema_prefix
        self.schema_suffix = schema_suffix
        self.disable_pytsql = disable_pytsql
        self.avoid_drop_create_schema = avoid_drop_create_schema
        self.pytsql_isolate_top_level_statements = pytsql_isolate_top_level_statements
        self.print_materialize = print_materialize
        self.print_sql = print_sql
        self.no_db_locking = no_db_locking
        self.metadata_schema = self.get_schema(self.METADATA_SCHEMA)

        self._init_database(engine_url, create_database_if_not_exists)
        self.engine_url_obj = sa.engine.make_url(engine_url)
        self.engine_url_no_pw = repr(self.engine_url_obj)
        self.engine = self._connect(engine_url, self.schema_prefix, self.schema_suffix)
        self.engines_other = dict()  # i.e. for IBIS connection

        # Set up metadata tables and schema
        from sqlalchemy import CLOB, BigInteger, Boolean, Column, DateTime, String

        if self.engine.dialect.name == "ibm_db_sa":
            clob_type = CLOB  # VARCHAR(MAX) does not exist
        else:
            clob_type = String  # VARCHAR(MAX)s

        self.sql_metadata = sa.MetaData()

        # Stage Table is unique for stage
        # (we currently cannot version changes of this metadata table when using
        # stage_commit_tequnique=read_views)
        self.stage_table = sa.Table(
            "stages",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("stage", String(64)),
            Column("cur_transaction_name", String(256)),
            schema=self.metadata_schema.get(),
        )

        # Store version number for metadata table schema evolution.
        # We disable caching in case of version mismatch.
        self.disable_caching = False
        self.metadata_version = "0.2.0"  # Increase version if metadata table changes
        self.version_table = sa.Table(
            "metadata_version",
            self.sql_metadata,
            Column("version", String(32)),
            schema=self.metadata_schema.get(),
        )

        # Task Table is unique for stage * in_transaction_schema
        self.tasks_table = sa.Table(
            "tasks",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String(128)),
            Column("stage", String(64)),
            Column("version", String(64)),
            Column("timestamp", DateTime),
            Column("run_id", String(32)),
            Column("input_hash", String(32)),
            Column("cache_fn_hash", String(32)),
            Column("output_json", clob_type),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Lazy Cache Table is unique for stage * in_transaction_schema
        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String(128)),
            Column("stage", String(64)),
            Column("query_hash", String(32)),
            Column("task_hash", String(32)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Sql Cache Table is unique for stage * in_transaction_schema
        self.raw_sql_cache_table = sa.Table(
            "raw_sql_tables",
            self.sql_metadata,
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("prev_tables", String(256)),
            Column("tables", String(256)),
            Column("stage", String(64)),
            Column("query_hash", String(32)),
            Column("task_hash", String(32)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        self.logger.info(
            "Initialized SQL Table Store",
            engine_url=self.engine_url_no_pw,
            schema_prefix=self.schema_prefix,
            schema_suffix=self.schema_suffix,
        )

    @staticmethod
    def _init_database(engine_url: str, create_database_if_not_exists: bool):
        if not create_database_if_not_exists:
            return

        engine = sa.create_engine(engine_url)
        if engine.dialect.name in ["mssql", "ibm_db_sa"]:
            engine.dispose()
            return

        # Attention: This is a really hacky way to create a generic engine for
        #            creating a database before one can open a connection to
        #            self.engine_url which references a database and will fail
        #            if the database does not exist
        url = sa.engine.make_url(engine_url)

        if engine.dialect.name == "postgresql":
            try:
                with engine.connect() as conn:
                    # try whether connection with database in connect string works
                    conn.execute(sa.text("SELECT 1"))
            except sa.exc.DBAPIError:
                postgres_db_url = url.set(database="postgres")
                tmp_engine = sa.create_engine(postgres_db_url)
                try:
                    with tmp_engine.connect() as conn:
                        conn.execute(sa.text("COMMIT"))
                        conn.execute(CreateDatabase(url.database))
                except sa.exc.DBAPIError:
                    # This happens if multiple instances try to create the database
                    # at the same time.
                    with engine.connect() as conn:
                        # Verify database actually exists
                        conn.execute(sa.text("SELECT 1"))
        else:
            raise NotImplementedError(
                "create_database_if_not_exists is only implemented for postgres, yet"
            )
        engine.dispose()

    @staticmethod
    def _connect(engine_url, schema_prefix, schema_suffix):
        engine = sa.create_engine(engine_url)

        # if engine.dialect.name == "ibm_db_sa":
        #     engine.dispose()
        #     # switch to READ STABILITY isolation level to avoid unnecessary deadlock
        #     # victims in case of background operations when reflecting columns
        #     engine = sa.create_engine(
        #         engine_url, execution_options={"isolation_level": "RS"}
        #     )
        # sqlalchemy 2 has create_engine().execution_options()

        if engine.dialect.name == "mssql":
            engine.dispose()
            # this is needed to allow for CREATE DATABASE statements
            # (we don't rely on database transactions anyways)
            engine = sa.create_engine(engine_url, connect_args={"autocommit": True})

        if engine.dialect.name == "mssql":
            if "." in schema_prefix and "." in schema_suffix:
                raise AttributeError(
                    "Config Error: It is not allowed to have a dot in both"
                    " schema_prefix and schema_suffix for SQL Server / mssql database:"
                    f' schema_prefix="{schema_prefix}", schema_suffix="{schema_suffix}"'
                )

            if (schema_prefix + schema_suffix).count(".") != 1:
                raise AttributeError(
                    "Config Error: There must be exactly dot in both schema_prefix and"
                    " schema_suffix together for SQL Server / mssql database:"
                    f' schema_prefix="{schema_prefix}", schema_suffix="{schema_suffix}"'
                )
        return engine

    @contextmanager
    def engine_connect(self):
        if self.engine.dialect.name == "ibm_db_sa":
            conn = self.engine.connect()
        else:
            # sqlalchemy 2.0 uses this (except for dialect ibm_db_sa)
            conn = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        try:
            yield conn
        finally:
            try:
                # sqlalchemy 2.0 + ibm_db_sa needs this
                conn.commit()
            except AttributeError:
                # sqlalchemy 1.x does not have this function and does not need it
                pass

    def get_schema(self, name):
        return Schema(name, self.schema_prefix, self.schema_suffix)

    def _execute(self, query, conn: sa.engine.Connection):
        if self.print_sql:
            if isinstance(query, str):
                query_str = query
            else:
                query_str = str(
                    query.compile(self.engine, compile_kwargs={"literal_binds": True})
                )
            pretty_query_str = self.format_sql_string(query_str)
            self.logger.info(f"Executing sql:\n{pretty_query_str}")

        if isinstance(query, str):
            return conn.execute(sa.text(query))
        else:
            return conn.execute(query)

    def execute(self, query, *, conn: sa.engine.Connection = None):
        if conn is None:
            with self.engine_connect() as conn:
                if isinstance(query, str):
                    return self.execute(sa.text(query), conn=conn)
                else:
                    return self.execute(query, conn=conn)

        if isinstance(query, sa.schema.DDLElement):
            # Some custom DDL statements contain multiple statements.
            # They are all seperated using a special seperator.
            query_str = str(
                query.compile(self.engine, compile_kwargs={"literal_binds": True})
            )
            try:
                # sqlalchemy 2.0 + ibm_db_sa needs this
                conn.commit()
            except AttributeError:
                # sqlalchemy 1.x does not have this function and does not need it
                pass
            with conn.begin():
                for part in split_ddl_statement(query_str):
                    self._execute(part, conn)
            return

        return self._execute(query, conn)

    def add_indexes(
        self, table: Table, schema: Schema, *, early_not_null_possible: bool = False
    ):
        if table.primary_key is not None:
            key = table.primary_key
            if isinstance(key, str):
                key = [key]
            self.add_primary_key(
                table.name, schema, key, early_not_null_possible=early_not_null_possible
            )
        if table.indexes is not None:
            for index in table.indexes:
                self.add_index(table.name, schema, index)

    @engine_dispatch
    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
        early_not_null_possible: bool = False,
    ):
        _ = early_not_null_possible  # only used for some dialects
        self.execute(
            ChangeColumnNullable(table_name, schema, key_columns, nullable=False)
        )
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    @engine_dispatch
    def add_index(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        self.execute(AddIndex(table_name, schema, index_columns, name))

    @add_primary_key.dialect("mssql")
    def _add_primary_key_mssql(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        *,
        name: str | None = None,
        early_not_null_possible: bool = False,
    ):
        _ = early_not_null_possible  # not needed for this dialect
        sql_types = self.reflect_sql_types(key_columns, table_name, schema)
        # impose some varchar(max) limit to allow use in primary key / index
        # TODO: consider making cap_varchar_max a config option
        self.execute(
            ChangeColumnTypes(
                table_name,
                schema,
                key_columns,
                sql_types,
                nullable=False,
                cap_varchar_max=1024,
            )
        )
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    @add_index.dialect("mssql")
    def _add_index_mssql(
        self, table_name: str, schema: Schema, index: list[str], name: str | None = None
    ):
        sql_types = self.reflect_sql_types(index, table_name, schema)
        if any(
            [
                isinstance(_type, sa.String) and _type.length is None
                for _type in sql_types
            ]
        ):
            # impose some varchar(max) limit to allow use in primary key / index
            self.execute(
                ChangeColumnTypes(
                    table_name, schema, index, sql_types, cap_varchar_max=1024
                )
            )
        self.execute(AddIndex(table_name, schema, index, name))

    @add_primary_key.dialect("ibm_db_sa")
    def _add_primary_key_ibm_db_sa(
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

    # @add_index.dialect("ibm_db_sa")
    # def _add_index_ibm_db_sa(
    #     self, table_name: str, schema: Schema, index: list[str],
    #     name: str | None = None
    # ):
    #     self.execute(AddIndex(table_name, schema, index, name))

    def copy_indexes(
        self, src_table: str, src_schema: Schema, dest_table: str, dest_schema: Schema
    ):
        inspector = sa.inspect(self.engine)
        pk_constraint = inspector.get_pk_constraint(src_table, schema=src_schema.get())
        if len(pk_constraint["constrained_columns"]) > 0:
            self.add_primary_key(
                dest_table,
                dest_schema,
                pk_constraint["constrained_columns"],
                name=pk_constraint["name"],
            )
        indexes = inspector.get_indexes(src_table, schema=src_schema.get())
        for index in indexes:
            if len(index["include_columns"]) > 0:
                self.logger.warning(
                    "Perfect index recreation in caching is not net implemented",
                    table=dest_table,
                    schema=dest_schema.get(),
                    include_columns=index["include_columns"],
                )
            if any(
                not isinstance(val, list) or len(val) > 0
                for val in index["dialect_options"].values()
            ):
                self.logger.warning(
                    "Perfect index recreation in caching is not net implemented",
                    table=dest_table,
                    schema=dest_schema.get(),
                    dialect_options=index["dialect_options"],
                )
            self.add_index(
                dest_table, dest_schema, index["column_names"], index["name"]
            )

    def reflect_sql_types(
        self, col_names: Iterable[str], table_name: str, schema: Schema
    ):
        inspector = sa.inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=schema.get())
        types = {d["name"]: d["type"] for d in columns}
        sql_types = [types[col] for col in col_names]
        return sql_types

    @staticmethod
    def format_sql_string(query_str: str) -> str:
        return textwrap.dedent(query_str).strip()

    @engine_dispatch
    def execute_raw_sql(self, raw_sql: RawSql):
        """Executed raw SQL statements in the associated transaction stage"""
        for statement in raw_sql.sql.split(";"):
            self.execute(statement)

    @execute_raw_sql.dialect("mssql")
    def _execute_raw_sql_mssql(self, raw_sql: RawSql):
        if self.disable_pytsql:
            return self._execute_raw_sql_mssql_fallback(raw_sql)
        return self._execute_raw_sql_mssql_pytsql(raw_sql)

    def _execute_raw_sql_mssql_fallback(self, raw_sql: RawSql):
        """Alternative to using pytsql for splitting SQL statement.

        Known Problems:
        - DECLARE statements will not be available across GO
        """
        last_use_statement = None
        for statement in re.split(r"\bGO\b", raw_sql.sql, flags=re.IGNORECASE):
            statement = statement.strip()
            if statement == "":
                # allow GO at end of script
                continue

            if re.match(r"\bUSE\b", statement):
                last_use_statement = statement

            with self.engine_connect() as conn:
                if last_use_statement is not None:
                    self.execute(last_use_statement, conn=conn)
                self.execute(statement, conn=conn)

    def _execute_raw_sql_mssql_pytsql(self, raw_sql: RawSql):
        """Use pytsql for executing T-SQL scripts containing many GO statements."""
        # noinspection PyUnresolvedReferences,PyPackageRequirements
        import pytsql

        sql_string = raw_sql.sql
        if self.print_sql:
            print_query_string = self.format_sql_string(sql_string)
            max_length = 5000
            if len(print_query_string) >= max_length:
                print_query_string = print_query_string[:max_length] + " [...]"
            self.logger.info(f"Executing sql:\n{print_query_string}")

        pytsql.executes(
            sql_string,
            self.engine,
            isolate_top_level_statements=self.pytsql_isolate_top_level_statements,
        )

    def setup(self):
        super().setup()
        if not self.avoid_drop_create_schema:
            self.execute(CreateSchema(self.metadata_schema, if_not_exists=True))
        with self.engine_connect() as conn:
            try:
                version = conn.execute(
                    sa.select(self.version_table.c.version)
                ).scalar_one_or_none()
            except sa.exc.ProgrammingError:
                # table metadata_version does not yet exist
                version = None
        if version is None:
            with self.engine_connect() as conn, conn.begin():
                self.sql_metadata.create_all(conn)
                version = conn.execute(
                    sa.select(self.version_table.c.version)
                ).scalar_one_or_none()
                assert version is None  # detect race condition
                conn.execute(
                    self.version_table.insert().values(
                        version=self.metadata_version,
                    )
                )
        elif version != self.metadata_version:
            # disable caching due to incompatible metadata table schemas
            # (in future versions, consider automatic metadata schema upgrade)
            self.disable_caching = True
            self.logger.warning(
                "Disabled caching due to metadata version mismatch",
                version=version,
                expected_version=self.metadata_version,
            )

    def dispose(self):
        self.engine.dispose()
        super().dispose()

    def init_stage(self, stage: Stage):
        stage_commit_technique = ConfigContext.get().stage_commit_technique
        if stage_commit_technique == StageCommitTechnique.SCHEMA_SWAP:
            return self._init_stage_schema_swap(stage)
        if stage_commit_technique == StageCommitTechnique.READ_VIEWS:
            return self._init_stage_read_views(stage)
        raise ValueError(f"Invalid stage commit technique: {stage_commit_technique}")

    @engine_dispatch
    def drop_all_dialect_specific(self, schema: Schema):
        pass

    @drop_all_dialect_specific.dialect("ibm_db_sa")
    def _drop_all_dialect_specific_ibm_db_sa(self, schema: Schema):
        with self.engine_connect() as conn:
            alias_names = conn.execute(
                sa.text(
                    "SELECT NAME FROM SYSIBM.SYSTABLES WHERE CREATOR ="
                    f" '{ibm_db_sa_fix_name(schema.get())}' and TYPE='A'"
                )
            ).all()
        alias_names = [row[0] for row in alias_names]
        for alias in alias_names:
            self.execute(DropAlias(alias, schema))

    @engine_dispatch
    def _init_stage_schema_swap(self, stage: Stage):
        schema = self.get_schema(stage.name)
        transaction_schema = self.get_schema(stage.transaction_name)

        cs_base = CreateSchema(schema, if_not_exists=True)
        ds_trans = DropSchema(
            transaction_schema, if_exists=True, cascade=True, engine=self.engine
        )
        cs_trans = CreateSchema(transaction_schema, if_not_exists=False)

        with self.engine_connect() as conn:
            if self.avoid_drop_create_schema:
                # empty tansaction_schema by deleting all tables and views
                # Attention: similar code in sql_ddl.py:visit_drop_schema_ibm_db_sa
                # for drop.cascade=True
                inspector = sa.inspect(self.engine)
                for table in inspector.get_table_names(schema=transaction_schema.get()):
                    self.execute(DropTable(table, schema=transaction_schema))
                for view in inspector.get_view_names(schema=transaction_schema.get()):
                    self.execute(DropView(view, schema=transaction_schema))
                self.drop_all_dialect_specific(schema=transaction_schema)
            else:
                self.execute(cs_base, conn=conn)
                self.execute(ds_trans, conn=conn)
                self.execute(cs_trans, conn=conn)

            if not self.disable_caching:
                for table in [
                    self.tasks_table,
                    self.lazy_cache_table,
                    self.raw_sql_cache_table,
                ]:
                    conn.execute(
                        table.delete()
                        .where(table.c.stage == stage.name)
                        .where(table.c.in_transaction_schema)
                    )

    @_init_stage_schema_swap.dialect("mssql")
    def _init_stage_schema_swap_mssql(self, stage: Stage):
        if "." not in self.schema_suffix:
            return self._init_stage_schema_swap.original(self, stage)

        schema = self.get_schema(stage.name)
        transaction_schema = self.get_schema(stage.transaction_name)

        cs_base = CreateSchema(schema, if_not_exists=True)
        cs_trans = CreateSchema(transaction_schema, if_not_exists=True)

        # TODO: detect whether tmp_schema exists and rename it as base or
        #       transaction schema if one is missing
        database, trans_schema_only = transaction_schema.get().split(".")

        # don't drop/create databases, just replace the schema underneath
        # (files will keep name on renaming)
        with self.engine_connect() as conn:
            if not self.avoid_drop_create_schema:
                self.execute(cs_base, conn=conn)
                self.execute(cs_trans, conn=conn)
            self.execute(f"USE [{database}]", conn=conn)
            # clear tables in schema
            self.execute(
                f"""
                EXEC sp_MSforeachtable
                  @command1 = 'DROP TABLE ?'
                , @whereand = 'AND SCHEMA_NAME(schema_id) = ''{trans_schema_only}'' '
                """,
                conn=conn,
            )

            # clear views and stored procedures
            for view in self.get_view_names(transaction_schema.get()):
                self.execute(
                    DropView(view, transaction_schema, if_exists=True), conn=conn
                )

            modules = self._get_mssql_sql_modules(transaction_schema.get())
            for name, _type in modules.items():
                _type = _type.strip()
                if _type == "P":
                    self.execute(
                        DropProcedure(name, transaction_schema, if_exists=True),
                        conn=conn,
                    )
                elif _type == "FN":
                    self.execute(
                        DropFunction(name, transaction_schema, if_exists=True),
                        conn=conn,
                    )

            synonyms = self._get_mssql_sql_synonyms(transaction_schema.get())
            for name in synonyms.keys():
                self.execute(
                    DropAlias(name, transaction_schema, if_exists=True),
                    conn=conn,
                )

            # Clear metadata
            if not self.disable_caching:
                for table in [
                    self.tasks_table,
                    self.lazy_cache_table,
                    self.raw_sql_cache_table,
                ]:
                    conn.execute(
                        table.delete()
                        .where(table.c.stage == stage.name)
                        .where(table.c.in_transaction_schema)
                    )

    def _init_stage_read_views(self, stage: Stage):
        try:
            with self.engine_connect() as conn:
                metadata_rows = (
                    conn.execute(
                        self.stage_table.select().where(
                            self.stage_table.c.stage == stage.name
                        )
                    )
                    .mappings()
                    .one()
                )
            cur_transaction_name = metadata_rows["cur_transaction_name"]
        except (sa.exc.MultipleResultsFound, sa.exc.NoResultFound):
            cur_transaction_name = ""

        suffix = "__even" if cur_transaction_name.endswith("__odd") else "__odd"
        new_transaction_name = stage.name + suffix

        # TODO: WE SHOULD NOT MODIFY THE STAGE AFTER CREATION!!!
        stage.set_transaction_name(new_transaction_name)

        # Call schema swap function with updated transaction name
        self._init_stage_schema_swap(stage)

    def commit_stage(self, stage: Stage):
        stage_commit_technique = ConfigContext.get().stage_commit_technique
        if stage_commit_technique == StageCommitTechnique.SCHEMA_SWAP:
            return self._commit_stage_schema_swap(stage)
        if stage_commit_technique == StageCommitTechnique.READ_VIEWS:
            return self._commit_stage_read_views(stage)
        raise ValueError(f"Invalid stage commit technique: {stage_commit_technique}")

    def _commit_stage_schema_swap(self, stage: Stage):
        schema = self.get_schema(stage.name)
        transaction_schema = self.get_schema(stage.transaction_name)
        tmp_schema = self.get_schema(stage.name + "__swap")

        # potentially this disposal must be optional since it does not allow
        # for multithreaded stage execution
        # dispose open connections which may prevent schema swapping
        self.engine.dispose()

        # We might want to also append ', conn.begin()' to the with statement,
        # but this collides with the inner conn.begin() used to execute
        # statement parts as one transaction. This is solvable via complex code,
        # but we typically don't want to rely on database transactions anyways.
        with self.engine_connect() as conn:
            # TODO: for mssql try to find schema does not exist and then move
            #       the forgotten tmp schema there
            assert not self.avoid_drop_create_schema, (
                "The option avoid_drop_create_schema is currently not supported in "
                "combination with dialect mssql"
            )
            self.execute(
                DropSchema(tmp_schema, if_exists=True, cascade=True), conn=conn
            )
            # TODO: in case "." is in self.schema_prefix, we need to implement
            #       schema renaming by creating the new schema and moving
            #       table objects over
            self.execute(RenameSchema(schema, tmp_schema), conn=conn)
            self.execute(RenameSchema(transaction_schema, schema), conn=conn)
            self.execute(RenameSchema(tmp_schema, transaction_schema), conn=conn)

            self._commit_stage_update_metadata(stage, conn=conn)

    def _commit_stage_read_views(self, stage: Stage):
        dest_schema = self.get_schema(stage.name)
        src_schema = self.get_schema(stage.transaction_name)

        with self.engine_connect() as conn:
            # Delete all read views in visible (destination) schema
            views = self.get_view_names(dest_schema.get())
            for view in views:
                self.execute(DropView(view, schema=dest_schema), conn=conn)
            self.drop_all_dialect_specific(schema=dest_schema)

            # Create views for all tables in transaction schema
            src_meta = sa.MetaData()
            src_meta.reflect(bind=self.engine, schema=src_schema.get())
            self._create_read_views(
                conn, dest_schema, src_schema, src_meta.tables.values()
            )

            # Update metadata
            stage_metadata_exists = (
                conn.execute(
                    sa.select(1).where(self.stage_table.c.stage == stage.name)
                ).scalar()
                == 1
            )

            if stage_metadata_exists:
                conn.execute(
                    self.stage_table.update()
                    .where(self.stage_table.c.stage == stage.name)
                    .values(cur_transaction_name=stage.transaction_name)
                )
            else:
                conn.execute(
                    self.stage_table.insert().values(
                        stage=stage.name,
                        cur_transaction_name=stage.transaction_name,
                    )
                )

            self._commit_stage_update_metadata(stage, conn=conn)

    @engine_dispatch
    def _create_read_views(
        self,
        conn,
        dest_schema: Schema,
        src_schema: Schema,
        src_tables: Iterable[sa.Table],
    ):
        _ = src_schema  # only needed by other dialects
        for table in src_tables:
            self.execute(
                CreateViewAsSelect(table.name, dest_schema, sa.select(table)),
                conn=conn,
            )

    @_create_read_views.dialect("ibm_db_sa")
    def _create_read_views_ibm_db_sa(
        self, conn, dest_schema, src_schema: Schema, src_tables: Iterable[sa.Table]
    ):
        # Instead of views create aliases which can be locked like tables and
        # have primary keys
        for table in src_tables:
            self.execute(
                CreateAlias(table.name, src_schema, table.name, dest_schema),
                conn=conn,
            )

    def _commit_stage_update_metadata(self, stage: Stage, conn: sa.engine.Connection):
        if not self.disable_caching:
            for table in [
                self.tasks_table,
                self.lazy_cache_table,
                self.raw_sql_cache_table,
            ]:
                conn.execute(
                    table.delete()
                    .where(table.c.stage == stage.name)
                    .where(~table.c.in_transaction_schema)
                )
                conn.execute(
                    table.update()
                    .where(table.c.stage == stage.name)
                    .values(in_transaction_schema=False)
                )

    def copy_table_to_transaction(self, table: Table):
        stage = table.stage
        schema_name = self.get_schema(stage.name).get()
        has_table = sa.inspect(self.engine).has_table(table.name, schema=schema_name)
        if not has_table:
            raise RuntimeError(
                f"Can't copy table '{table.name}' (schema: '{stage.name}')"
                " to transaction because no such table exists."
            )

        try:
            self.execute(
                CopyTable(
                    table.name,
                    self.get_schema(stage.name),
                    table.name,
                    self.get_schema(stage.transaction_name),
                )
            )
        except Exception as _e:
            msg = (
                f"Failed to copy table '{table.name}' (schema: '{stage.name}')"
                " to transaction."
            )
            self.logger.error(
                msg,
                exception=traceback.format_exc(),
            )
            raise RuntimeError(msg) from _e
        self.add_indexes(table, self.get_schema(stage.transaction_name))

    def deferred_copy_lazy_table_to_transaction(
        self, src_name: str, src_stage: Stage, table: Table
    ):
        stage = table.stage
        src_schema = self.get_schema(src_stage)
        dest_schema = self.get_schema(stage.transaction_name)
        thread_id = human_thread_id(threading.get_ident())
        try:
            self.logger.info(
                "Running table copy in background",
                thread=thread_id,
                table=src_name,
                src_schema=src_schema.get(),
                dest_schema=dest_schema.get(),
            )
            self.execute(
                CopyTable(
                    src_name,
                    src_schema,
                    "__cpy_" + src_name,
                    dest_schema,
                    early_not_null=table.primary_key,
                )
            )
        except Exception as _e:
            msg = (
                f"Failed to copy lazy table {src_name} (schema:"
                f" '{src_schema}' -> '{dest_schema}') to transaction."
            )
            self.logger.error(
                "Exception in table copy in background",
                thread=thread_id,
                table=src_name,
                msg=msg,
                exception=str(_e),
            )
            raise RuntimeError(msg) from _e
        table = copy.deepcopy(table)
        table.name = "__cpy_" + src_name
        self.add_indexes(
            table, self.get_schema(stage.transaction_name), early_not_null_possible=True
        )
        self.logger.info(
            "Completed table copy in background",
            thread=thread_id,
            table=src_name,
            src_schema=src_schema.get(),
            dest_schema=dest_schema.get(),
        )

    def swap_alias_and_copied_table(
        self, src_name: str, src_stage: Stage, table: Table
    ):
        stage = table.stage
        dest_schema = self.get_schema(stage.transaction_name)
        try:
            self.execute(
                DropAlias(
                    src_name,
                    dest_schema,
                )
            )
            self.execute(
                RenameTable(
                    "__cpy_" + src_name,
                    src_name,
                    dest_schema,
                )
            )
        except Exception as _e:
            msg = (
                f"Failed putting copied lazy table (__cpy_){src_name} (schema:"
                f" '{dest_schema.get()}') in place of alias."
            )
            raise RuntimeError(msg) from _e

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        stage = table.stage
        schema_name = self.get_schema(metadata.stage).get()
        has_table = sa.inspect(self.engine).has_table(metadata.name, schema=schema_name)
        if not has_table:
            msg = (
                f"Can't copy lazy table '{metadata.name}' (schema:"
                f" '{metadata.stage}') to transaction because no such table"
                " exists."
            )
            self.logger.error(msg)
            raise RuntimeError(msg)

        try:
            self.execute(
                CreateAlias(
                    metadata.name,
                    self.get_schema(metadata.stage),
                    metadata.name,
                    self.get_schema(stage.transaction_name),
                )
            )
            RunContext.get().deferred_table_store_op_if_stage_invalid(
                stage,
                "deferred_copy_lazy_table_to_transaction",
                "swap_alias_and_copied_table",
                metadata.name,
                metadata.stage,
                table,
            )
        except Exception as _e:
            msg = (
                f"Failed to copy lazy table {metadata.name} (schema:"
                f" '{metadata.stage}') to transaction."
            )
            self.logger.error(msg, exception=traceback.format_exc())
            raise RuntimeError(msg) from _e

    @engine_dispatch
    def get_view_names(self, schema: str, *, include_everything=False) -> list[str]:
        """
        List all views that are present in a schema.

        It may also include other database objects like stored procedures, functions,
        etc. which makes the name `get_view_names` too specific. But sqlalchemy
        only allows reading views for all dialects thus the storyline of dialect
        agnostic callers is much nicer to read. In the end we might need everything
        to recover the full cache output which was produced by a RawSQL statement
        (we want to be compatible with legacy sql code as a starting point).
        :param schema: the schema
        :param include_everything: If True, we might include stored procedures,
            functions and other database objects that have a schema associated name.
            Currently, this only makes a difference for dialect=mssql.
        :return: list of view names [and other objects]
        """
        _ = include_everything  # not used in this implementation
        inspector = sa.inspect(self.engine)
        return inspector.get_view_names(schema)

    @get_view_names.dialect("mssql")
    def _get_view_names_mssql(self, schema: str, *, include_everything=False):
        if not include_everything:
            return self.get_view_names.original(
                self, schema, include_everything=include_everything
            )
        return list(self._get_mssql_sql_modules(schema).keys())

    # noinspection SqlDialectInspection
    def _get_mssql_sql_modules(self, schema: str):
        with self.engine_connect() as conn:
            database, schema_only = schema.split(".")
            self.execute(f"USE {database}", conn=conn)
            # include stored procedures in view names because they can also be recreated
            # based on sys.sql_modules.description
            sql = (
                "select obj.name, obj.type from sys.sql_modules as mod "
                "left join sys.objects as obj on mod.object_id=obj.object_id "
                "left join sys.schemas as schem on schem.schema_id=obj.schema_id "
                f"where schem.name='{schema_only}'"
            )
            rows = self.execute(sql, conn=conn).fetchall()
        return {row[0]: row[1] for row in rows}

    # noinspection SqlDialectInspection
    def _get_mssql_sql_synonyms(self, schema: str):
        with self.engine_connect() as conn:
            database, schema_only = schema.split(".")
            self.execute(f"USE {database}", conn=conn)
            # include stored procedures in view names because they can also be recreated
            # based on sys.sql_modules.description
            sql = (
                "select syn.name, syn.type from sys.synonyms as syn "
                "left join sys.schemas as schem on schem.schema_id=syn.schema_id "
                f"where schem.name='{schema_only}'"
            )
            rows = self.execute(sql, conn=conn).fetchall()
        return {row[0]: row[1] for row in rows}

    def copy_raw_sql_tables_to_transaction(
        self, metadata: RawSqlMetadata, target_stage: Stage
    ):
        src_schema = self.get_schema(metadata.stage)
        dest_schema = self.get_schema(target_stage.transaction_name)

        views = set(self.get_view_names(src_schema.get(), include_everything=True))
        new_tables = set(metadata.tables) - set(metadata.prev_tables)

        tables_to_copy = new_tables - set(views)
        for table_name in tables_to_copy:
            try:
                # TODO: implement deferred execution in RunContextServer so
                # no data is copied if a stage is 100% cache valid
                self.execute(
                    CopyTable(
                        table_name,
                        src_schema,
                        table_name,
                        dest_schema,
                    )
                )
                self.copy_indexes(
                    table_name,
                    src_schema,
                    table_name,
                    dest_schema,
                )
            except Exception as _e:
                msg = (
                    f"Failed to copy raw sql generated table {table_name} (schema:"
                    f" '{src_schema}') to transaction."
                )
                self.logger.error(
                    msg,
                    exception=traceback.format_exc(),
                )
                raise RuntimeError(msg) from _e

        views_to_copy = new_tables & set(views)
        for view_name in views_to_copy:
            self._copy_view_to_transaction(view_name, src_schema, dest_schema)

    @engine_dispatch
    def _copy_view_to_transaction(
        self, view_name: str, src_schema: Schema, dest_schema: Schema
    ):
        raise NotImplementedError("Only implemented for mssql.")

    # noinspection SqlDialectInspection
    @_copy_view_to_transaction.dialect("mssql")
    def _copy_view_to_transaction_mssql(
        self, view_name: str, src_schema: Schema, dest_schema: Schema
    ):
        src_database, src_schema_only = src_schema.get().split(".")
        dest_database, dest_schema_only = dest_schema.get().split(".")

        with self.engine_connect() as conn:
            self.execute(f"USE {src_database}", conn=conn)
            view_sql = self.execute(
                f"""
                SELECT definition
                FROM sys.sql_modules
                WHERE [object_id] = OBJECT_ID('[{src_schema_only}].[{view_name}]');
                """,
                conn=conn,
            ).first()

            if view_sql is None:
                msg = f"No SQL query associated with view {view_name} found."
                raise ValueError(msg)
            view_sql = view_sql[0]

            # Update view SQL so that it references the destination schema
            if src_schema_only != dest_schema_only:
                names_product = itertools.product(
                    [src_schema_only, f"[{src_schema_only}]"],
                    [view_name, f"[{view_name}]"],
                )
                for schema, view in names_product:
                    view_sql.replace(
                        f"{schema}.{view}",
                        f"[{dest_schema_only}].[{view_name}]",
                    )

            self.execute(f"USE {dest_database}", conn=conn)
            self.execute(view_sql, conn=conn)

    def delete_table_from_transaction(self, table: Table):
        self.execute(
            DropTable(
                table.name,
                self.get_schema(table.stage.transaction_name),
                if_exists=True,
            )
        )

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        if not self.disable_caching:
            with self.engine_connect() as conn:
                conn.execute(
                    self.tasks_table.insert().values(
                        name=metadata.name,
                        stage=metadata.stage,
                        version=metadata.version,
                        timestamp=metadata.timestamp,
                        run_id=metadata.run_id,
                        input_hash=metadata.input_hash,
                        cache_fn_hash=metadata.cache_fn_hash,
                        output_json=metadata.output_json,
                        in_transaction_schema=True,
                    )
                )

    def retrieve_task_metadata(
        self, task: MaterializingTask, input_hash: str, cache_fn_hash: str
    ) -> TaskMetadata:
        if self.disable_caching:
            raise CacheError(
                "Caching is disabled, so we also don't even try to retrieve task"
                f" cache: {task}"
            )

        ignore_fresh_input = RunContext.get().ignore_fresh_input
        try:
            with self.engine_connect() as conn:
                result = (
                    conn.execute(
                        self.tasks_table.select()
                        .where(self.tasks_table.c.name == task.name)
                        .where(self.tasks_table.c.stage == task.stage.name)
                        .where(self.tasks_table.c.version == task.version)
                        .where(self.tasks_table.c.input_hash == input_hash)
                        .where(
                            self.tasks_table.c.cache_fn_hash == cache_fn_hash
                            if not ignore_fresh_input
                            else sa.literal(True)
                        )
                        .where(self.tasks_table.c.in_transaction_schema.in_([False]))
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise RuntimeError("Multiple results found task metadata") from None

        if result is None:
            raise CacheError(f"Couldn't retrieve task from cache: {task}")

        return TaskMetadata(
            name=result.name,
            stage=result.stage,
            version=result.version,
            timestamp=result.timestamp,
            run_id=result.run_id,
            input_hash=result.input_hash,
            cache_fn_hash=result.cache_fn_hash,
            output_json=result.output_json,
        )

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        if not self.disable_caching:
            with self.engine_connect() as conn:
                conn.execute(
                    self.lazy_cache_table.insert().values(
                        name=metadata.name,
                        stage=metadata.stage,
                        query_hash=metadata.query_hash,
                        task_hash=metadata.task_hash,
                        in_transaction_schema=True,
                    )
                )

    # noinspection DuplicatedCode
    def retrieve_lazy_table_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> LazyTableMetadata:
        if self.disable_caching:
            raise CacheError(
                "Caching is disabled, so we also don't even try to retrieve lazy table"
                " cache"
            )

        try:
            with self.engine_connect() as conn:
                result = (
                    conn.execute(
                        self.lazy_cache_table.select()
                        .where(self.lazy_cache_table.c.stage == stage.name)
                        .where(self.lazy_cache_table.c.query_hash == query_hash)
                        .where(self.lazy_cache_table.c.task_hash == task_hash)
                        .where(
                            self.lazy_cache_table.c.in_transaction_schema.in_([False])
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise RuntimeError(
                "Multiple results found for lazy table cache key"
            ) from None

        if result is None:
            raise CacheError("No result found for lazy table cache key")

        return LazyTableMetadata(
            name=result.name,
            stage=result.stage,
            query_hash=result.query_hash,
            task_hash=result.task_hash,
        )

    def store_raw_sql_metadata(self, metadata: RawSqlMetadata):
        if not self.disable_caching:
            with self.engine_connect() as conn:
                conn.execute(
                    self.raw_sql_cache_table.insert().values(
                        prev_tables=json.dumps(metadata.prev_tables),
                        tables=json.dumps(metadata.tables),
                        stage=metadata.stage,
                        query_hash=metadata.query_hash,
                        task_hash=metadata.task_hash,
                        in_transaction_schema=True,
                    )
                )

    # noinspection DuplicatedCode
    def retrieve_raw_sql_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> RawSqlMetadata:
        if self.disable_caching:
            raise CacheError(
                "Caching is disabled, so we also don't even try to retrieve raw sql"
                " cache"
            )

        try:
            with self.engine_connect() as conn:
                result = (
                    conn.execute(
                        self.raw_sql_cache_table.select()
                        .where(self.raw_sql_cache_table.c.stage == stage.name)
                        .where(self.raw_sql_cache_table.c.query_hash == query_hash)
                        .where(self.raw_sql_cache_table.c.task_hash == task_hash)
                        .where(
                            self.raw_sql_cache_table.c.in_transaction_schema.in_(
                                [False]
                            )
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except sa.exc.MultipleResultsFound:
            raise RuntimeError("Multiple results found for raw sql cache key") from None

        if result is None:
            raise CacheError("No result found for raw sql cache key")

        return RawSqlMetadata(
            prev_tables=json.loads(result.prev_tables),
            tables=json.loads(result.tables),
            stage=result.stage,
            query_hash=result.query_hash,
            task_hash=result.task_hash,
        )

    @engine_dispatch
    def resolve_aliases(self, table_name, schema):
        return table_name, schema

    @resolve_aliases.dialect("ibm_db_sa")
    def resolve_aliases_ibm_db_sa(self, table_name, schema):
        with self.engine_connect() as conn:
            return _resolve_alias_ibm_db_sa(conn, table_name, schema)

    @resolve_aliases.dialect("mssql")
    def resolve_aliases_mssql(self, table_name, schema):
        with self.engine_connect() as conn:
            return _resolve_alias_mssql(conn, table_name, schema)

    def list_tables(self, stage: Stage, *, include_everything=False):
        """
        List all tables that were generated in a stage.

        It may also include other objects database objects like views, stored
        procedures, functions, etc. which makes the name `list_tables` too specific.
        But the predominant idea is that tasks produce tables in stages and thus the
        storyline of callers is much nicer to read. In the end we might need everything
        to recover the full cache output which was produced by a RawSQL statement
        (we want to be compatible with legacy sql code as a starting point).

        :param stage: the stage
        :param include_everything: If True, we might include stored procedures,
            functions and other database objects that have a schema associated name.
        :return: list of tables [and other objects]
        """
        inspector = sa.inspect(self.engine)
        schema = self.get_schema(stage.transaction_name).get()

        table_names = inspector.get_table_names(schema)
        view_names = self.get_view_names(schema, include_everything=include_everything)

        return table_names + view_names

    def get_stage_hash(self, stage: Stage) -> str:
        """Compute hash that represents entire stage's output metadata."""
        # We only need to look in tasks_table since the output_json column is updated
        # after evaluating lazy output objects for cache validity. This is the same
        # information we use for producing input_hash of downstream tasks.
        if self.disable_caching:
            raise NotImplementedError(
                "computing stage hash with disabled caching is currently not supported"
            )
        with self.engine_connect() as conn:
            result = conn.execute(
                sa.select(self.tasks_table.c.output_json)
                .where(self.tasks_table.c.stage == stage.name)
                .where(self.tasks_table.c.in_transaction_schema.in_([False]))
                .order_by(self.tasks_table.c.output_json)
            ).all()
            result = [row[0] for row in result]
        return compute_cache_key(*result)


@SQLTableStore.register_table()
class SQLAlchemyTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(
            type_, (sa.sql.elements.TextClause, sa.sql.selectable.Selectable)
        )

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == sa.Table

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[sa.sql.elements.TextClause | sa.Text],
        stage_name,
        task_info: TaskInfo,
    ):
        obj = table.obj
        if isinstance(table.obj, sa.Table):
            obj = sa.select(table.obj)

        source_tables = [
            dict(
                name=tbl.name,
                schema=store.get_schema(
                    tbl.stage.transaction_name
                    if tbl.stage in task_info.open_stages
                    else tbl.stage.name
                ).get(),
            )
            for tbl in task_info.input_tables
        ]
        schema = store.get_schema(stage_name)
        store.execute(
            CreateTableAsSelect(
                table.name,
                schema,
                obj,
                early_not_null=table.primary_key,
                source_tables=source_tables,
            )
        )
        store.add_indexes(table, schema, early_not_null_possible=True)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        table_name = table.name
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_aliases(table_name, schema)
        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                tbl = sa.Table(
                    table_name,
                    sa.MetaData(),
                    schema=schema,
                    autoload_with=store.engine,
                )
                break
            except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                if retry_iteration == 3:
                    raise
                time.sleep(retry_iteration * retry_iteration * 1.2)
        return tbl

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        if isinstance(obj, sa.sql.selectable.FromClause):
            query = sa.select(sa.text("*")).select_from(obj)
        else:
            query = obj
        query_str = str(
            query.compile(store.engine, compile_kwargs={"literal_binds": True})
        )
        # hacky way to canonicalize query (despite __tmp/__even/__odd suffixes
        # and alias resolution)
        query_str = re.sub(r'["\[\]]', "", query_str)
        query_str = re.sub(
            r'(__tmp|__even|__odd)(?=[ \t\n.;"]|$)', "", query_str.lower()
        )
        return query_str


def _resolve_alias_ibm_db_sa(conn, table_name: str, schema: str, *, _iteration=0):
    # TODO: consider speeding this up by caching information for complete schemas
    #  instead of running at least two queries per source table; Ultimately problem can
    #  also be fixed by upstream contribution to ibm_db_sa

    # we need to resolve aliases since pandas.read_sql_table does not work for them
    # and sa.Table does not auto-reflect columns
    table_name = ibm_db_sa_fix_name(table_name)
    schema = ibm_db_sa_fix_name(schema)
    tbl = sa.Table("SYSTABLES", sa.MetaData(), schema="SYSIBM", autoload_with=conn)
    query = (
        sa_select([tbl.c.base_name, tbl.c.base_schema])
        .select_from(tbl)
        .where(
            (tbl.c.creator == schema) & (tbl.c.name == table_name) & (tbl.c.TYPE == "A")
        )
    )
    for retry_iteration in range(4):
        # retry operation since it might have been terminated as a deadlock victim
        try:
            row = conn.execute(query).mappings().one_or_none()
            break
        except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
            if retry_iteration == 3:
                raise
            time.sleep(retry_iteration * retry_iteration)
    if row is not None:
        assert _iteration < 3, f"Unexpected recursion looking up {schema}.{table_name}"
        table_name, schema = _resolve_alias_ibm_db_sa(
            conn, row["base_name"], row["base_schema"], _iteration=_iteration + 1
        )
    return table_name, schema


def _resolve_alias_mssql(conn, table_name: str, schema: str, *, _iteration=0):
    # TODO: consider speeding this up by caching information for complete schemas
    #  instead of running at least two queries per source table; Ultimately problem can
    #  also be fixed by upstream contribution to ibm_db_sa

    # we need to resolve aliases since pandas.read_sql_table does not work for them
    # and sa.Table does not auto-reflect columns
    database, schema_only = schema.split(".")
    conn.execute(sa.text(f"USE [{database}]"))
    # include stored procedures in view names because they can also be recreated
    # based on sys.sql_modules.description
    sql = (
        "select syn.base_object_name from sys.synonyms as syn left join sys.schemas as"
        f" schem on schem.schema_id=syn.schema_id where schem.name='{schema_only}' and"
        f" syn.name='{table_name}' and syn.type='SN'"
    )
    row = conn.execute(sa.text(sql)).mappings().one_or_none()
    if row is not None:
        parts = row["base_object_name"].split(".")
        assert len(parts) == 3, (
            "Unexpected number of '.' delimited parts when looking up synonym for "
            f"{schema}.{table_name}: {row['base_object_name']}"
        )
        assert _iteration < 3, f"Unexpected recursion looking up {schema}.{table_name}"
        parts = [
            part[1:-1] if part.startswith("[") and part.endswith("]") else part
            for part in parts
        ]
        schema = ".".join(parts[0:2])
        table_name = parts[2]
        table_name, schema = _resolve_alias_mssql(
            conn, table_name, schema, _iteration=_iteration + 1
        )
    return table_name, schema


def adj_pandas_types(df: pd.DataFrame):
    df = df.copy()
    for col in df.dtypes.loc[lambda x: x == int].index:
        df[col] = df[col].astype(pd.Int64Dtype())
    for col in df.dtypes.loc[lambda x: x == bool].index:
        df[col] = df[col].astype(pd.BooleanDtype())
    for col in df.dtypes.loc[lambda x: x == object].index:
        df[col] = df[col].astype(pd.StringDtype())
    return df


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
    """
    Materalize pandas->sql and retrieve sql->pandas.

    We like to limit the use of types, so operations can be implemented reliably
    without the use of pandas dtype=object where every row can have different type:
      - string/varchar/clob: pd.StringDType
      - date: datetime64[ns] (we cap year in 1900..2199 and save original year
            in separate column)
      - datetime: datetime64[ns] (we cap year in 1900..2199 and save original year
            in separate column)
      - int: pd.Int64DType
      - boolean: pd.BooleanDType

    Dialect specific aspects:
      * ibm_db_sa:
        - DB2 does not support boolean type -> integer
        - We limit string columns to 256 characters since larger columns have
          trouble with adding indexes/primary keys
    """

    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pd.DataFrame],
        stage_name,
        task_info: TaskInfo,
    ):
        # we might try to avoid this copy for speedup / saving RAM
        df = table.obj.copy()
        schema = store.get_schema(stage_name)
        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}':\n{table.obj}"
            )
        dtype_map = {}
        table_name = table.name
        str_type = sa.String()
        if store.engine.dialect.name == "ibm_db_sa":
            # Default string target is CLOB which can't be used for indexing.
            # We could use VARCHAR(32000), but if we change this to VARCHAR(256)
            # for indexed columns, DB2 hangs.
            str_type = sa.VARCHAR(256)
            table_name = ibm_db_sa_fix_name(table_name)
        # force dtype=object to string (we require dates to be stored in datetime64[ns])
        for col in df.dtypes.loc[lambda x: x == object].index:
            if table.type_map is None or col not in table.type_map:
                df[col] = df[col].astype(str)
                dtype_map[col] = str_type
        for col in df.dtypes.loc[
            lambda x: (x != object) & x.isin([pd.Int32Dtype, pd.UInt16Dtype])
        ].index:
            dtype_map[col] = sa.INTEGER()
        for col in df.dtypes.loc[
            lambda x: (x != object)
            & x.isin([pd.Int16Dtype, pd.Int8Dtype, pd.UInt8Dtype])
        ].index:
            dtype_map[col] = sa.SMALLINT()
        if table.type_map is not None:
            dtype_map.update(table.type_map)
        # Todo: do better abstraction of dialect specific code
        if store.engine.dialect.name == "mssql":
            for col, _type in dtype_map.items():
                if _type == sa.DateTime:
                    dtype_map[col] = DATETIME2()
        df.to_sql(
            table_name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtype_map,
            chunksize=100_000,
        )
        store.add_indexes(table, schema)

    @staticmethod
    def _pandas_type(sql_type):
        if isinstance(sql_type, sa.SmallInteger):
            return pd.Int16Dtype()
        elif isinstance(sql_type, sa.BigInteger):
            return pd.Int64Dtype()
        elif isinstance(sql_type, sa.Integer):
            return pd.Int32Dtype()
        elif isinstance(sql_type, sa.Boolean):
            return pd.BooleanDtype()
        elif isinstance(sql_type, sa.String):
            return pd.StringDtype()
        elif isinstance(sql_type, sa.Date):
            return "datetime64[ns]"
        elif isinstance(sql_type, sa.DateTime):
            return "datetime64[ns]"

    @staticmethod
    def _fix_cols(cols: dict[str, Any], dialect_name):
        cols = cols.copy()
        year_cols = []
        min_date = "1900-01-01"
        max_date = "2199-12-31"
        for name, col in list(cols.items()):
            if isinstance(col.type, sa.Date) or isinstance(col.type, sa.DateTime):
                if name + "_year" not in cols:
                    cols[name + "_year"] = sa.cast(
                        sa.func.extract("year", cols[name]), sa.Integer
                    ).label(name + "_year")
                    year_cols.append(name + "_year")
                # Todo: do better abstraction of dialect specific code
                if dialect_name == "mssql":
                    cap_min = sa.func.iif(cols[name] < min_date, min_date, cols[name])
                    cols[name] = sa.func.iif(
                        cap_min > max_date, max_date, cap_min
                    ).label(name)
                else:
                    cols[name] = sa.func.least(
                        max_date, sa.func.greatest(min_date, cols[name])
                    ).label(name)
        return cols, year_cols

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        schema = store.get_schema(stage_name).get()
        with store.engine.connect() as conn:
            table_name = table.name
            table_name, schema = store.resolve_aliases(table_name, schema)
            for retry_iteration in range(4):
                # retry operation since it might have been terminated as a
                # deadlock victim
                try:
                    sql_table = sa.Table(
                        table_name,
                        sa.MetaData(),
                        schema=schema,
                        autoload_with=store.engine,
                    ).alias("tbl")
                    dtype_map = {c.name: cls._pandas_type(c.type) for c in sql_table.c}
                    cols, year_cols = cls._fix_cols(
                        {c.name: c for c in sql_table.c}, store.engine.dialect.name
                    )
                    dtype_map.update({col: pd.Int16Dtype() for col in year_cols})
                    query = sa_select(cols.values()).select_from(sql_table)
                    try:
                        # works only from pandas 2.0.0 on
                        df = pd.read_sql(
                            query,
                            con=conn,
                            dtype=dtype_map,
                        )
                    except TypeError:
                        df = pd.read_sql(query, con=conn)
                        for col, _type in dtype_map.items():
                            df[col] = df[col].astype(_type)
                    break
                except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                    if retry_iteration == 3:
                        raise
                    time.sleep(retry_iteration * retry_iteration * 1.3)
            return df

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


try:
    # optional dependency to polars
    import connectorx
    import polars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    polars = None
    connectorx = None


@SQLTableStore.register_table(polars, connectorx)
class PolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        # attention: tidypolars.Tibble is subclass of polars DataFrame
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[polars.dataframe.DataFrame],
        stage_name,
        task_info: TaskInfo,
    ):
        schema = store.get_schema(stage_name)
        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}':\n{table.obj}"
            )
        table_name = table.name
        # TODO:
        # dtype_map = {}
        # if store.engine.dialect.name == "ibm_db_sa":
        #     # Default string target is CLOB which can't be used for indexing.
        #     # We could use VARCHAR(32000), but if we change this to VARCHAR(256)
        #     # for indexed columns, DB2 hangs.
        #     dtype_map = {
        #         col: sa.VARCHAR(256)
        #         for col in table.obj.dtypes.loc[lambda x: x == object].index
        #     }
        #     table_name = ibm_db_sa_fix_name(table_name)
        table.obj.to_pandas(use_pyarrow_extension_array=True).to_sql(
            name=table_name, con=store.engine, schema=schema.get(), index=False
        )
        store.add_indexes(table, schema)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        schema = store.get_schema(stage_name).get()
        table_name = table.name
        table_name, schema = store.resolve_aliases(table_name, schema)
        conn = str(store.engine_url_obj)
        df = polars.read_database(f'SELECT * FROM {schema}."{table_name}"', conn)
        return df

    @classmethod
    def auto_table(cls, obj: polars.dataframe.DataFrame):
        return super().auto_table(obj)


try:
    # optional dependency to polars
    import tidypolars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    tidypolars = None


@SQLTableStore.register_table(tidypolars, polars, connectorx)
class TidyPolarsTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, tidypolars.Tibble)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == tidypolars.Tibble

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[tidypolars.Tibble],
        stage_name,
        task_info: TaskInfo,
    ):
        table = copy.deepcopy(table)
        table.obj = table.obj.to_polars()
        PolarsTableHook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        df = PolarsTableHook.retrieve(store, table, stage_name, as_type)
        return tidypolars.from_polars(df)

    @classmethod
    def auto_table(cls, obj: tidypolars.Tibble):
        return super().auto_table(obj)


try:
    # optional dependency to pydiverse-transform
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = None


# noinspection PyUnresolvedReferences, PyProtectedMember
@SQLTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pdt.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        return issubclass(type_, (PandasTableImpl, SQLTableImpl))

    @classmethod
    def materialize(
        cls, store, table: Table[pdt.Table], stage_name, task_info: TaskInfo
    ):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            # noinspection PyTypeChecker
            return PandasTableHook.materialize(store, table, stage_name, task_info)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            # noinspection PyTypeChecker
            return SQLAlchemyTableHook.materialize(store, table, stage_name, task_info)
        raise NotImplementedError

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        if issubclass(as_type, PandasTableImpl):
            df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))
        if issubclass(as_type, SQLTableImpl):
            sa_tbl = SQLAlchemyTableHook.retrieve(store, table, stage_name, sa.Table)
            return pdt.Table(SQLTableImpl(store.engine, sa_tbl))
        raise NotImplementedError

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        return Table(obj, obj._impl.name)

    @classmethod
    def lazy_query_str(cls, store, obj: pdt.Table) -> str:
        from pydiverse.transform.core.verbs import build_query

        query = obj >> build_query()

        if query is not None:
            return str(query)
        return super().lazy_query_str(store, obj)


try:
    # optional dependency to ibis
    import ibis
    import ibis.expr.types as ibis_types
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    ibis = None
    ibis_types = None


@SQLTableStore.register_table(ibis)
class IbisTableHook(TableHook[SQLTableStore]):
    @staticmethod
    def get_con(store):
        # TODO: move this function to a better ibis specific place
        con = store.engines_other.get("ibis")
        if con is None:
            url = store.engine_url_obj
            dialect = store.engine.dialect.name
            if dialect == "postgresql":
                con = ibis.postgres.connect(
                    host=url.host,
                    user=url.username,
                    password=url.password,
                    port=url.port,
                    database=url.database,
                )
            elif dialect == "mssql":
                con = ibis.mssql.connect(
                    host=url.host,
                    user=url.username,
                    password=url.password,
                    port=url.port,
                    database=url.database,
                )
            else:
                raise RuntimeError(
                    f"initializing ibis for {dialect} is not supported, yet "
                    "-- supported: postgresql, mssql"
                )
            store.engines_other["ibis"] = con
        return con

    @classmethod
    def can_materialize(cls, type_) -> bool:
        # Operations on a table like mutate() or join() don't change the type
        return issubclass(type_, ibis_types.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return issubclass(type_, ibis_types.Table)

    @classmethod
    def materialize(
        cls, store, table: Table[ibis_types.Table], stage_name, task_info: TaskInfo
    ):
        t = table.obj
        table.obj = sa.text(cls.lazy_query_str(store, t))
        return SQLAlchemyTableHook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        con = cls.get_con(store)
        table_name = table.name
        schema = store.get_schema(stage_name).get()
        table_name, schema = store.resolve_aliases(table_name, schema)
        for retry_iteration in range(4):
            # retry operation since it might have been terminated as a deadlock victim
            try:
                tbl = con.table(
                    table_name,
                    schema=schema,
                )
                break
            except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError):
                if retry_iteration == 3:
                    raise
                time.sleep(retry_iteration * retry_iteration * 1.2)
        return tbl

    @classmethod
    def auto_table(cls, obj: ibis_types.Table):
        if obj.has_name():
            return Table(obj, obj.get_name())
        else:
            return super().auto_table(obj)

    @classmethod
    def lazy_query_str(cls, store, obj: ibis_types.Table) -> str:
        return str(ibis.to_sql(obj, cls.get_con(store).name))


def sa_select(*args, **kwargs):
    """Run sa.select in 'old' way compatible with sqlalchemy>=2.0."""
    try:
        return sa.select(*args, **kwargs)
    except sa.exc.ArgumentError:
        if len(args) == 1 and isinstance(args[0], Iterable) and len(kwargs) == 0:
            # for sqlalchemy 2.0, we need to unpack columns
            return sa.select(*args[0])
        else:
            raise
