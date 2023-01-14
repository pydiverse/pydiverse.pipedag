from __future__ import annotations

import itertools
import json
import re
import textwrap
import warnings
from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.sql.elements

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.backend.table.util import engine_dispatch
from pydiverse.pipedag.backend.table.util.sql_ddl import (
    AddIndex,
    AddPrimaryKey,
    ChangeColumnNullable,
    ChangeColumnTypes,
    CopyTable,
    CreateDatabase,
    CreateSchema,
    CreateTableAsSelect,
    CreateViewAsSelect,
    DropFunction,
    DropProcedure,
    DropSchema,
    DropTable,
    DropView,
    RenameSchema,
    Schema,
    split_ddl_statement,
)
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.context.context import ConfigContext, StageCommitTechnique
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.materialize.util import compute_cache_key


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
        self.pytsql_isolate_top_level_statements = pytsql_isolate_top_level_statements
        self.print_materialize = print_materialize
        self.print_sql = print_sql
        self.no_db_locking = no_db_locking
        self.metadata_schema = self.get_schema(self.METADATA_SCHEMA)

        self._init_database(engine_url, create_database_if_not_exists)
        self.engine = self._connect(engine_url, self.schema_prefix, self.schema_suffix)

        # Set up metadata tables and schema
        from sqlalchemy import BigInteger, Boolean, Column, DateTime, String

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
            Column("output_json", String(2048)),  # 2k might be too small => TBD
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
                    conn.execute("SELECT 1")
            except sa.exc.DBAPIError:
                postgres_db_url = url.set(database="postgres")
                tmp_engine = sa.create_engine(postgres_db_url)
                try:
                    with tmp_engine.connect() as conn:
                        conn.execute("COMMIT")
                        conn.execute(CreateDatabase(url.database))
                except sa.exc.DBAPIError:
                    # This happens if multiple instances try to create the database
                    # at the same time.
                    with engine.connect() as conn:
                        # Verify database actually exists
                        conn.execute("SELECT 1")
        else:
            raise NotImplementedError(
                "create_database_if_not_exists is only implemented for postgres, yet"
            )
        engine.dispose()

    @staticmethod
    def _connect(engine_url, schema_prefix, schema_suffix):
        engine = sa.create_engine(engine_url)
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

        return conn.execute(query)

    def execute(self, query, *, conn: sa.engine.Connection = None):
        if conn is None:
            with self.engine.connect() as conn:
                return self.execute(query, conn=conn)

        if isinstance(query, sa.schema.DDLElement):
            # Some custom DDL statements contain multiple statements.
            # They are all seperated using a special seperator.
            query_str = str(
                query.compile(self.engine, compile_kwargs={"literal_binds": True})
            )
            for part in split_ddl_statement(query_str):
                self._execute(part, conn)
            return

        return self._execute(query, conn)

    def add_indexes(self, table: Table, schema: Schema):
        if table.primary_key is not None:
            key = table.primary_key
            if isinstance(key, str):
                key = [key]
            self.add_primary_key(table.name, schema, key)
        if table.indexes is not None:
            for index in table.indexes:
                self.add_index(table.name, schema, index)

    @engine_dispatch
    def add_primary_key(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        name: str | None = None,
    ):
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
        name: str | None = None,
    ):
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

    # @add_primary_key.dialect("ibm_db_sa")
    # def _add_primary_key_ibm_db_sa(self, table_name: str, schema: Schema,
    #         key_columns: list[str], name: str | None = None):
    #     # # Failed to make this work: (Database hangs when creating index on
    #     # # altered column)
    #     # sql_types = self.reflect_sql_types(key, table, schema)
    #     # if any([isinstance(_type, sa.String) and (_type.length is None or
    #     #           _type.length > 256) for _type in sql_types]):
    #     #     # impose some varchar length limit to allow use in primary key
    #     #     # / index
    #     #     self.execute(ChangeColumnTypes(table.name, schema, key,
    #     #           sql_types, nullable=False, cap_varchar_max=256))
    #     # else:
    #     self.execute(ChangeColumnNullable(table_name, schema, key_columns,
    #           nullable=False))
    #     self.execute(AddPrimaryKey(table_name, schema, key_columns, name))
    #
    # @add_index.dialect("mssql")
    # def _add_index_ibm_db_sa(self, table_name: str, schema: Schema,
    #         index: list[str], name: str | None = None):
    #     # # Failed to make this work: (Database hangs when creating index on
    #     # # altered column)
    #     # sql_types = self.reflect_sql_types(index, table, schema)
    #     # if any([isinstance(_type, sa.String) and (_type.length is None or
    #     #       _type.length > 256) for _type in sql_types]):
    #     #     # impose some varchar(max) limit to allow use in primary key
    #     #     # / index
    #     #     self.execute(
    #     #         ChangeColumnTypes(table.name, schema, index, sql_types,
    #     #               cap_varchar_max=256))
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
                pk_constraint["name"],
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
        meta = sa.MetaData()
        meta.reflect(bind=self.engine, schema=schema.get())
        tables = {table.name: table for table in meta.tables.values()}
        sql_types = [tables[table_name].c[col].type for col in col_names]
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

            with self.engine.connect() as conn:
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
        self.execute(CreateSchema(self.metadata_schema, if_not_exists=True))
        with self.engine.connect() as conn:
            try:
                version = conn.execute(
                    sa.select(self.version_table.c.version)
                ).scalar_one_or_none()
            except sa.exc.ProgrammingError:
                # table metadata_version does not yet exist
                version = None
            if version is None:
                with conn.begin():
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
    def _init_stage_schema_swap(self, stage: Stage):
        schema = self.get_schema(stage.name)
        transaction_schema = self.get_schema(stage.transaction_name)

        cs_base = CreateSchema(schema, if_not_exists=True)
        ds_trans = DropSchema(
            transaction_schema, if_exists=True, cascade=True, engine=self.engine
        )
        cs_trans = CreateSchema(transaction_schema, if_not_exists=False)

        with self.engine.connect() as conn:
            self.execute(cs_base, conn=conn)
            self.execute(ds_trans, conn=conn)
            self.execute(cs_trans, conn=conn)

            if not self.disable_caching:
                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema.in_([True]))
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
        with self.engine.connect() as conn:
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
            # the 'USE [{database}]' statement is important for these calls
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

            # Clear metadata
            if not self.disable_caching:
                conn.execute(
                    self.tasks_table.delete()
                    .where(self.tasks_table.c.stage == stage.name)
                    .where(self.tasks_table.c.in_transaction_schema.in_([True]))
                )

    def _init_stage_read_views(self, stage: Stage):
        try:
            with self.engine.connect() as conn:
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

        with self.engine.connect() as conn, conn.begin():
            # TODO: for mssql try to find schema does not exist and then move
            #       the forgotten tmp schema there
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

        with self.engine.connect() as conn:
            # Delete all read views in visible (destination) schema
            views = self.get_view_names(dest_schema.get())
            for view in views:
                self.execute(DropView(view, schema=dest_schema), conn=conn)

            # Create views for all tables in transaction schema
            src_meta = sa.MetaData()
            src_meta.reflect(bind=self.engine, schema=src_schema.get())
            for table in src_meta.tables.values():
                self.execute(
                    CreateViewAsSelect(table.name, dest_schema, sa.select(table)),
                    conn=conn,
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
                    .where(table.c.in_transaction_schema.in_([False]))
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
            raise CacheError(
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
            raise CacheError(msg) from _e
        self.add_indexes(table, self.get_schema(stage.transaction_name))

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        stage = table.stage
        schema_name = self.get_schema(metadata.stage).get()
        has_table = sa.inspect(self.engine).has_table(metadata.name, schema=schema_name)
        if not has_table:
            raise CacheError(
                f"Can't copy lazy table '{metadata.name}' (schema:"
                f" '{metadata.stage}') to transaction because no such table"
                " exists."
            )

        try:
            self.execute(
                CopyTable(
                    metadata.name,
                    self.get_schema(metadata.stage),
                    metadata.name,
                    self.get_schema(stage.transaction_name),
                )
            )
        except Exception as _e:
            msg = (
                f"Failed to copy lazy table {metadata.name} (schema:"
                f" '{metadata.stage}') to transaction."
            )
            raise CacheError(msg) from _e
        self.add_indexes(table, self.get_schema(stage.transaction_name))

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
        with self.engine.connect() as conn:
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
                    f"Failed to copy table {table_name} (schema:"
                    f" '{src_schema}') to transaction."
                )
                raise CacheError(msg) from _e

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

        with self.engine.connect() as conn:
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
            with self.engine.connect() as conn:
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
            with self.engine.connect() as conn:
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
            raise CacheError("Multiple results found task metadata") from None

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
            with self.engine.connect() as conn:
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
            with self.engine.connect() as conn:
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
            raise CacheError(
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
            with self.engine.connect() as conn:
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
            with self.engine.connect() as conn:
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
            raise CacheError("Multiple results found for raw sql cache key") from None

        if result is None:
            raise CacheError("No result found for raw sql cache key")

        return RawSqlMetadata(
            prev_tables=json.loads(result.prev_tables),
            tables=json.loads(result.tables),
            stage=result.stage,
            query_hash=result.query_hash,
            task_hash=result.task_hash,
        )

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
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.select([self.tasks_table.c.output_json])
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
    ):
        obj = table.obj
        if isinstance(table.obj, sa.Table):
            obj = sa.select(table.obj)

        schema = store.get_schema(stage_name)
        store.execute(CreateTableAsSelect(table.name, schema, obj))
        store.add_indexes(table, schema)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        return sa.Table(
            table.name,
            sa.MetaData(bind=store.engine),
            schema=store.get_schema(stage_name).get(),
            autoload_with=store.engine,
        )

    @classmethod
    def lazy_query_str(cls, store, obj) -> str:
        return str(obj.compile(store.engine, compile_kwargs={"literal_binds": True}))


@SQLTableStore.register_table(pd)
class PandasTableHook(TableHook[SQLTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(cls, store, table: Table[pd.DataFrame], stage_name):
        schema = store.get_schema(stage_name)
        if store.print_materialize:
            store.logger.info(
                f"Writing table '{schema.get()}.{table.name}':\n{table.obj}"
            )
        dtype_map = {}
        if store.engine.dialect.name == "ibm_db_sa":
            # Default string target is CLOB which can't be used for indexing.
            # We could use VARCHAR(32000), but if we change this to VARCHAR(256)
            # for indexed columns, DB2 hangs.
            dtype_map = {
                col: sa.VARCHAR(256)
                for col in table.obj.dtypes.loc[lambda x: x == object].index
            }
        table.obj.to_sql(
            table.name,
            store.engine,
            schema=schema.get(),
            index=False,
            dtype=dtype_map,
        )
        store.add_indexes(table, schema)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        schema = store.get_schema(stage_name).get()
        with store.engine.connect() as conn:
            df = pd.read_sql_table(table.name, conn, schema=schema)
            return df

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


try:
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
    def materialize(cls, store, table: Table[pdt.Table], stage_name):
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        t = table.obj
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            return PandasTableHook.materialize(store, table, stage_name)
        if isinstance(t._impl, SQLTableImpl):
            table.obj = t._impl.build_select()
            return SQLAlchemyTableHook.materialize(store, table, stage_name)
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
