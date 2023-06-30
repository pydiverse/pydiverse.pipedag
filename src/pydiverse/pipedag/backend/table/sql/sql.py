from __future__ import annotations

import json
import textwrap
import warnings
from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any

import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.sql.elements

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore
from pydiverse.pipedag.backend.table.sql.ddl import (
    AddIndex,
    AddPrimaryKey,
    CopyTable,
    CreateAlias,
    CreateDatabase,
    CreateSchema,
    DropAlias,
    DropSchema,
    DropSchemaContent,
    DropTable,
    RenameSchema,
    RenameTable,
    Schema,
    split_ddl_statement,
)
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.context.context import ConfigContext, StageCommitTechnique
from pydiverse.pipedag.context.run_context import DeferredTableStoreOp
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.container import RawSql
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)
from pydiverse.pipedag.util.hashing import stable_hash


class SQLTableStore(BaseTableStore):
    """Table store that materializes tables to a SQL database

    Uses schema swapping for transactions:
    Creates a schema for each stage and a temporary schema for each
    transaction. If all tasks inside a stage succeed, swaps the schemas by
    renaming them.
    """

    METADATA_SCHEMA = "pipedag_metadata"
    LOCK_SCHEMA = "pipedag_locks"

    _REGISTERED_DIALECTS = {}
    _dialect_name: str

    def __new__(cls, engine_url: str, *args, **kwargs):
        # Dynamically instantiate the proper subclass based on the dialect found
        # in the engine url.
        dialect_name = sa.engine.make_url(engine_url).get_dialect().name
        dialect_specific_cls = SQLTableStore._REGISTERED_DIALECTS.get(dialect_name, cls)
        return super(SQLTableStore, dialect_specific_cls).__new__(dialect_specific_cls)

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        config = config.copy()
        engine_url = config.pop("url")
        return cls(engine_url, **config)

    def __init__(
        self,
        engine_url: str,
        *,
        create_database_if_not_exists: bool = False,
        schema_prefix: str = "",
        schema_suffix: str = "",
        avoid_drop_create_schema: bool = False,
        disable_pytsql: bool = False,
        pytsql_isolate_top_level_statements: bool = True,
        print_materialize: bool = False,
        print_sql: bool = False,
        no_db_locking: bool = True,
        unlogged_tables: bool = False,
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
        :param unlogged_tables:
            whether to use UNLOGGED tables or not (dialect postgresql only)
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
        self.unlogged_tables = unlogged_tables
        self.metadata_schema = self.get_schema(self.METADATA_SCHEMA)

        self.engine_url = sa.engine.make_url(engine_url)

        self.engine = self._create_engine()
        self.engines_other = dict()  # i.e. for IBIS connection

        self._init_database()

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

        def autoincrement_pk(name: str, seq_name: str):
            if self.engine.dialect.name == "duckdb":
                sequence = sqlalchemy.Sequence(
                    f"{seq_name}_{name}_seq", schema=self.metadata_schema.get()
                )
                return Column(
                    name,
                    BigInteger,
                    sequence,
                    server_default=sequence.next_value(),
                    primary_key=True,
                )

            return Column(name, BigInteger, primary_key=True, autoincrement=True)

        self.stage_table = sa.Table(
            "stages",
            self.sql_metadata,
            autoincrement_pk("id", "stages"),
            Column("stage", String(64)),
            Column("cur_transaction_name", String(256)),
            schema=self.metadata_schema.get(),
        )

        # Store version number for metadata table schema evolution.
        # We disable caching in case of version mismatch.
        self.disable_caching = False
        self.metadata_version = "0.3.0"  # Increase version if metadata table changes
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
            autoincrement_pk("id", "tasks"),
            Column("name", String(128)),
            Column("stage", String(64)),
            Column("version", String(64)),
            Column("timestamp", DateTime),
            Column("run_id", String(20)),
            Column("position_hash", String(20)),
            Column("input_hash", String(20)),
            Column("cache_fn_hash", String(20)),
            Column("output_json", clob_type),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Lazy Cache Table is unique for stage * in_transaction_schema
        self.lazy_cache_table = sa.Table(
            "lazy_tables",
            self.sql_metadata,
            autoincrement_pk("id", "lazy_tables"),
            Column("name", String(128)),
            Column("stage", String(64)),
            Column("query_hash", String(20)),
            Column("task_hash", String(20)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        # Sql Cache Table is unique for stage * in_transaction_schema
        self.raw_sql_cache_table = sa.Table(
            "raw_sql_tables",
            self.sql_metadata,
            autoincrement_pk("id", "raw_sql_tables"),
            Column("prev_tables", String(256)),
            Column("tables", String(256)),
            Column("stage", String(64)),
            Column("query_hash", String(20)),
            Column("task_hash", String(20)),
            Column("in_transaction_schema", Boolean),
            schema=self.metadata_schema.get(),
        )

        self.logger.info(
            "Initialized SQL Table Store",
            engine_url=self.engine_url.render_as_string(hide_password=True),
            schema_prefix=self.schema_prefix,
            schema_suffix=self.schema_suffix,
        )

    def __init_subclass__(cls, **kwargs):
        dialect_name = getattr(cls, "_dialect_name", None)
        if dialect_name is None:
            raise ValueError(
                "All subclasses of SQLTableStore must have a `_dialect_name` attribute."
                f" But {cls.__name__}._dialect_name is None."
            )

        if dialect_name in SQLTableStore._REGISTERED_DIALECTS:
            warnings.warn(
                f"Already registered a SQLTableStore for dialect {dialect_name}"
            )
        SQLTableStore._REGISTERED_DIALECTS[dialect_name] = cls

    def _init_database(self):
        if not self.create_database_if_not_exists:
            return

        raise NotImplementedError(
            "create_database_if_not_exists is not implemented for this engine"
        )

    def _init_database_with_database(
        self, database: str, execution_options: dict = None
    ):
        if not self.create_database_if_not_exists:
            return

        if execution_options is None:
            execution_options = {}

        try:
            # Check if database exists
            with self.engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            return
        except sa.exc.DBAPIError:
            # Database doesn't exist
            pass

        # Create new database using temporary engine object
        tmp_db_url = self.engine_url.set(database=database)
        tmp_engine = sa.create_engine(tmp_db_url, execution_options=execution_options)

        try:
            with tmp_engine.connect() as conn:
                conn.execute(CreateDatabase(self.engine_url.database))
        except sa.exc.DBAPIError:
            # This happens if multiple instances try to create the database
            # at the same time.
            with self.engine.connect() as conn:
                # Verify database actually exists
                conn.exec_driver_sql("SELECT 1")

    def _create_engine(self):
        return sa.create_engine(self.engine_url)

    @contextmanager
    def engine_connect(self) -> sa.Connection:
        with self.engine.connect() as conn:
            yield conn
            conn.commit()

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
            self.logger.info("Executing sql", query=pretty_query_str)

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
        self.execute(AddPrimaryKey(table_name, schema, key_columns, name))

    def add_index(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        self.execute(AddIndex(table_name, schema, index_columns, name))

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

    def execute_raw_sql(self, raw_sql: RawSql):
        """Executed raw SQL statements in the associated transaction stage"""
        for statement in raw_sql.sql.split(";"):
            self.execute(statement)

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
                self.execute(
                    DropSchemaContent(transaction_schema, self.engine), conn=conn
                )
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
        # If the stage is 100% cache valid, then we just need to update the
        # "in_transaction_schema" column of the metadata tables.
        if not RunContext.get().has_stage_changed(stage):
            self.logger.info("Stage is cache valid", stage=stage)
            with self.engine_connect() as conn:
                self._commit_stage_update_metadata(stage, conn)
            return

        # Commit normally
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
            # TODO: self.avoid_drop_create_schema is currently being ignored...
            self.execute(
                DropSchema(
                    tmp_schema, if_exists=True, cascade=True, engine=self.engine
                ),
                conn=conn,
            )
            self.execute(RenameSchema(schema, tmp_schema, self.engine), conn=conn)
            self.execute(
                RenameSchema(transaction_schema, schema, self.engine), conn=conn
            )
            self.execute(
                RenameSchema(tmp_schema, transaction_schema, self.engine), conn=conn
            )

            self._commit_stage_update_metadata(stage, conn=conn)

    def _commit_stage_read_views(self, stage: Stage):
        dest_schema = self.get_schema(stage.name)
        src_schema = self.get_schema(stage.transaction_name)

        with self.engine_connect() as conn:
            # Clear contents of destination schema
            self.execute(DropSchemaContent(dest_schema, self.engine), conn=conn)

            # Create aliases for all tables in transaction schema
            inspector = sa.inspect(self.engine)
            for table in inspector.get_table_names(schema=src_schema.get()):
                self.execute(CreateAlias(table, src_schema, table, dest_schema))

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
                    .where(~table.c.in_transaction_schema)
                )
                conn.execute(
                    table.update()
                    .where(table.c.stage == stage.name)
                    .values(in_transaction_schema=False)
                )

    def copy_table_to_transaction(self, table: Table):
        from_schema = self.get_schema(table.stage.name)
        from_name = table.name

        if RunContext.get().has_stage_changed(table.stage):
            self._copy_table(table, from_schema, from_name)
        else:
            self._deferred_copy_table(table, from_schema, from_name)

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        from_schema = self.get_schema(metadata.stage)
        from_name = metadata.name

        if RunContext.get().has_stage_changed(table.stage):
            self._copy_table(table, from_schema, from_name)
        else:
            self._deferred_copy_table(table, from_schema, from_name)

    def _copy_table(self, table: Table, from_schema: Schema, from_name: str):
        """Copies the table immediately"""
        inspector = sa.inspect(self.engine)
        has_table = inspector.has_table(from_name, schema=from_schema.get())

        if not has_table:
            available_tables = inspector.get_table_names(from_schema.get())
            msg = (
                f"Can't copy table '{from_name}' (schema: '{from_schema}') to "
                f"transaction because no such table exists.\n"
                f"Tables in schema: {available_tables}"
            )
            self.logger.error(msg)
            raise CacheError(msg)

        self.execute(
            CopyTable(
                from_name,
                from_schema,
                table.name,
                self.get_schema(table.stage.transaction_name),
                early_not_null=table.primary_key,
            )
        )
        self.add_indexes(table, self.get_schema(table.stage.transaction_name))

    def _deferred_copy_table(
        self,
        table: Table,
        from_schema: Schema,
        from_name: str,
    ):
        """Tries to perform a deferred copy

        As long as the stage is determined to still be 100% cache valid, we don't
        actually copy any tables to the transaction, and instead just create an alias
        from the main schema to the transaction schema.

        If at the end of the stage, the all tables in the stage are cache valid, then
        we don't actually need to commit the stage, and instead just rename the
        tables from their old names to their new names.

        Otherwise, we copy the tables to the transaction schema in the background,
        and once we're ready to commit, we replace the aliases with the copied tables.
        """
        assert from_schema == self.get_schema(table.stage.name)

        inspector = sa.inspect(self.engine)
        has_table = inspector.has_table(from_name, schema=from_schema.get())
        if not has_table:
            available_tables = inspector.get_table_names(from_schema.get())
            msg = (
                f"Can't deferred copy table '{from_name}' (schema: '{from_schema}') to "
                f"transaction because no such table exists.\n"
                f"Tables in schema: {available_tables}"
            )
            self.logger.error(msg)
            raise CacheError(msg)

        try:
            ctx = RunContext.get()
            stage = table.stage

            self.execute(
                CreateAlias(
                    from_name,
                    from_schema,
                    table.name,
                    self.get_schema(stage.transaction_name),
                )
            )

            table_copy = table.copy_without_obj()
            table_copy.name = f"{table.name}__copy"

            ctx.defer_table_store_op(
                stage,
                DeferredTableStoreOp(
                    "_copy_table",
                    DeferredTableStoreOp.Condition.ON_STAGE_CHANGED,
                    (
                        table_copy,
                        from_schema,
                        from_name,
                    ),
                ),
            )
            ctx.defer_table_store_op(
                stage,
                DeferredTableStoreOp(
                    "_swap_alias_with_table_copy",
                    DeferredTableStoreOp.Condition.ON_STAGE_COMMIT,
                    (),
                    {"table": table, "table_copy": table_copy},
                ),
            )
            ctx.defer_table_store_op(
                stage,
                DeferredTableStoreOp(
                    "_rename_table",
                    DeferredTableStoreOp.Condition.ON_STAGE_ABORT,
                    (from_name, table.name, from_schema),
                ),
            )

        except Exception as _e:
            msg = (
                f"Failed to copy table {from_name} (schema: '{from_schema}') "
                f"to transaction."
            )
            self.logger.exception(msg)
            raise CacheError(msg) from _e

    def _swap_alias_with_table_copy(self, table: Table, table_copy: Table):
        assert table_copy.stage.name == table.stage.name

        schema = self.get_schema(table.stage.transaction_name)
        try:
            self.execute(
                DropAlias(
                    table.name,
                    schema,
                )
            )
            self.execute(
                RenameTable(
                    table_copy.name,
                    table.name,
                    schema,
                )
            )
        except Exception as _e:
            msg = (
                f"Failed putting copied table {table_copy.name} (schema: '{schema}') "
                f"in place of alias."
            )
            raise RuntimeError(msg) from _e

    def _rename_table(self, from_name: str, to_name: str, schema: Schema):
        if from_name == to_name:
            return
        self.logger.info("RENAME", fr=from_name, to=to_name)
        self.execute(RenameTable(from_name, to_name, schema))

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
        inspector = sa.inspect(self.engine)
        return inspector.get_view_names(schema)

    def copy_raw_sql_tables_to_transaction(
        self, metadata: RawSqlMetadata, target_stage: Stage
    ):
        src_schema = self.get_schema(metadata.stage)
        dest_schema = self.get_schema(target_stage.transaction_name)

        new_tables = set(metadata.tables) - set(metadata.prev_tables)

        views = set(self.get_view_names(src_schema.get(), include_everything=True))
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
                self.logger.exception(
                    msg
                    + " This error is treated as cache-lookup-failure and thus we can"
                    " continue."
                )
                raise CacheError(msg) from _e

        views_to_copy = new_tables & set(views)
        for view_name in views_to_copy:
            self._copy_view_to_transaction(view_name, src_schema, dest_schema)

    def _copy_view_to_transaction(
        self, view_name: str, src_schema: Schema, dest_schema: Schema
    ):
        raise NotImplementedError("Only implemented for mssql.")

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
                        position_hash=metadata.position_hash,
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

        ignore_fresh_input = ConfigContext.get().ignore_fresh_input
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
            raise CacheError("Multiple results found task metadata") from None

        if result is None:
            raise CacheError(f"Couldn't retrieve task from cache: {task}")

        return TaskMetadata(
            name=result.name,
            stage=result.stage,
            version=result.version,
            timestamp=result.timestamp,
            run_id=result.run_id,
            position_hash=result.position_hash,
            input_hash=result.input_hash,
            cache_fn_hash=result.cache_fn_hash,
            output_json=result.output_json,
        )

    def retrieve_all_task_metadata(self, task: MaterializingTask) -> list[TaskMetadata]:
        with self.engine_connect() as conn:
            results = (
                conn.execute(
                    self.tasks_table.select()
                    .where(self.tasks_table.c.name == task.name)
                    .where(self.tasks_table.c.stage == task.stage.name)
                    .where(self.tasks_table.c.position_hash == task.position_hash)
                )
                .mappings()
                .all()
            )

        return [
            TaskMetadata(
                name=result.name,
                stage=result.stage,
                version=result.version,
                timestamp=result.timestamp,
                run_id=result.run_id,
                position_hash=result.position_hash,
                input_hash=result.input_hash,
                cache_fn_hash=result.cache_fn_hash,
                output_json=result.output_json,
            )
            for result in results
        ]

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

    def resolve_alias(self, table: str, schema: str) -> tuple[str, str]:
        return table, schema

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
        return stable_hash(*result)

    # DatabaseLockManager

    def get_engine_for_locking(self) -> sa.Engine:
        return self._create_engine()

    def get_lock_schema(self) -> Schema:
        return self.get_schema(self.LOCK_SCHEMA)


# Load SQLTableStore Hooks
import pydiverse.pipedag.backend.table.sql.hooks  # noqa

# Load SQLTableStore dialect specific subclasses
import pydiverse.pipedag.backend.table.sql.dialects  # noqa
