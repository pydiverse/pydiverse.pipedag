from __future__ import annotations

import copy
import re

import pyparsing as pp
import sqlalchemy as sa
from attr import frozen
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import TextClause

__all__ = [
    "Schema",
    "CreateSchema",
    "DropSchema",
    "RenameSchema",
    "DropSchemaContent",
    "CreateTableAsSelect",
    "CreateViewAsSelect",
    "CreateAlias",
    "CopyTable",
    "RenameTable",
    "DropTable",
    "CreateDatabase",
    "DropAlias",
    "DropFunction",
    "DropProcedure",
    "DropView",
    "AddPrimaryKey",
    "AddIndex",
    "ChangeColumnNullable",
    "ChangeColumnTypes",
    "ChangeTableLogged",
    "split_ddl_statement",
]


@frozen
class Schema:
    name: str
    prefix: str
    suffix: str

    def get(self):
        return self.prefix + self.name + self.suffix

    def __str__(self):
        return self.get()


class CreateSchema(DDLElement):
    def __init__(self, schema: Schema, if_not_exists=False):
        self.schema = schema
        self.if_not_exists = if_not_exists


class DropSchema(DDLElement):
    def __init__(self, schema: Schema, if_exists=False, cascade=False, *, engine=None):
        """
        :param engine: Used if cascade=True but the database doesn't support cascade.
        """
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade

        self.engine = engine


class RenameSchema(DDLElement):
    def __init__(self, from_: Schema, to: Schema, engine: sa.Engine):
        self.from_ = from_
        self.to = to
        self.engine = engine


class DropSchemaContent(DDLElement):
    def __init__(self, schema: Schema, engine: sa.Engine):
        self.schema = schema
        self.engine = engine


class CreateDatabase(DDLElement):
    def __init__(self, database: str, if_not_exists=False):
        self.database = database
        self.if_not_exists = if_not_exists


class DropDatabase(DDLElement):
    def __init__(self, database: str, if_exists=False, cascade=False):
        self.database = database
        self.if_exists = if_exists
        self.cascade = cascade


class CreateTableAsSelect(DDLElement):
    def __init__(
        self,
        name: str,
        schema: Schema,
        query: Select | TextClause | sa.Text,
        *,
        early_not_null: None | str | list[str] = None,
        source_tables: None | list[dict[str, str]] = None,
        unlogged: bool = False,
    ):
        self.name = name
        self.schema = schema
        self.query = query
        # some dialects may choose to set NOT-NULL constraint in the middle of
        # create table as select
        self.early_not_null = early_not_null
        # some dialects may lock source and destination tables
        self.source_tables = source_tables
        # Postgres supports creating unlogged tables. Flag should get ignored by
        # other dialects
        self.unlogged = unlogged


class CreateViewAsSelect(DDLElement):
    def __init__(self, name: str, schema: Schema, query: Select | TextClause | sa.Text):
        self.name = name
        self.schema = schema
        self.query = query


class CreateAlias(DDLElement):
    def __init__(
        self,
        from_name,
        from_schema: Schema,
        to_name,
        to_schema: Schema,
        or_replace=False,
    ):
        self.from_name = from_name
        self.from_schema = from_schema
        self.to_name = to_name
        self.to_schema = to_schema
        self.or_replace = or_replace


class CopyTable(DDLElement):
    def __init__(
        self,
        from_name,
        from_schema: Schema,
        to_name,
        to_schema: Schema,
        if_not_exists=False,
        early_not_null: None | str | list[str] = None,
    ):
        self.from_name = from_name
        self.from_schema = from_schema
        self.to_name = to_name
        self.to_schema = to_schema
        self.if_not_exists = if_not_exists
        self.early_not_null = early_not_null


class RenameTable(DDLElement):
    def __init__(
        self,
        from_name,
        to_name,
        schema: Schema,
    ):
        self.from_name = from_name
        self.to_name = to_name
        self.schema = schema


class DropTable(DDLElement):
    def __init__(self, name, schema: Schema, if_exists=False):
        self.name = name
        self.schema = schema
        self.if_exists = if_exists


class DropView(DDLElement):
    """
    Attention: For mssql, this statement must be prefixed with
               a 'USE <database>' statement.
    """

    def __init__(self, name, schema: Schema, if_exists=False):
        self.name = name
        self.schema = schema
        self.if_exists = if_exists


class DropAlias(DDLElement):
    """
    This is used for dialect=ibm_sa_db
    """

    def __init__(self, name, schema: Schema, if_exists=False):
        self.name = name
        self.schema = schema
        self.if_exists = if_exists


class DropProcedure(DDLElement):
    """
    Attention: For mssql, this statement must be prefixed with
               a 'USE <database>' statement.
    """

    def __init__(self, name, schema: Schema, if_exists=False):
        self.name = name
        self.schema = schema
        self.if_exists = if_exists


class DropFunction(DDLElement):
    """
    Attention: For mssql, this statement must be prefixed with
               a 'USE <database>' statement.
    """

    def __init__(self, name, schema: Schema, if_exists=False):
        self.name = name
        self.schema = schema
        self.if_exists = if_exists


class AddPrimaryKey(DDLElement):
    def __init__(
        self,
        table_name: str,
        schema: Schema,
        key_columns: list[str],
        name: str | None = None,
    ):
        self.table_name = table_name
        self.schema = schema
        self.key = key_columns
        self._name = name

    @property
    def name(self) -> str:
        if self._name:
            return self._name
        columns = "_".join(c.lower() for c in self.key)
        return "pk_" + columns + "_" + self.table_name.lower()


class AddIndex(DDLElement):
    def __init__(
        self,
        table_name: str,
        schema: Schema,
        index_columns: list[str],
        name: str | None = None,
    ):
        self.table_name = table_name
        self.schema = schema
        self.index = index_columns
        self._name = name

    @property
    def name(self) -> str:
        if self._name:
            return self._name
        columns = "_".join(c.lower() for c in self.index)
        return "idx_" + columns + "_" + self.table_name.lower()


class ChangeColumnTypes(DDLElement):
    def __init__(
        self,
        table_name: str,
        schema: Schema,
        column_names: list[str],
        column_types: list[str],
        nullable: bool | list[bool] | None = None,
        cap_varchar_max: int | None = None,
    ):
        if not isinstance(nullable, list):
            nullable = [nullable for _ in column_names]
        self.table_name = table_name
        self.schema = schema
        self.column_names = column_names
        self.column_types = column_types
        self.nullable = nullable
        self.cap_varchar_max = cap_varchar_max


class ChangeColumnNullable(DDLElement):
    def __init__(
        self,
        table_name: str,
        schema: Schema,
        column_names: list[str],
        nullable: bool | list[bool],
    ):
        if isinstance(nullable, bool):
            nullable = [nullable for _ in column_names]
        self.table_name = table_name
        self.schema = schema
        self.column_names = column_names
        self.nullable = nullable


class ChangeTableLogged(DDLElement):
    """Changes a postgres table from LOGGED to UNLOGGED (or vice-versa)

    This reduces safety in case of a crash or unclean shutdown, but can significantly
    increase write performance:

    https://www.postgresql.org/docs/9.5/sql-createtable.html#SQL-CREATETABLE-UNLOGGED
    """

    def __init__(
        self,
        table_name: str,
        schema: Schema,
        logged: bool,
    ):
        self.table_name = table_name
        self.schema = schema
        self.logged = logged


@compiles(CreateSchema)
def visit_create_schema(create: CreateSchema, compiler, **kw):
    _ = kw
    schema = compiler.preparer.format_schema(create.schema.get())
    text = ["CREATE SCHEMA"]
    if create.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(schema)
    return " ".join(text)


@compiles(CreateSchema, "mssql")
def visit_create_schema_mssql(create: CreateSchema, compiler, **kw):
    _ = kw
    schema = compiler.preparer.format_schema(create.schema.get())
    if create.if_not_exists:
        unquoted_schema = create.schema.get()
        return f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.schemas WHERE name = N'{unquoted_schema}'
        )
        BEGIN EXEC('CREATE SCHEMA {schema}') END
        """
    else:
        return f"CREATE SCHEMA {schema}"


@compiles(CreateSchema, "ibm_db_sa")
def visit_create_schema_ibm_db_sa(create: CreateSchema, compiler, **kw):
    """For IBM DB2 we need to jump through extra hoops for if_exists=True."""
    _ = kw
    schema = compiler.preparer.format_schema(create.schema.get())
    if create.if_not_exists:
        return f"""
        BEGIN
            declare continue handler for sqlstate '42710' begin end;
            execute immediate 'CREATE SCHEMA {schema}';
        END
        """
    else:
        return f"CREATE SCHEMA {schema}"


@compiles(DropSchema)
def visit_drop_schema(drop: DropSchema, compiler, **kw):
    _ = kw
    schema = compiler.preparer.format_schema(drop.schema.get())
    text = ["DROP SCHEMA"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(schema)
    if drop.cascade:
        text.append("CASCADE")
    return " ".join(text)


@compiles(DropSchema, "mssql")
def visit_drop_schema_mssql(drop: DropSchema, compiler, **kw):
    _ = kw
    schema = compiler.preparer.format_schema(drop.schema.get())
    statements = []

    if drop.cascade:
        if drop.engine is None:
            raise ValueError(
                "Using DropSchema with cascade=True for mssql requires passing"
                " the engine kwarg to DropSchema."
            )

        statements.append(DropSchemaContent(drop.schema, drop.engine))

    text = ["DROP SCHEMA"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(schema)
    statements.append(" ".join(text))

    return join_ddl_statements(statements, compiler, **kw)


@compiles(DropSchema, "ibm_db_sa")
def visit_drop_schema_ibm_db_sa(drop: DropSchema, compiler, **kw):
    statements = []
    if drop.cascade:
        if drop.engine is None:
            raise ValueError(
                "Using DropSchema with cascade=True for ibm_db2 requires passing"
                " the engine kwarg to DropSchema."
            )
        statements.append(DropSchemaContent(drop.schema, drop.engine))

    # Compile DROP SCHEMA statement
    schema = compiler.preparer.format_schema(drop.schema.get())
    if drop.if_exists:
        # Add error handler to cache the case that the schema doesn't exist
        statements.append(
            f"""
            BEGIN
                declare continue handler for sqlstate '42704' begin end;
                execute immediate 'DROP SCHEMA {schema} RESTRICT';
            END
            """
        )
    else:
        statements.append(f"DROP SCHEMA {schema} RESTRICT")

    return join_ddl_statements(statements, compiler, **kw)


@compiles(RenameSchema)
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
    _ = kw
    from_ = compiler.preparer.format_schema(rename.from_.get())
    to = compiler.preparer.format_schema(rename.to.get())
    return "ALTER SCHEMA " + from_ + " RENAME TO " + to


@compiles(RenameSchema, "mssql")
def visit_rename_schema_mssql(rename: RenameSchema, compiler, **kw):
    # MSSql doesn't support renaming schemas, but it allows you to move objects from
    # one schema to another.
    # https://stackoverflow.com/questions/17571233/how-to-change-schema-of-all-tables-views-and-stored-procedures-in-mssql
    # https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-schema-transact-sql?view=sql-server-ver16
    from pydiverse.pipedag.backend.table.sql.reflection import (
        PipedagMSSqlReflection,
    )

    _ = kw
    from_ = compiler.preparer.format_schema(rename.from_.get())
    to = compiler.preparer.format_schema(rename.to.get())

    inspector = sa.inspect(rename.engine)

    # Reflect to get objects which we want to move
    names_to_move = []
    names_to_redefine = []

    table_names = inspector.get_table_names(schema=rename.from_.get())
    view_names = inspector.get_view_names(schema=rename.from_.get())
    alias_names = PipedagMSSqlReflection.get_alias_names(
        rename.engine, schema=rename.from_.get()
    )
    procedure_names = PipedagMSSqlReflection.get_procedure_names(
        rename.engine, schema=rename.from_.get()
    )
    function_names = PipedagMSSqlReflection.get_function_names(
        rename.engine, schema=rename.from_.get()
    )

    names_to_move.extend(table_names)
    names_to_move.extend(alias_names)
    names_to_redefine.extend(view_names)
    names_to_redefine.extend(procedure_names)
    names_to_redefine.extend(function_names)

    if not (names_to_move or names_to_redefine):
        return ""

    # Produce statement
    statements = []

    statements.append(CreateSchema(rename.to, if_not_exists=True))
    for name in names_to_move:
        name = compiler.preparer.quote(name)
        statements.append(f"ALTER SCHEMA {to} TRANSFER {from_}.{name}")

    # Recreate views, procedures and functions, but replace all references to the
    # old schema name with the new schema name
    with rename.engine.connect() as conn:
        for name in names_to_redefine:
            definition = _mssql_update_definition(conn, name, rename.from_, rename.to)
            statements.append(definition)

    for view in view_names:
        statements.append(DropView(view, rename.from_))
    for procedure in procedure_names:
        statements.append(DropProcedure(procedure, rename.from_))
    for function in function_names:
        statements.append(DropFunction(function, rename.from_))

    statements.append(DropSchema(rename.from_))
    return join_ddl_statements(statements, compiler, **kw)


@compiles(DropSchemaContent)
def visit_drop_schema_content(drop: DropSchemaContent, compiler, **kw):
    _ = kw
    schema = drop.schema
    engine = drop.engine
    inspector = sa.inspect(engine)

    statements = []

    for table in inspector.get_table_names(schema=schema.get()):
        statements.append(DropTable(table, schema=schema))
    for view in inspector.get_view_names(schema=schema.get()):
        statements.append(DropView(view, schema=schema))

    return join_ddl_statements(statements, compiler, **kw)


@compiles(DropSchemaContent, "mssql")
def visit_drop_schema_content_mssql(drop: DropSchemaContent, compiler, **kw):
    from pydiverse.pipedag.backend.table.sql.reflection import (
        PipedagMSSqlReflection,
    )

    _ = kw
    schema = drop.schema
    engine = drop.engine
    inspector = sa.inspect(engine)

    statements = []

    for table in inspector.get_table_names(schema=schema.get()):
        statements.append(DropTable(table, schema=schema))
    for view in inspector.get_view_names(schema=schema.get()):
        statements.append(DropView(view, schema=schema))
    for alias in PipedagMSSqlReflection.get_alias_names(engine, schema=schema.get()):
        statements.append(DropAlias(alias, schema=drop.schema))
    for procedure in PipedagMSSqlReflection.get_procedure_names(
        engine, schema=schema.get()
    ):
        statements.append(DropProcedure(procedure, schema=schema))
    for function in PipedagMSSqlReflection.get_function_names(
        engine, schema=schema.get()
    ):
        statements.append(DropFunction(function, schema=schema))

    return join_ddl_statements(statements, compiler, **kw)


@compiles(DropSchemaContent, "ibm_db_sa")
def visit_drop_schema_content_ibm_db2(drop: DropSchemaContent, compiler, **kw):
    from pydiverse.pipedag.backend.table.sql.reflection import PipedagDB2Reflection

    _ = kw
    schema = drop.schema
    engine = drop.engine
    inspector = sa.inspect(engine)

    statements = []

    for table in inspector.get_table_names(schema=schema.get()):
        statements.append(DropTable(table, schema=schema))
    for view in inspector.get_view_names(schema=schema.get()):
        statements.append(DropView(view, schema=schema))
    for alias in PipedagDB2Reflection.get_alias_names(engine, schema=schema.get()):
        statements.append(DropAlias(alias, schema=drop.schema))

    return join_ddl_statements(statements, compiler, **kw)


@compiles(CreateDatabase)
def visit_create_database(create: CreateDatabase, compiler, **kw):
    _ = kw
    database = compiler.preparer.format_schema(create.database)
    text = ["CREATE DATABASE"]
    if create.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(database)
    return " ".join(text)


@compiles(DropDatabase)
def visit_drop_database(drop: DropDatabase, compiler, **kw):
    _ = kw
    schema = compiler.preparer.format_schema(drop.database)
    text = ["DROP DATABASE"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(schema)
    if drop.cascade:
        text.append("CASCADE")
    ret = " ".join(text)
    raise NotImplementedError(
        f"Disable for now for safety reasons (not yet needed): {ret}"
    )


def _visit_create_obj_as_select(create, compiler, _type, kw, *, prefix="", suffix=""):
    name = compiler.preparer.quote(create.name)
    schema = compiler.preparer.format_schema(create.schema.get())
    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)
    return f"CREATE {_type} {schema}.{name} AS\n{prefix}{select}{suffix}"


@compiles(CreateTableAsSelect)
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    return _visit_create_obj_as_select(create, compiler, "TABLE", kw)


@compiles(CreateTableAsSelect, "postgresql")
def visit_create_table_as_select_postgresql(
    create: CreateTableAsSelect, compiler, **kw
):
    if create.unlogged:
        return _visit_create_obj_as_select(create, compiler, "UNLOGGED TABLE", kw)
    else:
        return _visit_create_obj_as_select(create, compiler, "TABLE", kw)


@compiles(CreateTableAsSelect, "mssql")
def visit_create_table_as_select_mssql(create: CreateTableAsSelect, compiler, **kw):
    name = compiler.preparer.quote(create.name)
    schema = compiler.preparer.format_schema(create.schema.get())

    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)

    return insert_into_in_query(select, schema, name)


@compiles(CreateTableAsSelect, "ibm_db_sa")
def visit_create_table_as_select_ibm_db_sa(create: CreateTableAsSelect, compiler, **kw):
    prepare_statement = _visit_create_obj_as_select(
        create, compiler, "TABLE", kw, prefix="(", suffix=") DEFINITION ONLY"
    )

    if create.early_not_null is not None:
        not_null_cols = create.early_not_null
        if isinstance(not_null_cols, str):
            not_null_cols = [not_null_cols]
        not_null_statements = _get_nullable_change_statements(
            ChangeColumnNullable(
                create.name,
                create.schema,
                column_names=not_null_cols,
                nullable=False,
            ),
            compiler,
        )
    else:
        not_null_statements = []

    preparer = compiler.preparer
    name = preparer.quote(create.name)
    schema = preparer.format_schema(create.schema.get())

    lock_statements = [f"LOCK TABLE {schema}.{name} IN EXCLUSIVE MODE"]
    if create.source_tables is not None:
        src_tables = [
            f"{preparer.format_schema(tbl['schema'])}.{preparer.quote(tbl['name'])}"
            for tbl in create.source_tables
        ]
        lock_statements += [f"LOCK TABLE {ref} IN SHARE MODE" for ref in src_tables]

    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)
    create_statement = f"INSERT INTO {schema}.{name}\n{select}"

    return join_ddl_statements(
        [prepare_statement]
        + not_null_statements
        + lock_statements
        + [create_statement],
        compiler,
        **kw,
    )


@compiles(CreateViewAsSelect)
def visit_create_view_as_select(create: CreateViewAsSelect, compiler, **kw):
    return _visit_create_obj_as_select(create, compiler, "VIEW", kw)


def insert_into_in_query(select_sql, schema, table):
    into = f"INTO {schema}.{table}"
    into_point = None
    # insert INTO before first FROM, WHERE, GROUP BY, WINDOW, HAVING,
    #                          ORDER BY, UNION, EXCEPT, INTERSECT
    for marker in [
        "FROM",
        "WHERE",
        r"GROUP\s*BY",
        "WINDOW",
        "HAVING",
        r"ORDER\s*BY",
        "UNION",
        "EXCEPT",
        "INTERSECT",
    ]:
        regex = re.compile(marker, re.IGNORECASE)
        for match in regex.finditer(select_sql):
            match_start = match.span()[0]
            prev = select_sql[0:match_start]
            # ignore marker in subqueries in select columns
            if prev.count("(") == prev.count(")"):
                into_point = match_start
                break
        if into_point is not None:
            break
    return (
        select_sql[0:into_point] + into + " " + select_sql[into_point:]
        if into_point is not None
        else select_sql + " " + into
    )


@compiles(CreateAlias)
def visit_create_alias(create_alias: CreateAlias, compiler, **kw):
    from_name = compiler.preparer.quote(create_alias.from_name)
    from_schema = compiler.preparer.format_schema(create_alias.from_schema.get())
    query = sa.select("*").select_from(sa.text(f"{from_schema}.{from_name}"))
    return compiler.process(
        CreateViewAsSelect(create_alias.to_name, create_alias.to_schema, query), **kw
    )


@compiles(CreateAlias, "mssql")
def visit_create_alias(create_alias: CreateAlias, compiler, **kw):
    from_name = compiler.preparer.quote(create_alias.from_name)
    from_schema = compiler.preparer.format_schema(create_alias.from_schema.get())
    to_name = compiler.preparer.quote(create_alias.to_name)
    to_schema = compiler.preparer.format_schema(create_alias.to_schema.get())

    text = ["CREATE"]
    if create_alias.or_replace:
        text.append("OR REPLACE")
    text.append(f"SYNONYM {to_schema}.{to_name} FOR {from_schema}.{from_name}")
    return " ".join(text)


@compiles(CreateAlias, "ibm_db_sa")
def visit_create_alias(create_alias: CreateAlias, compiler, **kw):
    from_name = compiler.preparer.quote(create_alias.from_name)
    from_schema = compiler.preparer.format_schema(create_alias.from_schema.get())
    to_name = compiler.preparer.quote(create_alias.to_name)
    to_schema = compiler.preparer.format_schema(create_alias.to_schema.get())

    text = ["CREATE"]
    if create_alias.or_replace:
        text.append("OR REPLACE")
    text.append(f"ALIAS {to_schema}.{to_name} FOR TABLE {from_schema}.{from_name}")
    return " ".join(text)


@compiles(CopyTable)
def visit_copy_table(copy_table: CopyTable, compiler, **kw):
    from_name = compiler.preparer.quote(copy_table.from_name)
    from_schema = compiler.preparer.format_schema(copy_table.from_schema.get())
    query = sa.select("*").select_from(sa.text(f"{from_schema}.{from_name}"))
    create = CreateTableAsSelect(
        copy_table.to_name,
        copy_table.to_schema,
        query,
        early_not_null=copy_table.early_not_null,
        source_tables=[
            dict(name=copy_table.from_name, schema=copy_table.from_schema.get())
        ],
    )
    return compiler.process(create, **kw)


@compiles(RenameTable)
def visit_rename_table(rename_table: RenameTable, compiler, **kw):
    _ = kw
    from_table = compiler.preparer.quote(rename_table.from_name)
    to_table = compiler.preparer.quote(rename_table.to_name)
    schema = compiler.preparer.format_schema(rename_table.schema.get())
    return f"ALTER TABLE {schema}.{from_table} RENAME TO {to_table}"


@compiles(RenameTable, "mssql")
def visit_rename_table(rename_table: RenameTable, compiler, **kw):
    _ = kw

    schema = compiler.preparer.format_schema(rename_table.schema.get())
    from_table = compiler.preparer.quote(rename_table.from_name)
    to_table = rename_table.to_name  # no quoting is intentional

    return f"EXEC sp_rename '{schema}.{from_table}', '{to_table}'"


@compiles(RenameTable, "ibm_db_sa")
def visit_rename_table(rename_table: RenameTable, compiler, **kw):
    _ = kw
    from_table = compiler.preparer.quote(rename_table.from_name)
    to_table = compiler.preparer.quote(rename_table.to_name)
    schema = compiler.preparer.format_schema(rename_table.schema.get())
    return f"RENAME TABLE {schema}.{from_table} TO {to_table}"


@compiles(DropTable)
def visit_drop_table(drop: DropTable, compiler, **kw):
    return _visit_drop_anything(drop, "TABLE", compiler, **kw)


@compiles(DropView)
def visit_drop_view(drop: DropView, compiler, **kw):
    return _visit_drop_anything(drop, "VIEW", compiler, **kw)


@compiles(DropAlias)
def visit_drop_alias(drop: DropAlias, compiler, **kw):
    # Not all dialects support a table ALIAS as a first class object.
    # For those that don't we just use views.
    return _visit_drop_anything(drop, "VIEW", compiler, **kw)


@compiles(DropAlias, "mssql")
def visit_drop_alias_mssql(drop: DropAlias, compiler, **kw):
    # What is called ALIAS for dialect ibm_db_sa is called SYNONYM for mssql
    return _visit_drop_anything(drop, "SYNONYM", compiler, **kw)


@compiles(DropAlias, "ibm_db_sa")
def visit_drop_alias_mssql(drop: DropAlias, compiler, **kw):
    return _visit_drop_anything(drop, "ALIAS", compiler, **kw)


@compiles(DropProcedure)
def visit_drop_table(drop: DropProcedure, compiler, **kw):
    return _visit_drop_anything(drop, "PROCEDURE", compiler, **kw)


@compiles(DropFunction)
def visit_drop_table(drop: DropFunction, compiler, **kw):
    return _visit_drop_anything(drop, "FUNCTION", compiler, **kw)


def _visit_drop_anything(
    drop: DropTable | DropView | DropProcedure | DropFunction | DropAlias,
    _type,
    compiler,
    **kw,
):
    _ = kw
    table = compiler.preparer.quote(drop.name)
    schema = compiler.preparer.format_schema(drop.schema.get())
    text = [f"DROP {_type}"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(f"{schema}.{table}")
    return " ".join(text)


@compiles(AddPrimaryKey)
def visit_add_primary_key(add_primary_key: AddPrimaryKey, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote(add_primary_key.table_name)
    schema = compiler.preparer.format_schema(add_primary_key.schema.get())
    pk_name = compiler.preparer.quote(add_primary_key.name)
    cols = ",".join([compiler.preparer.quote(col) for col in add_primary_key.key])
    return f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({cols})"


@compiles(AddPrimaryKey, "duckdb")
def visit_add_primary_key(add_primary_key: AddPrimaryKey, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote(add_primary_key.table_name)
    schema = compiler.preparer.format_schema(add_primary_key.schema.get())
    pk_name = compiler.preparer.quote(add_primary_key.name)
    cols = ",".join([compiler.preparer.quote(col) for col in add_primary_key.key])
    return f"CREATE UNIQUE INDEX {pk_name} ON {schema}.{table} ({cols})"


@compiles(AddIndex)
def visit_add_index(add_index: AddIndex, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote(add_index.table_name)
    schema = compiler.preparer.format_schema(add_index.schema.get())
    index_name = compiler.preparer.quote(add_index.name)
    cols = ",".join([compiler.preparer.quote(col) for col in add_index.index])
    return f"CREATE INDEX {index_name} ON {schema}.{table} ({cols})"


@compiles(AddIndex, "ibm_db_sa")
def visit_add_index(add_index: AddIndex, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote(add_index.table_name)
    schema = compiler.preparer.format_schema(add_index.schema.get())
    index_name = compiler.preparer.quote(add_index.name)
    cols = ",".join([compiler.preparer.quote(col) for col in add_index.index])
    return f"CREATE INDEX {schema}.{index_name} ON {schema}.{table} ({cols})"


@compiles(ChangeColumnTypes)
def visit_change_column_types(change: ChangeColumnTypes, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    alter_columns = ",".join(
        [
            f"ALTER COLUMN {compiler.preparer.quote(col)} SET DATA TYPE"
            f" {compiler.type_compiler.process(_type)}"
            for col, _type, nullable in zip(
                change.column_names, change.column_types, change.nullable
            )
        ]
        + [
            "ALTER COLUMN"
            f" {compiler.preparer.quote(col)}"
            f" {'SET' if not nullable else 'DROP'} NOT NULL"
            for col, nullable in zip(change.column_names, change.nullable)
            if nullable is not None
        ]
    )
    return f"ALTER TABLE {schema}.{table} {alter_columns}"


@compiles(ChangeColumnTypes, "duckdb")
def visit_change_column_types_duckdb(change: ChangeColumnTypes, compiler, **kw):
    table = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    alter_columns = [
        f"ALTER COLUMN {compiler.preparer.quote(col)} SET DATA TYPE"
        f" {compiler.type_compiler.process(_type)}"
        for col, _type, nullable in zip(
            change.column_names, change.column_types, change.nullable
        )
    ] + [
        "ALTER COLUMN"
        f" {compiler.preparer.quote(col)}"
        f" {'SET' if not nullable else 'DROP'} NOT NULL"
        for col, nullable in zip(change.column_names, change.nullable)
        if nullable is not None
    ]
    statements = [
        f"ALTER TABLE {schema}.{table} {statement}" for statement in alter_columns
    ]
    return join_ddl_statements(statements, compiler, **kw)


@compiles(ChangeColumnTypes, "mssql")
def visit_change_column_types(change: ChangeColumnTypes, compiler, **kw):
    _ = kw

    table = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())

    def modify_type(_type):
        if change.cap_varchar_max is not None:
            _type = copy.copy(_type)
            if isinstance(_type, sa.String) and (
                _type.length is None or _type.length > change.cap_varchar_max
            ):
                # impose some limit to allow use in primary key / index
                _type.length = change.cap_varchar_max
        return _type

    statements = [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote(col)} "
        f"{compiler.type_compiler.process(modify_type(_type))}"
        f"{'' if nullable is None else ' NULL' if nullable else ' NOT NULL'}"
        for col, _type, nullable in zip(
            change.column_names, change.column_types, change.nullable
        )
    ]
    return join_ddl_statements(statements, compiler, **kw)


@compiles(ChangeColumnTypes, "ibm_db_sa")
def visit_change_column_types(change: ChangeColumnTypes, compiler, **kw):
    _ = kw

    def modify_type(_type):
        if change.cap_varchar_max is not None:
            _type = copy.copy(_type)
            if isinstance(_type, sa.String) and (
                _type.length is None or _type.length > change.cap_varchar_max
            ):
                # impose some limit to allow use in primary key / index
                _type.length = change.cap_varchar_max
        return _type

    table = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    statements = [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote(col)} SET DATA TYPE"
        f" {compiler.type_compiler.process(modify_type(_type))}"
        for col, _type, nullable in zip(
            change.column_names, change.column_types, change.nullable
        )
    ] + [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote(col)}"
        f" {'SET' if not nullable else 'DROP'} NOT NULL"
        for col, nullable in zip(change.column_names, change.nullable)
        if nullable is not None
    ]
    statements.append(f"call sysproc.admin_cmd('REORG TABLE {schema}.{table}')")
    return join_ddl_statements(statements, compiler, **kw)


@compiles(ChangeColumnNullable)
def visit_change_column_nullable(change: ChangeColumnNullable, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    alter_columns = ",".join(
        [
            "ALTER COLUMN"
            f" {compiler.preparer.quote(col)}"
            f" {'SET' if not nullable else 'DROP'} NOT NULL"
            for col, nullable in zip(change.column_names, change.nullable)
        ]
    )
    return f"ALTER TABLE {schema}.{table} {alter_columns}"


@compiles(ChangeColumnNullable, "ibm_db_sa")
def visit_change_column_nullable(change: ChangeColumnNullable, compiler, **kw):
    _ = kw
    statements = _get_nullable_change_statements(change, compiler)
    return join_ddl_statements(statements, compiler, **kw)


@compiles(ChangeTableLogged, "postgresql")
def visit_change_table_logged(change: ChangeTableLogged, compiler, **kw):
    _ = kw
    table_name = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    logged = "LOGGED" if change.logged else "UNLOGGED"
    return f"ALTER TABLE {schema}.{table_name} SET {logged}"


def _get_nullable_change_statements(change, compiler):
    table = compiler.preparer.quote(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    statements = [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote(col)}"
        f" {'SET' if not nullable else 'DROP'} NOT NULL"
        for col, nullable in zip(change.column_names, change.nullable)
    ]
    statements.append(f"call sysproc.admin_cmd('REORG TABLE {schema}.{table}')")
    return statements


def _mssql_update_definition(
    conn: sa.Connection,
    name: str,
    old_schema: Schema,
    new_schema: Schema,
) -> str:
    """
    Loads the definition of object `name` in `old_schema`, and replaces all references
    to `old_schema` inside the definition with `new_schema`.

    :return: updated definition referencing `new_schema` instead of `old_schema`.
    """

    # Get definition from database (using unquoted name)
    query = f"SELECT OBJECT_DEFINITION(OBJECT_ID(N'{old_schema.get()}.{name}'))"
    definition = conn.exec_driver_sql(query).scalar_one()

    # Replace unquoted names with quoted ones
    identifier_preparer = conn.dialect.identifier_preparer
    old_schema = identifier_preparer.format_schema(old_schema.get())
    new_schema = identifier_preparer.format_schema(new_schema.get())

    # Replace schema in definition with new destination schema
    string_literal = pp.QuotedString(quote_char="'", esc_quote="''")
    quoted_identifier = pp.QuotedString(
        quote_char="[", esc_quote="]]", end_quote_char="]"
    )

    schema_expr = pp.CaselessKeyword(old_schema).ignore(string_literal)
    if old_schema.startswith("["):
        schema_expr = schema_expr.ignore(quoted_identifier)
    else:
        schema_expr = schema_expr.ignore(quoted_identifier) | (
            pp.Literal("[") + schema_expr + pp.Literal("]")
        )

    schema_expr = schema_expr.set_parse_action(pp.replace_with(new_schema))
    expr = schema_expr + pp.FollowedBy(".")

    return expr.transform_string(definition)


STATEMENT_SEPERATOR = "; -- PYDIVERSE-PIPEDAG-SPLIT\n"


def join_ddl_statements(statements, compiler, **kw):
    """Mechanism to combine multiple DDL statements into one."""
    statement_strings = []
    kw["literal_binds"] = True
    for statement in statements:
        if isinstance(statement, str):
            statement_strings.append(statement)
        else:
            statement_strings.append(compiler.process(statement, **kw))
    return STATEMENT_SEPERATOR.join(statement_strings)


def split_ddl_statement(statement: str):
    """Split previously combined DDL statements apart"""
    return [
        statement
        for statement in statement.split(STATEMENT_SEPERATOR)
        if statement.strip() != ""
    ]
