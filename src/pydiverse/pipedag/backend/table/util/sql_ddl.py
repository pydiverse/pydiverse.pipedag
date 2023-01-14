from __future__ import annotations

import copy
import re

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
    "CreateTableAsSelect",
    "CreateViewAsSelect",
    "CopyTable",
    "DropTable",
    "CreateDatabase",
    "DropFunction",
    "DropProcedure",
    "DropView",
    "AddPrimaryKey",
    "AddIndex",
    "ChangeColumnNullable",
    "ChangeColumnTypes",
    "split_ddl_statement",
]


@frozen
class Schema:
    name: str
    prefix: str
    suffix: str

    def get(self):
        return self.prefix + self.name + self.suffix


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
    def __init__(self, from_: Schema, to: Schema):
        self.from_ = from_
        self.to = to


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
    def __init__(self, name: str, schema: Schema, query: Select | TextClause | sa.Text):
        self.name = name
        self.schema = schema
        self.query = query


class CreateViewAsSelect(DDLElement):
    def __init__(self, name: str, schema: Schema, query: Select | TextClause | sa.Text):
        self.name = name
        self.schema = schema
        self.query = query


class CopyTable(DDLElement):
    def __init__(
        self,
        from_name,
        from_schema: Schema,
        to_name,
        to_schema: Schema,
        if_not_exists=False,
    ):
        self.from_name = from_name
        self.from_schema = from_schema
        self.to_name = to_name
        self.to_schema = to_schema
        self.if_not_exists = if_not_exists


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
        self.name = name


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
        self.name = name


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


@compiles(CreateSchema)
def visit_create_schema(create: CreateSchema, compiler, **kw):
    _ = kw
    schema = compiler.preparer.format_schema(create.schema.get())
    text = ["CREATE SCHEMA"]
    if create.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(schema)
    return " ".join(text)


# noinspection SqlDialectInspection
@compiles(CreateSchema, "mssql")
def visit_create_schema_mssql(create: CreateSchema, compiler, **kw):
    # For SQL Server we support two modes:  using databases as schemas,
    # or schemas as schemas.
    _ = kw
    if "." in create.schema.name:
        raise AttributeError(
            "We currently do not support dots in schema names "
            " when working with mssql database"
        )
    full_name = create.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    create_schema = f"CREATE SCHEMA {schema}"
    create_database = f"CREATE DATABASE {database}"
    if create.if_not_exists:
        create_database = f"""
            IF NOT EXISTS ( 
                SELECT * FROM sys.databases WHERE name = N'{database_name}'
            )
            BEGIN {create_database} END"""
        create_schema = f"""
            IF NOT EXISTS (
                SELECT * FROM sys.schema WHERE name = N'{schema_name}'
            )
            BEGIN {create_schema} END"""

    if "." in create.schema.prefix:
        # With prefix like "my_db." we create our stages as schemas
        # Attention: we have to rely on a preceding USE statement
        #            for correct prefix database
        return create_schema
    else:
        # With suffix like ".dbo" we create our stages as databases
        return create_database


@compiles(CreateSchema, "ibm_db_sa")
def visit_create_schema_ibm_db_sa(create: CreateSchema, compiler, **kw):
    """For IBM DB2 we need to jump through extra hoops for if_exists=True."""
    _ = kw
    schema = compiler.preparer.format_schema(create.schema.get())
    if create.if_not_exists:
        return (
            "BEGIN\ndeclare continue handler for sqlstate '42710' begin end;\n"
            f"execute immediate 'CREATE SCHEMA {schema}';\nEND"
        )
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
    full_name = drop.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    if "." in drop.schema.prefix:
        # With prefix like "my_db." we create our stages as schemas
        # Attention: we have to rely on a preceding USE statement
        #            for correct prefix database
        text = ["DROP SCHEMA"]
        name = compiler.preparer.format_schema(schema_name)
    else:
        # With suffix like ".dbo" we create our stages as databases
        text = ["DROP DATABASE"]
        name = compiler.preparer.format_schema(database_name)
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(name)
    return " ".join(text)


# noinspection SqlDialectInspection
@compiles(DropSchema, "ibm_db_sa")
def visit_drop_schema_ibm_db_sa(drop: DropSchema, compiler, **kw):
    """
    Because IBM DB2 doesn't support CASCADE, we must manually drop all tables in
    the schema first.
    """
    statements = []
    if drop.cascade:
        if drop.engine is None:
            raise ValueError(
                "Using DropSchema with cascade=True for ibm_db2 requires passing"
                " the engine kwarg to DropSchema."
            )

        with drop.engine.connect() as conn:
            meta = sa.MetaData()
            meta.reflect(bind=conn, schema=drop.schema.get())

        for table in meta.tables.values():
            statements.append(DropTable(table.name, drop.schema))

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
    _ = kw
    if rename.from_.prefix != rename.to.prefix:
        raise AttributeError(
            "We currently do not support varying schema prefixes for mssql database"
        )
    from_full_name = rename.from_.get()
    to_full_name = rename.to.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    from_database_name, from_schema_name = from_full_name.split(".")
    to_database_name, to_schema_name = to_full_name.split(".")
    if "." in rename.from_.prefix:
        # With prefix like "my_db." we create our stages as schemas
        raise NotImplementedError(
            "There is not SCHEMA rename expression for mssql. "
            "Needs to be addressed on higher level!"
        )
    else:
        # With suffix like ".dbo" we create our stages as databases
        from_name = compiler.preparer.format_schema(from_database_name)
        to_name = compiler.preparer.format_schema(to_database_name)
        return f"ALTER DATABASE {from_name} MODIFY NAME = {to_name}"


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
    name = compiler.preparer.quote_identifier(create.name)
    schema = compiler.preparer.format_schema(create.schema.get())
    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)
    return f"CREATE {_type} {schema}.{name} AS\n{prefix}{select}{suffix}"


@compiles(CreateTableAsSelect)
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    return _visit_create_obj_as_select(create, compiler, "TABLE", kw)


@compiles(CreateTableAsSelect, "mssql")
def visit_create_table_as_select_mssql(create: CreateTableAsSelect, compiler, **kw):
    name = compiler.preparer.quote_identifier(create.name)
    full_name = create.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)

    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)

    return insert_into_in_query(select, database, schema, name)


# noinspection SqlDialectInspection
@compiles(CreateTableAsSelect, "ibm_db_sa")
def visit_create_table_as_select_ibm_db_sa(create: CreateTableAsSelect, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    create = copy.deepcopy(create)
    create.name = ibm_db_sa_fix_name(create.name)

    prepare_statement = _visit_create_obj_as_select(
        create, compiler, "TABLE", kw, prefix="(", suffix=") DEFINITION ONLY"
    )

    name = compiler.preparer.quote_identifier(create.name)
    schema = compiler.preparer.format_schema(create.schema.get())
    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)
    create_statement = f"INSERT INTO {schema}.{name}\n{select}"

    return join_ddl_statements([prepare_statement, create_statement], compiler, **kw)


@compiles(CreateViewAsSelect)
def visit_create_view_as_select(create: CreateViewAsSelect, compiler, **kw):
    return _visit_create_obj_as_select(create, compiler, "VIEW", kw)


@compiles(CreateViewAsSelect, "ibm_db_sa")
def visit_create_view_as_select_ibm_db_sa(create: CreateViewAsSelect, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    create = copy.deepcopy(create)
    create.name = ibm_db_sa_fix_name(create.name)
    return _visit_create_obj_as_select(create, compiler, "VIEW", kw)


def insert_into_in_query(select_sql, database, schema, table):
    into = f"INTO {database}.{schema}.{table}"
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


@compiles(CopyTable)
def visit_copy_table(copy_table: CopyTable, compiler, **kw):
    from_name = compiler.preparer.quote_identifier(copy_table.from_name)
    from_schema = compiler.preparer.format_schema(copy_table.from_schema.get())
    query = sa.select("*").select_from(sa.text(f"{from_schema}.{from_name}"))
    create = CreateTableAsSelect(copy_table.to_name, copy_table.to_schema, query)
    return compiler.process(create, **kw)


# noinspection SqlDialectInspection
@compiles(CopyTable, "mssql")
def visit_copy_table_mssql(copy_table: CopyTable, compiler, **kw):
    from_name = compiler.preparer.quote_identifier(copy_table.from_name)
    database, schema = _get_mssql_database_schema(copy_table.from_schema, compiler)
    query = sa.text(f"SELECT * FROM {database}.{schema}.{from_name}")
    create = CreateTableAsSelect(copy_table.to_name, copy_table.to_schema, query)
    return compiler.process(create, **kw)


@compiles(CopyTable, "ibm_db_sa")
def visit_copy_table_ibm_db_sa(copy_table: CopyTable, compiler, **kw):
    copy_table = copy.deepcopy(copy_table)
    copy_table.from_name = ibm_db_sa_fix_name(copy_table.from_name)
    copy_table.to_name = ibm_db_sa_fix_name(copy_table.to_name)
    return visit_copy_table(copy_table, compiler, **kw)


@compiles(DropTable)
def visit_drop_table(drop: DropTable, compiler, **kw):
    return _visit_drop_anything(drop, "TABLE", compiler, **kw)


@compiles(DropTable, "mssql")
def visit_drop_table_mssql(drop: DropTable, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "TABLE", compiler, **kw)


@compiles(DropTable, "ibm_db_sa")
def visit_drop_table_ibm_db_sa(drop: DropTable, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    drop = copy.deepcopy(drop)
    drop.name = ibm_db_sa_fix_name(drop.name)
    return _visit_drop_anything(drop, "TABLE", compiler, **kw)


@compiles(DropView)
def visit_drop_view(drop: DropView, compiler, **kw):
    return _visit_drop_anything(drop, "VIEW", compiler, **kw)


@compiles(DropView, "mssql")
def visit_drop_view_mssql(drop: DropView, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "VIEW", compiler, **kw)


@compiles(DropView, "ibm_db_sa")
def visit_drop_view_ibm_db_sa(drop: DropView, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    drop = copy.deepcopy(drop)
    drop.name = ibm_db_sa_fix_name(drop.name)
    return _visit_drop_anything(drop, "VIEW", compiler, **kw)


@compiles(DropProcedure)
def visit_drop_table(drop: DropProcedure, compiler, **kw):
    return _visit_drop_anything(drop, "PROCEDURE", compiler, **kw)


@compiles(DropProcedure, "mssql")
def visit_drop_table_mssql(drop: DropProcedure, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "PROCEDURE", compiler, **kw)


@compiles(DropFunction)
def visit_drop_table(drop: DropFunction, compiler, **kw):
    return _visit_drop_anything(drop, "FUNCTION", compiler, **kw)


@compiles(DropFunction, "mssql")
def visit_drop_table_mssql(drop: DropProcedure, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "FUNCTION", compiler, **kw)


def _visit_drop_anything(
    drop: DropTable | DropView | DropProcedure | DropFunction,
    _type,
    compiler,
    dont_quote_table=False,
    **kw,
):
    _ = kw
    if dont_quote_table:
        table = drop.name
    else:
        table = compiler.preparer.quote_identifier(drop.name)
    schema = compiler.preparer.format_schema(drop.schema.get())
    text = [f"DROP {_type}"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(f"{schema}.{table}")
    return " ".join(text)


def _visit_drop_anything_mssql(
    drop: DropTable | DropView | DropProcedure | DropFunction, _type, compiler, **kw
):
    _ = kw
    table = compiler.preparer.quote_identifier(drop.name)
    database, schema = _get_mssql_database_schema(drop.schema, compiler)
    text = [f"DROP {_type}"]
    if drop.if_exists:
        text.append("IF EXISTS")
    if isinstance(drop, (DropView, DropProcedure, DropFunction)):
        # attention: this statement must be prefixed with a 'USE <database>' statement
        text.append(f"{schema}.{table}")
    else:
        text.append(f"{database}.{schema}.{table}")
    return " ".join(text)


# noinspection SqlDialectInspection
@compiles(AddPrimaryKey)
def visit_add_primary_key(add_primary_key: AddPrimaryKey, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote_identifier(add_primary_key.table_name)
    schema = compiler.preparer.format_schema(add_primary_key.schema.get())
    pk_name = compiler.preparer.quote_identifier(
        add_primary_key.name
        if add_primary_key.name is not None
        else "pk_" + "_".join(add_primary_key.key)
    )
    cols = ",".join(
        [compiler.preparer.quote_identifier(col) for col in add_primary_key.key]
    )
    return f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({cols})"


# noinspection SqlDialectInspection
@compiles(AddPrimaryKey, "mssql")
def visit_add_primary_key(add_primary_key: AddPrimaryKey, compiler, **kw):
    _ = kw
    database, schema = _get_mssql_database_schema(add_primary_key.schema, compiler)

    table = compiler.preparer.quote_identifier(add_primary_key.table_name)
    pk_name = compiler.preparer.quote_identifier(
        add_primary_key.name
        if add_primary_key.name is not None
        else "pk_" + "_".join(add_primary_key.key)
    )
    cols = ",".join(
        [compiler.preparer.quote_identifier(col) for col in add_primary_key.key]
    )
    return (
        f"ALTER TABLE {database}.{schema}.{table} ADD CONSTRAINT {pk_name} PRIMARY KEY"
        f" ({cols})"
    )


# noinspection SqlDialectInspection
@compiles(AddPrimaryKey, "ibm_db_sa")
def visit_add_primary_key(add_primary_key: AddPrimaryKey, compiler, **kw):
    _ = kw
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    add_primary_key = copy.deepcopy(add_primary_key)
    add_primary_key.table_name = ibm_db_sa_fix_name(add_primary_key.table_name)

    table = compiler.preparer.quote_identifier(add_primary_key.table_name)
    schema = compiler.preparer.format_schema(add_primary_key.schema.get())
    pk_name = compiler.preparer.quote_identifier(
        add_primary_key.name
        if add_primary_key.name is not None
        else "pk_" + "_".join(add_primary_key.key)
    )
    cols = ",".join(
        [
            compiler.preparer.quote_identifier(ibm_db_sa_fix_name(col))
            for col in add_primary_key.key
        ]
    )
    return f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({cols})"


# noinspection SqlDialectInspection
@compiles(AddIndex)
def visit_add_index(add_index: AddIndex, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote_identifier(add_index.table_name)
    schema = compiler.preparer.format_schema(add_index.schema.get())
    index_name = compiler.preparer.quote_identifier(
        add_index.name
        if add_index.name is not None
        else "idx_" + "_".join(add_index.index)
    )
    cols = ",".join(
        [compiler.preparer.quote_identifier(col) for col in add_index.index]
    )
    return f"CREATE INDEX {index_name} ON {schema}.{table} ({cols})"


# noinspection SqlDialectInspection
@compiles(AddIndex, "mssql")
def visit_add_index(add_index: AddIndex, compiler, **kw):
    _ = kw
    database, schema = _get_mssql_database_schema(add_index.schema, compiler)

    table = compiler.preparer.quote_identifier(add_index.table_name)
    index_name = compiler.preparer.quote_identifier(
        add_index.name
        if add_index.name is not None
        else "idx_" + "_".join(add_index.index)
    )
    cols = ",".join(
        [compiler.preparer.quote_identifier(col) for col in add_index.index]
    )
    return f"CREATE INDEX {index_name} ON {database}.{schema}.{table} ({cols})"


# noinspection SqlDialectInspection
@compiles(AddIndex, "ibm_db_sa")
def visit_add_index(add_index: AddIndex, compiler, **kw):
    _ = kw
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    add_index = copy.deepcopy(add_index)
    add_index.table_name = ibm_db_sa_fix_name(add_index.table_name)

    table = compiler.preparer.quote_identifier(add_index.table_name)
    schema = compiler.preparer.format_schema(add_index.schema.get())
    index_name = compiler.preparer.quote_identifier(
        add_index.name
        if add_index.name is not None
        else "idx_" + "_".join(add_index.index)
    )
    cols = ",".join(
        [
            compiler.preparer.quote_identifier(ibm_db_sa_fix_name(col))
            for col in add_index.index
        ]
    )
    return f"CREATE INDEX {schema}.{index_name} ON {schema}.{table} ({cols})"


@compiles(ChangeColumnTypes)
def visit_change_column_types(change: ChangeColumnTypes, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote_identifier(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    alter_columns = ",".join(
        [
            f"ALTER COLUMN {compiler.preparer.quote_identifier(col)} SET DATA TYPE"
            f" {compiler.type_compiler.process(_type)}"
            for col, _type, nullable in zip(
                change.column_names, change.column_types, change.nullable
            )
        ]
        + [
            "ALTER COLUMN"
            f" {compiler.preparer.quote_identifier(col)}"
            f" {'SET' if not nullable else 'DROP'} NOT NULL"
            for col, nullable in zip(change.column_names, change.nullable)
            if nullable is not None
        ]
    )
    return f"ALTER TABLE {schema}.{table} {alter_columns}"


# noinspection SqlDialectInspection
@compiles(ChangeColumnTypes, "mssql")
def visit_change_column_types(change: ChangeColumnTypes, compiler, **kw):
    _ = kw
    database, schema = _get_mssql_database_schema(change.schema, compiler)

    table = compiler.preparer.quote_identifier(change.table_name)

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
        f"ALTER TABLE {database}.{schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote_identifier(col)} "
        f"{compiler.type_compiler.process(modify_type(_type))}"
        f"{'' if nullable is None else ' NULL' if nullable else ' NOT NULL'}"
        for col, _type, nullable in zip(
            change.column_names, change.column_types, change.nullable
        )
    ]
    return join_ddl_statements(statements, compiler, **kw)


# noinspection SqlDialectInspection
@compiles(ChangeColumnTypes, "ibm_db_sa")
def visit_change_column_types(change: ChangeColumnTypes, compiler, **kw):
    _ = kw
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    change = copy.deepcopy(change)
    change.table_name = ibm_db_sa_fix_name(change.table_name)

    def modify_type(_type):
        if change.cap_varchar_max is not None:
            _type = copy.copy(_type)
            if isinstance(_type, sa.String) and (
                _type.length is None or _type.length > change.cap_varchar_max
            ):
                # impose some limit to allow use in primary key / index
                _type.length = change.cap_varchar_max
        return _type

    table = compiler.preparer.quote_identifier(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    statements = [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote_identifier(ibm_db_sa_fix_name(col))} SET DATA TYPE"
        f" {compiler.type_compiler.process(modify_type(_type))}"
        for col, _type, nullable in zip(
            change.column_names, change.column_types, change.nullable
        )
    ] + [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote_identifier(ibm_db_sa_fix_name(col))}"
        f" {'SET' if not nullable else 'DROP'} NOT NULL"
        for col, nullable in zip(change.column_names, change.nullable)
        if nullable is not None
    ]
    statements.append(f"call sysproc.admin_cmd('REORG TABLE {schema}.{table}')")
    return join_ddl_statements(statements, compiler, **kw)


# noinspection SqlDialectInspection
@compiles(ChangeColumnNullable)
def visit_change_column_types(change: ChangeColumnNullable, compiler, **kw):
    _ = kw
    table = compiler.preparer.quote_identifier(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    alter_columns = ",".join(
        [
            "ALTER COLUMN"
            f" {compiler.preparer.quote_identifier(col)}"
            f" {'SET' if not nullable else 'DROP'} NOT NULL"
            for col, nullable in zip(change.column_names, change.nullable)
        ]
    )
    return f"ALTER TABLE {schema}.{table} {alter_columns}"


# noinspection SqlDialectInspection
@compiles(ChangeColumnNullable, "ibm_db_sa")
def visit_change_column_types(change: ChangeColumnNullable, compiler, **kw):
    _ = kw
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    change = copy.deepcopy(change)
    change.table_name = ibm_db_sa_fix_name(change.table_name)

    table = compiler.preparer.quote_identifier(change.table_name)
    schema = compiler.preparer.format_schema(change.schema.get())
    statements = [
        f"ALTER TABLE {schema}.{table} ALTER COLUMN"
        f" {compiler.preparer.quote_identifier(ibm_db_sa_fix_name(col))}"
        f" {'SET' if not nullable else 'DROP'} NOT NULL"
        for col, nullable in zip(change.column_names, change.nullable)
    ]
    statements.append(f"call sysproc.admin_cmd('REORG TABLE {schema}.{table}')")
    return join_ddl_statements(statements, compiler, **kw)


def ibm_db_sa_fix_name(name):
    # DB2 seems to create tables uppercase if all lowercase given
    return name.upper() if name.islower() else name


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
    return statement.split(STATEMENT_SEPERATOR)


def _get_mssql_database_schema(schema: Schema, compiler):
    full_name = schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    return database, schema
