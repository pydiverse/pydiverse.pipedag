from __future__ import annotations

import copy
import re

import sqlalchemy as sa
from attr import frozen
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import Select

__all__ = [
    "Schema",
    "CreateSchema",
    "DropSchema",
    "RenameSchema",
    "CreateTableAsSelect",
    "PrepareCreateTableAsSelect",
    "CreateViewAsSelect",
    "CopyTable",
    "DropTable",
    "CreateDatabase",
    "DropFunction",
    "DropProcedure",
    "DropView",
]

from sqlalchemy.sql.elements import TextClause


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


class PrepareCreateTableAsSelect(DDLElement):
    """Prepare a CreateTableAsSelect statement for DB2."""

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
def visit_create_schema(create: CreateSchema, compiler, **kw):
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
def visit_create_schema(create: CreateSchema, compiler, **kw):
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
def visit_drop_schema(drop: DropSchema, compiler, **kw):
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


@compiles(DropSchema, "ibm_db_sa")
def visit_drop_schema(drop: DropSchema, compiler, **kw):
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

        kw["literal_binds"] = True
        for table in meta.tables.values():
            drop_table = compiler.process(DropTable(table.name, drop.schema), **kw)
            statements.append(drop_table)

    # Compile DROP SCHEMA statement
    schema = compiler.preparer.format_schema(drop.schema.get())
    text = ["DROP SCHEMA"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(schema)
    statements.append(" ".join(text))

    return ";\n".join(statements)


@compiles(RenameSchema)
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
    _ = kw
    from_ = compiler.preparer.format_schema(rename.from_.get())
    to = compiler.preparer.format_schema(rename.to.get())
    return "ALTER SCHEMA " + from_ + " RENAME TO " + to


@compiles(RenameSchema, "mssql")
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
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
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    name = compiler.preparer.quote_identifier(create.name)
    full_name = create.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)

    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)

    return insert_into_in_query(select, database, schema, name)


@compiles(CreateTableAsSelect, "ibm_db_sa")
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    create_name = ibm_db_sa_fix_name(create.name)

    name = compiler.preparer.quote_identifier(create_name)
    schema = compiler.preparer.format_schema(create.schema.get())
    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)
    return f"INSERT INTO {schema}.{name}\n{select}"


@compiles(PrepareCreateTableAsSelect, "ibm_db_sa")
def visit_prepare_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    create = copy.deepcopy(create)
    create.name = ibm_db_sa_fix_name(create.name)
    return _visit_create_obj_as_select(
        create, compiler, "TABLE", kw, prefix="(", suffix=") DEFINITION ONLY"
    )


# noinspection DuplicatedCode
@compiles(CreateViewAsSelect)
def visit_create_view_as_select(create: CreateViewAsSelect, compiler, **kw):
    return _visit_create_obj_as_select(create, compiler, "VIEW", kw)


@compiles(CreateViewAsSelect, "ibm_db_sa")
def visit_create_view_as_select(create: CreateViewAsSelect, compiler, **kw):
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


@compiles(CopyTable, "mssql")
def visit_copy_table(copy_table: CopyTable, compiler, **kw):
    from_name = compiler.preparer.quote_identifier(copy_table.from_name)
    full_name = copy_table.from_schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    query = sa.text(f"SELECT * FROM {database}.{schema}.{from_name}")
    create = CreateTableAsSelect(copy_table.to_name, copy_table.to_schema, query)
    return compiler.process(create, **kw)


@compiles(DropTable)
def visit_drop_table(drop: DropTable, compiler, **kw):
    return _visit_drop_anything(drop, "TABLE", compiler, **kw)


@compiles(DropTable, "mssql")
def visit_drop_table(drop: DropTable, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "TABLE", compiler, **kw)


@compiles(DropTable, "ibm_db_sa")
def visit_drop_table(drop: DropTable, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    drop = copy.deepcopy(drop)
    drop.name = ibm_db_sa_fix_name(drop.name)
    return _visit_drop_anything(drop, "TABLE", compiler, **kw)


@compiles(DropView)
def visit_drop_view(drop: DropView, compiler, **kw):
    return _visit_drop_anything(drop, "VIEW", compiler, **kw)


@compiles(DropView, "mssql")
def visit_drop_view(drop: DropView, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "VIEW", compiler, **kw)


@compiles(DropView, "ibm_db_sa")
def visit_drop_view(drop: DropView, compiler, **kw):
    # DB2 stores capitalized table names but sqlalchemy reflects them lowercase
    drop = copy.deepcopy(drop)
    drop.name = ibm_db_sa_fix_name(drop.name)
    return _visit_drop_anything(drop, "VIEW", compiler, **kw)


@compiles(DropProcedure)
def visit_drop_table(drop: DropProcedure, compiler, **kw):
    return _visit_drop_anything(drop, "PROCEDURE", compiler, **kw)


@compiles(DropProcedure, "mssql")
def visit_drop_table(drop: DropProcedure, compiler, **kw):
    return _visit_drop_anything_mssql(drop, "PROCEDURE", compiler, **kw)


@compiles(DropFunction)
def visit_drop_table(drop: DropFunction, compiler, **kw):
    return _visit_drop_anything(drop, "FUNCTION", compiler, **kw)


@compiles(DropFunction, "mssql")
def visit_drop_table(drop: DropProcedure, compiler, **kw):
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
    full_name = drop.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    text = [f"DROP {_type}"]
    if drop.if_exists:
        text.append("IF EXISTS")
    if isinstance(drop, (DropView, DropProcedure, DropFunction)):
        # attention: this statement must be prefixed with a 'USE <database>' statement
        text.append(f"{schema}.{table}")
    else:
        text.append(f"{database}.{schema}.{table}")
    return " ".join(text)


def ibm_db_sa_fix_name(name):
    # DB2 seems to create tables uppercase if all lowercase given
    return name.upper() if name.islower() else name
