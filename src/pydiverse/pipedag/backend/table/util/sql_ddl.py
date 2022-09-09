from __future__ import annotations

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
    "CopyTable",
    "DropTable",
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
    def __init__(self, schema: Schema, if_exists=False, cascade=False):
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade


class RenameSchema(DDLElement):
    def __init__(self, from_: Schema, to: Schema):
        self.from_ = from_
        self.to = to


class CreateTableAsSelect(DDLElement):
    def __init__(self, name: str, schema: Schema, query: Select):
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


@compiles(CreateSchema)
def visit_create_schema(create: CreateSchema, compiler, **kw):
    schema = compiler.preparer.format_schema(create.schema.get())
    text = ["CREATE SCHEMA"]
    if create.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(schema)
    return " ".join(text)


@compiles(CreateSchema, "mssql")
def visit_create_schema(create: CreateSchema, compiler, **kw):
    """For SQL Server we support two modes: using databases as schemas or schemas as schemas.
    """
    if "." in create.schema.name:
        raise AttributeError(
            f"We currently do not support dots in schema names when working with mssql"
            f" database"
        )
    full_name = create.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    create_schema = f"CREATE SCHEMA {schema}"
    create_database = f"CREATE DATABASE {database}"
    if create.if_not_exists:
        create_database = (
            f"""IF NOT EXISTS ( SELECT * FROM sys.databases WHERE name = N'{database_name}') """
            f"""BEGIN {create_database} END"""
        )
        create_schema = (
            f"""IF NOT EXISTS ( SELECT * FROM sys.schema WHERE name = N'{schema_name}') """
            f"""BEGIN {create_schema} END"""
        )

    if "." in create.schema.prefix:
        # With prefix like "my_db." we create our stages as schemas
        # Attention: we have to rely on a preceding USE statement for correct prefix database
        return create_schema
    else:
        # With suffix like ".dbo" we create our stages as databases
        return create_database


@compiles(DropSchema)
def visit_drop_schema(drop: DropSchema, compiler, **kw):
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
    full_name = drop.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    if "." in drop.schema.prefix:
        # With prefix like "my_db." we create our stages as schemas
        # Attention: we have to rely on a preceding USE statement for correct prefix database
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


@compiles(RenameSchema)
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
    from_ = compiler.preparer.format_schema(rename.from_.get())
    to = compiler.preparer.format_schema(rename.to.get())
    return "ALTER SCHEMA " + from_ + " RENAME TO " + to


@compiles(RenameSchema, "mssql")
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
    if rename.from_.prefix != rename.to.prefix:
        raise AttributeError(
            f"We currently do not support varying schema prefixes for mssql database"
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


@compiles(CreateTableAsSelect)
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    name = compiler.preparer.quote_identifier(create.name)
    schema = compiler.preparer.format_schema(create.schema.get())

    kw = kw.copy()
    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)

    return f"CREATE TABLE {schema}.{name} AS\n{select}"


@compiles(CreateTableAsSelect, "mssql")
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    name = compiler.preparer.quote_identifier(create.name)
    full_name = create.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)

    kw = kw.copy()
    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)

    into = f"INTO {database}.{schema}.{name}"
    # Attention: this code assumes no subqueries in columns before FROM (can be fixed by counting FROMs)
    # TODO: fix this code for more queries without FROM and make it case insensitive
    return select.replace("FROM", into + " FROM", 1) if "FROM" in select else select + " " + into


# noinspection SqlDialectInspection
@compiles(CopyTable)
def visit_copy_table(copy: CopyTable, compiler, **kw):
    from_name = compiler.preparer.quote_identifier(copy.from_name)
    from_schema = compiler.preparer.format_schema(copy.from_schema.get())
    query = sa.text(f"SELECT * FROM {from_schema}.{from_name}")
    create = CreateTableAsSelect(copy.to_name, copy.to_schema, query)
    return compiler.process(create, **kw)


# noinspection SqlDialectInspection
@compiles(CopyTable, "mssql")
def visit_copy_table(copy: CopyTable, compiler, **kw):
    from_name = compiler.preparer.quote_identifier(copy.from_name)
    full_name = copy.from_schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    query = sa.text(f"SELECT * FROM {database}.{schema}.{from_name}")
    create = CreateTableAsSelect(copy.to_name, copy.to_schema, query)
    return compiler.process(create, **kw)


@compiles(DropTable)
def visit_drop_table(drop: DropTable, compiler, **kw):
    table = compiler.preparer.quote_identifier(drop.name)
    schema = compiler.preparer.format_schema(drop.schema.get())
    text = ["DROP TABLE"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(f"{schema}.{table}")
    return " ".join(text)


@compiles(DropTable, "mssql")
def visit_drop_table(drop: DropTable, compiler, **kw):
    table = compiler.preparer.quote_identifier(drop.name)
    full_name = drop.schema.get()
    # it was already checked that there is exactly one dot in schema prefix + suffix
    database_name, schema_name = full_name.split(".")
    database = compiler.preparer.format_schema(database_name)
    schema = compiler.preparer.format_schema(schema_name)
    text = ["DROP TABLE"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(f"{database}.{schema}.{table}")
    return " ".join(text)
