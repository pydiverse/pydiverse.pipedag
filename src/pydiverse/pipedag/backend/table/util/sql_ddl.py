from __future__ import annotations

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import Select

__all__ = [
    "CreateSchema",
    "DropSchema",
    "RenameSchema",
    "CreateTableAsSelect",
    "CopyTable",
    "DropTable",
]


class CreateSchema(DDLElement):
    def __init__(self, name, if_not_exists=False):
        self.name = name
        self.if_not_exists = if_not_exists


class DropSchema(DDLElement):
    def __init__(self, name, if_exists=False, cascade=False):
        self.name = name
        self.if_exists = if_exists
        self.cascade = cascade


class RenameSchema(DDLElement):
    def __init__(self, _from, to):
        self._from = _from
        self.to = to


class CreateTableAsSelect(DDLElement):
    def __init__(self, name: str, schema: str, query: Select):
        self.name = name
        self.schema = schema
        self.query = query


class CopyTable(DDLElement):
    def __init__(self, from_name, from_schema, to_name, to_schema, if_not_exists=False):
        self.from_name = from_name
        self.from_schema = from_schema
        self.to_name = to_name
        self.to_schema = to_schema
        self.if_not_exists = if_not_exists


class DropTable(DDLElement):
    def __init__(self, name, schema, if_exists=False):
        self.name = name
        self.schema = schema
        self.if_exists = if_exists


@compiles(CreateSchema)
def visit_create_schema(create: CreateSchema, compiler, **kw):
    schema = compiler.preparer.format_schema(create.name)
    text = ["CREATE SCHEMA"]
    if create.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(schema)
    return " ".join(text)


@compiles(DropSchema)
def visit_drop_schema(drop: DropSchema, compiler, **kw):
    schema = compiler.preparer.format_schema(drop.name)
    text = ["DROP SCHEMA"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(schema)
    if drop.cascade:
        text.append("CASCADE")
    return " ".join(text)


@compiles(RenameSchema)
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
    _from = compiler.preparer.format_schema(rename._from)
    to = compiler.preparer.format_schema(rename.to)
    return "ALTER SCHEMA " + _from + " RENAME TO " + to


@compiles(CreateTableAsSelect)
def visit_create_table_as_select(create: CreateTableAsSelect, compiler, **kw):
    name = compiler.preparer.quote_identifier(create.name)
    schema = compiler.preparer.format_schema(create.schema)

    kw["literal_binds"] = True
    select = compiler.sql_compiler.process(create.query, **kw)

    return f"CREATE TABLE {schema}.{name} AS\n{select}"


@compiles(CopyTable)
def visit_copy_table(copy: CopyTable, compiler, *kw):
    from_name = compiler.preparer.quote_identifier(copy.from_name)
    to_name = compiler.preparer.quote_identifier(copy.to_name)
    from_schema = compiler.preparer.format_schema(copy.from_schema)
    to_schema = compiler.preparer.format_schema(copy.to_schema)

    text = ["CREATE TABLE"]
    if copy.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(f"{to_schema}.{to_name}")
    text.append("AS")

    return " ".join(text) + f"\nSELECT * FROM {from_schema}.{from_name}"


@compiles(DropTable)
def visit_drop_table(drop: DropTable, compiler, **kw):
    table = compiler.preparer.quote_identifier(drop.name)
    schema = compiler.preparer.format_schema(drop.schema)
    text = ["DROP TABLE"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(f"{schema}.{table}")
    return " ".join(text)
