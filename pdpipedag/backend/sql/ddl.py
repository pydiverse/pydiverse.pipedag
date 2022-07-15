from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement

__all__ = [
    'CreateSchema',
    'DropSchema',
    'RenameSchema',
    'CopyTable',
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

class CopyTable(DDLElement):
    def __init__(self, name, from_schema, to_schema, if_not_exists=False):
        self.name = name
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.if_not_exists = if_not_exists


@compiles(CreateSchema)
def visit_create_schema(create: CreateSchema, compiler, **kw):
    schema = compiler.preparer.format_schema(create.name)
    text = ["CREATE SCHEMA"]
    if create.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(schema)
    return ' '.join(text)

@compiles(DropSchema)
def visit_drop_schema(drop: DropSchema, compiler, **kw):
    schema = compiler.preparer.format_schema(drop.name)
    text = ["DROP SCHEMA"]
    if drop.if_exists:
        text.append("IF EXISTS")
    text.append(schema)
    if drop.cascade:
        text.append("CASCADE")
    return ' '.join(text)

@compiles(RenameSchema)
def visit_rename_schema(rename: RenameSchema, compiler, **kw):
    _from = compiler.preparer.format_schema(rename._from)
    to = compiler.preparer.format_schema(rename.to)
    return "ALTER SCHEMA " + _from + " RENAME TO " + to

@compiles(CopyTable)
def visit_copy_table(copy: CopyTable, compiler, *kw):
    name = compiler.preparer.quote_identifier(copy.name)
    from_schema = compiler.preparer.format_schema(copy.from_schema)
    to_schema = compiler.preparer.format_schema(copy.to_schema)

    text = ["CREATE TABLE"]
    if copy.if_not_exists:
        text.append("IF NOT EXISTS")
    text.append(f"{to_schema}.{name}")
    text.append("AS")

    return ' '.join(text) + f"\nSELECT * FROM {from_schema}.{name}"
