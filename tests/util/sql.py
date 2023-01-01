import sqlalchemy as sa

from pydiverse.pipedag.context import ConfigContext


def select_as(value, as_):
    return sa.select(sa.literal(value).label(as_))


def compile_sql(query):
    engine = ConfigContext.get().store.table_store.engine
    return str(query.compile(engine, compile_kwargs={"literal_binds": True}))
