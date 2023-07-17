from __future__ import annotations

import sqlalchemy as sa

from pydiverse.pipedag.context import ConfigContext


def select_as(value, as_):
    return sa.select(sa.literal(value).label(as_))


def sql_table_expr(cols: dict):
    num_values = {len(vals) for vals in cols.values()}
    assert len(num_values) == 1

    queries = []
    num_values = num_values.pop()
    for i in range(num_values):
        literals = []
        for col, vals in cols.items():
            literals.append(sa.literal(vals[i]).label(col))

        queries.append(sa.select(*literals))

    return sa.union_all(*queries)


def compile_sql(query):
    engine = ConfigContext.get().store.table_store.engine
    return str(query.compile(engine, compile_kwargs={"literal_binds": True}))
