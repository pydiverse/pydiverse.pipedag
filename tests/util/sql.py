from __future__ import annotations

import copy

import sqlalchemy as sa

from pydiverse.pipedag.backend import BaseTableStore
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


def get_config_with_table_store(
    base_cfg: ConfigContext, table_store_class: type[BaseTableStore]
):
    instance = base_cfg.instance_name
    flow = base_cfg.flow_name
    cfg = ConfigContext.new(
        copy.deepcopy(base_cfg._config_dict), base_cfg.pipedag_name, flow, instance
    )
    cfg._config_dict["table_store"]["class"] = table_store_class
    # this actually instantiates the table store
    table_store = cfg.store.table_store
    assert type(table_store) == table_store_class
    return cfg
