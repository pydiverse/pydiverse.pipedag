# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from pydiverse.pipedag import ConfigContext


def compile_sql(query):
    engine = ConfigContext.get().store.table_store.engine
    return str(query.compile(engine, compile_kwargs={"literal_binds": True}))
