# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

# %%
from IPython.core.getipython import get_ipython

from pydiverse.pipedag import PipedagConfig


def _init_ipython():
    """Initialise an interactive python script to use out duckdb connection."""
    ip = get_ipython()

    if ip is None:
        return

    ip.run_line_magic("config", "SqlMagic.feedback = False")
    ip.run_line_magic("config", "SqlMagic.displaycon = False")
    ip.run_line_magic("config", "SqlMagic.autolimit = 1000")
    ip.run_line_magic("config", "SqlMagic.displaylimit = 1000")

    ip.run_line_magic("load_ext", "sql")
    return ip


def _init_sql_magic_conn(ip, engine):
    conn_url = str(engine.url)  # e.g. "duckdb:///path/to/db.duckdb"
    ip.run_line_magic("sql", conn_url)


def initialize_notebook(instance: str | None = None):
    """Initialize an interactive script to work with our duckdb instances."""
    pipedag_cfg = PipedagConfig.default
    instance_cfg = pipedag_cfg.get(instance)
    store = instance_cfg.store.table_store
    store.setup()
    store.sync_metadata()

    engine = store.engine

    ip = _init_ipython()
    _init_sql_magic_conn(ip, engine)
    return engine


# %%
