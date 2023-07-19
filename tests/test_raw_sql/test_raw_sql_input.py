from __future__ import annotations

from pathlib import Path

import pandas as pd

from pydiverse.pipedag import Flow, Stage, materialize
from pydiverse.pipedag.context import ConfigContext
from tests.fixtures.instances import with_instances
from tests.test_raw_sql.util import sql_script


@materialize(input_type=pd.DataFrame)
def raw_sql_object(raw_sql):
    df_1 = raw_sql["table_1"]
    df_2 = raw_sql["table_2"]

    assert not df_1.empty
    assert not df_2.empty


@materialize(input_type=pd.DataFrame)
def raw_sql_individual_table(df_1):
    assert not df_1.empty


@with_instances("postgres", "mssql")
def test_raw_sql_task_input():
    instance_name = ConfigContext.get().instance_name
    dir_ = Path(__file__).parent / "scripts" / instance_name / "create_tables"

    with Flow() as f:
        with Stage("raw_0"):
            simple_tables = sql_script("simple_tables.sql", dir_)

            raw_sql_object(simple_tables)
            raw_sql_individual_table(simple_tables["table_1"])
            raw_sql_individual_table(simple_tables["table_2"])

    f.run()
    f.run()
