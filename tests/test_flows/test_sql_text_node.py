from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize, PipedagConfig
from pydiverse.pipedag.context import ConfigContext


@materialize(input_type=sa.Table, lazy=True)
def table_1(script_path: str):
    sql = Path(script_path).read_text()
    return Table(sa.text(sql), name="table_1")


@materialize(input_type=sa.Table, lazy=True)
def table_2(script_path: str, dependent_table: Table):
    sql = (
        Path(script_path)
        .read_text()
        .replace("{{dependent}}", str(dependent_table.original))
    )
    return Table(sa.text(sql), name="test_table2")


@materialize(input_type=pd.DataFrame, lazy=True)
def assert_result(df: pd.DataFrame):
    pd.testing.assert_frame_equal(
        df, pd.DataFrame({"coltab2": [24]}).astype(pd.Int32Dtype())
    )


def test_sql_node():
    # use a different way to initialize configuration for more test coverage
    with PipedagConfig.default.get(per_user=True):
        with Flow("FLOW") as flow:
            with Stage("schema1"):
                cfg = ConfigContext.get()
                print(f"pipedag_name={cfg.pipedag_name}")
                parent_dir = Path(__file__).parent
                tab1 = table_1(str(parent_dir / "sql_scripts" / "script1.sql"))
                tab2 = table_2(str(parent_dir / "sql_scripts" / "script2.sql"), tab1)
                assert_result(tab2)

        flow_result = flow.run()
        assert flow_result.successful


# noinspection DuplicatedCode
@pytest.mark.mssql
def test_sql_node_mssql():
    # use a different way to initialize configuration for more test coverage
    with PipedagConfig.default.get(instance="mssql", per_user=True):
        with Flow() as flow:
            with Stage("schema1"):
                parent_dir = Path(__file__).parent
                tab1 = table_1(str(parent_dir / "sql_scripts" / "script1.sql"))
                tab2 = table_2(str(parent_dir / "sql_scripts" / "script2.sql"), tab1)
                assert_result(tab2)

        flow_result = flow.run()
        assert flow_result.successful


# noinspection DuplicatedCode
@pytest.mark.ibm_db2
def test_sql_node_db2():
    # use a different way to initialize configuration for more test coverage
    with PipedagConfig.default.get(instance="ibm_db2", per_user=True):
        with Flow() as flow:
            with Stage("schema1"):
                parent_dir = Path(__file__).parent
                tab1 = table_1(str(parent_dir / "sql_scripts" / "script1-db2.sql"))
                tab2 = table_2(str(parent_dir / "sql_scripts" / "script2.sql"), tab1)
                assert_result(tab2)

        flow_result = flow.run()
        assert flow_result.successful


if __name__ == "__main__":
    test_sql_node()
