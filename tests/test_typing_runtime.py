# Copyright (c) QuantCo and pydiverse contributors 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Run-time behaviour that the static type annotations rely on (see tests/type_checking)."""

import pandas as pd

from pydiverse.pipedag import Flow, MaterializingTaskGetItem, RawSql, Result, Stage, Table, materialize
from pydiverse.pipedag.materialize.materializing_task import MaterializingTask2, MaterializingTask3


@materialize(nout=2)
def two():
    return Table(pd.DataFrame()), Table(pd.DataFrame())


def test_unpacked_items_are_materializing_task_get_items():
    with Flow(), Stage("stage"):
        a, b = two()
        whole = two()

    assert isinstance(a, MaterializingTaskGetItem)
    assert isinstance(b, MaterializingTaskGetItem)
    assert all(isinstance(x, MaterializingTaskGetItem) for x in whole)


def test_nout_task_types_are_subscriptable_at_run_time():
    # Annotations using them get evaluated by `typing.get_type_hints`
    assert MaterializingTask2[Table, Table] is not None
    assert MaterializingTask3[Table, Table, Table] is not None


def test_raw_sql_get():
    raw_sql = RawSql("SELECT 1")
    raw_sql.table_names = ["tbl"]
    raw_sql.loaded_tables = {"tbl": "loaded"}

    assert raw_sql.get("tbl") == "loaded"
    assert raw_sql.get("missing", "default") == "default"


def test_result_evolve():
    result = Result(
        flow=None,  # type: ignore
        subflow=None,  # type: ignore
        underlying=None,
        successful=True,
        config_context=None,
        task_values={},
        task_states={},
        exception=None,
    )

    evolved = result.evolve(successful=False)
    assert evolved.successful is False
    assert result.successful is True
