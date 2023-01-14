from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import RunContext, StageLockContext
from pydiverse.pipedag.util.config import PipedagConfig

from tests.util import select_as, tasks_library as m
from tests.fixtures.instance import *


@pytest.mark.parametrize(
    "task",
    [
        m.simple_dataframe,
        m.simple_dataframe_with_pk,
        m.simple_dataframe_with_pk2,
        m.simple_dataframe_with_index,
        m.simple_dataframe_with_indexes,
        m.simple_lazy_table,
        m.simple_lazy_table_with_pk,
        m.simple_lazy_table_with_pk2,
        m.simple_lazy_table_with_index,
        m.simple_lazy_table_with_indexes,
    ],
)
def test_materialize_table_with_indexes(task):
    with Flow("flow") as f:
        with Stage("stage"):
            x = task()

            m.assert_table_equal(x, x)

    assert f.run().successful
