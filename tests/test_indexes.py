from __future__ import annotations

import pytest

from pydiverse.pipedag import Flow, Stage

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util import tasks_library as m
from tests.util import tasks_library_imperative as m2

pytestmark = [with_instances(DATABASE_INSTANCES)]


@pytest.mark.parametrize(
    "task",
    [
        m.simple_dataframe,
        m.simple_dataframe_with_pk,
        m.simple_dataframe_with_pk2,
        m.simple_dataframe_with_index,
        m.simple_dataframe_with_indexes,
        m.simple_dataframes_with_indexes,
        m.simple_lazy_table,
        m.simple_lazy_table_with_pk,
        m.simple_lazy_table_with_pk2,
        m.simple_lazy_table_with_index,
        m.simple_lazy_table_with_indexes,
        m2.simple_lazy_table,
        m2.simple_lazy_table_with_pk,
    ],
)
def test_materialize_table_with_indexes(task):
    with Flow("flow") as f:
        with Stage("stage"):
            x = task()

            m.assert_table_equal(x, x)
            m.check_pk_length(x)

    assert f.run().successful
