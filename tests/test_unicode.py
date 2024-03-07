from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, materialize
from pydiverse.pipedag.context import StageLockContext

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import (
    ALL_INSTANCES,
    ORCHESTRATION_INSTANCES,
    skip_instances,
    with_instances,
)
from tests.util import tasks_library as m
from tests.util.tasks_library import simple_dataframe

pytestmark = [with_instances(ALL_INSTANCES, ORCHESTRATION_INSTANCES)]


def test_unicode(unicode_str="äöüßéç"):
    @materialize(lazy=True, input_type=sa.Table)
    def unicode(src):
        return sa.select(sa.literal(unicode_str).label("a")).select_from(src).limit(1)

    with Flow("flow") as f:
        with Stage("stage"):
            dummy_source = simple_dataframe()
            x = unicode(dummy_source)
            x2 = m.noop(x)
            x3 = m.noop_lazy(x2)
            m.assert_table_equal(x, x2)
            m.assert_table_equal(x, x3)

    with StageLockContext():
        result = f.run()
        assert result.successful
        assert result.get(x3, as_type=pd.DataFrame)["a"][0] == unicode_str


@skip_instances("mssql", "mssql_pytsql")
def test_unicode_beyond_mssql():
    test_unicode("λ")
