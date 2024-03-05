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

pytestmark = [with_instances(ALL_INSTANCES, ORCHESTRATION_INSTANCES)]


def test_unicode():
    @materialize(lazy=True)
    def unicode():
        return sa.text("SELECT 'äöüßéç' as a")

    with Flow("flow") as f:
        with Stage("stage"):
            x = unicode()
            x2 = m.noop(x)
            x3 = m.noop_lazy(x2)
            m.assert_table_equal(x, x2)
            m.assert_table_equal(x, x3)

    with StageLockContext():
        result = f.run()
        assert result.successful
        assert result.get(x3, as_type=pd.DataFrame)["a"][0] == "äöüßéç"


@skip_instances("mssql", "mssql_pytsql")
def test_unicode_beyond_mssql():
    @materialize(lazy=True)
    def unicode():
        return sa.text("SELECT 'λ' as a")

    with Flow("flow") as f:
        with Stage("stage"):
            x = unicode()
            x2 = m.noop(x)
            x3 = m.noop_lazy(x2)
            m.assert_table_equal(x, x2)
            m.assert_table_equal(x, x3)

    with StageLockContext():
        result = f.run()
        assert result.successful
        assert result.get(x3, as_type=pd.DataFrame)["a"][0] == "λ"
