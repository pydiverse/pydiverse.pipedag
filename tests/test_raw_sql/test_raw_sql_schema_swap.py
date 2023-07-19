from __future__ import annotations

from pathlib import Path

from pydiverse.pipedag import Flow, Stage
from pydiverse.pipedag.context import ConfigContext, FinalTaskState
from tests.fixtures.instances import with_instances
from tests.test_raw_sql.util import sql_script


# TODO: Extend tests for other backends
@with_instances("mssql")
def test_raw_sql_schema_swap():
    # This test creates various different objects in one schema and then
    # checks if, after swapping the schema, if they are still working correctly.

    instance_name = ConfigContext.get().instance_name
    dir_ = Path(__file__).parent / "scripts" / instance_name / "schema_swap"

    with Flow() as f:
        with Stage("raw_0") as raw_0:
            sql_1 = sql_script("create_objects.sql", dir_)
        with Stage("raw_1"):
            sql_2 = sql_script(
                "check_objects.sql", dir_, input_stage=raw_0, depend=[sql_1]
            )

    f.run()

    # Check that running the flow again results in the cache being used
    for _ in range(2):
        result = f.run()
        assert result.task_states[sql_1] == FinalTaskState.CACHE_VALID
        assert result.task_states[sql_2] == FinalTaskState.CACHE_VALID
