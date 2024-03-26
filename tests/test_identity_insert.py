from __future__ import annotations

from pydiverse.pipedag import Flow, Stage

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import (
    DATABASE_INSTANCES,
    skip_instances,
    with_instances,
)
from tests.util import tasks_library as m

pytestmark = [with_instances(DATABASE_INSTANCES)]


@with_instances(DATABASE_INSTANCES, "mssql_materialization_details")
@skip_instances("ibm_db2", "postgres", "duckdb")
def test_identity_insert():
    with Flow("flow") as f:
        with Stage("stage"):
            _ = m.simple_dataframe()
            _ = m.simple_identity_insert()

    assert f.run().successful
