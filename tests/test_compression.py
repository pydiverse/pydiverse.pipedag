from __future__ import annotations

import pytest
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, Table, materialize
from pydiverse.pipedag.backend.table.sql.dialects import (
    IBMDB2TableStore,
    MSSqlTableStore,
)

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import (
    DATABASE_INSTANCES,
    skip_instances,
    with_instances,
)
from tests.util import tasks_library as m

pytestmark = [with_instances(DATABASE_INSTANCES)]


@pytest.mark.parametrize(
    "task, stage_materialization_details",
    [
        (m.simple_table_compressed_one_method, "adaptive_value_compression"),
        (m.simple_table_compressed_two_methods, "adaptive_value_compression"),
        (m.simple_dataframe_compressed_one_method, "adaptive_value_compression"),
        (m.simple_dataframe_compressed_two_methods, "adaptive_value_compression"),
        (m.simple_table_default_compressed, "adaptive_value_compression"),
        (m.simple_dataframe_uncompressed, None),
    ],
)
@with_instances(DATABASE_INSTANCES, "ibm_db2_materialization_details")
@skip_instances("ibm_db2")
def test_compression(task, stage_materialization_details):
    @materialize(input_type=sa.Table, lazy=False)
    def get_compression_attributes(table: sa.sql.expression.Alias):
        query = f"""
        SELECT COMPRESSION, ROWCOMPMODE FROM SYSCAT.TABLES
        WHERE TABSCHEMA = '{table.original.schema.upper()}'
         AND TABNAME = '{table.original.name.upper()}'
        """
        return Table(sa.text(query), f"compression_attributes_{table.name}")

    with Flow("flow") as f:
        with Stage("stage", materialization_details=stage_materialization_details):
            comp_exp_x, x = task()
            config = ConfigContext.get()
            store = config.store.table_store
            if isinstance(store, IBMDB2TableStore):
                comp_x = get_compression_attributes(x)
                m.assert_table_equal(comp_exp_x, comp_x)

            m.assert_table_equal(x, x)

    for _ in range(3):
        if (
            not isinstance(store, (MSSqlTableStore, IBMDB2TableStore))
            and task != m.simple_dataframe_uncompressed
        ):
            with pytest.raises(
                ValueError,
                match="To silence this exception set"
                " strict_materialization_details=False",
            ):
                assert f.run().successful
        else:
            assert f.run().successful
