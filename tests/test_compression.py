from __future__ import annotations

import pytest
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, Table, materialize
from pydiverse.pipedag.backend.table.sql.dialects import IBMDB2TableStore

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util import tasks_library as m

pytestmark = [with_instances(DATABASE_INSTANCES)]


@pytest.mark.parametrize(
    "task",
    [
        m.simple_table_compressed_one_method,
        m.simple_table_compressed_two_methods,
        m.simple_dataframe_compressed_one_method,
        m.simple_dataframe_compressed_two_methods,
    ],
)
def test_materialize_table_with_indexes(task):
    @materialize(input_type=sa.Table, lazy=False)
    def get_compression_attributes(table: sa.Table):
        query = f"""
        SELECT COMPRESSION, ROWCOMPMODE FROM SYSCAT.TABLES
        WHERE TABSCHEMA = '{table.original.schema.upper()}'
         AND TABNAME = '{table.original.name.upper()}'
        """
        return Table(sa.text(query), f"compression_attributes_{table.name}")

    with Flow("flow") as f:
        with Stage("stage"):
            comp_exp_x, x = task()
            config = ConfigContext.get()
            store = config.store.table_store
            if isinstance(store, IBMDB2TableStore):
                comp_x = get_compression_attributes(x)
                m.assert_table_equal(comp_exp_x, comp_x)

            m.assert_table_equal(x, x)

    assert f.run().successful
