from __future__ import annotations

import pandas as pd
import pytest

from pydiverse.pipedag import *

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util.tasks_library import assert_table_equal

pytestmark = [pytest.mark.pdtransform, with_instances(DATABASE_INSTANCES)]


try:
    from pydiverse.transform.core.verbs import mutate
    from pydiverse.transform.eager import PandasTableImpl
    from pydiverse.transform.lazy import SQLTableImpl
except ImportError:
    SQLTableImpl = None
    PandasTableImpl = None


@pytest.mark.parametrize(
    "impl_type",
    [SQLTableImpl, PandasTableImpl],
)
def test_table_store(impl_type: type):
    def cache_fn(*args, **kwargs):
        return impl_type.__name__

    @materialize()
    def in_table():
        return Table(
            pd.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                }
            )
        )

    @materialize()
    def expected_out_table():
        return Table(
            pd.DataFrame(
                {
                    "col": [0, 1, 2, 3],
                    "x": [1, 1, 1, 1],
                    "y": [2, 2, 2, 2],
                }
            )
        )

    @materialize(input_type=impl_type, cache=cache_fn)
    def noop(x):
        return Table(x)

    @materialize(lazy=True, input_type=impl_type, cache=cache_fn)
    def noop_lazy(x):
        return Table(x)

    @materialize(input_type=impl_type, cache=cache_fn)
    def add_column(x):
        return Table(x >> mutate(x=1))

    @materialize(lazy=True, input_type=impl_type, cache=cache_fn)
    def add_column_lazy(x):
        return Table(x >> mutate(y=2))

    with Flow() as f:
        with Stage("pdtransform"):
            table = in_table()
            table = noop(table)
            table = noop_lazy(table)
            table = add_column(table)
            table = add_column_lazy(table)

            expected = expected_out_table()
            _ = assert_table_equal(table, expected, check_dtype=False)

    assert f.run().successful
