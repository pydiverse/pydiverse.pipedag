# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pandas as pd
import pytest

from pydiverse.pipedag import Flow, Stage, Table, materialize

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util.tasks_library import assert_table_equal

# unfortunately, pydiverse.transform currently does not support ibm_db2
pytestmark = [pytest.mark.pdtransform, with_instances(tuple(set(DATABASE_INSTANCES) - {"ibm_db2"}))]

try:
    import pydiverse.transform as pdt

    _ = pdt

    try:
        from pydiverse.transform.core.verbs import mutate
        from pydiverse.transform.eager import PandasTableImpl
        from pydiverse.transform.lazy import SQLTableImpl

        # ensures a "used" state for the import, preventing black from deleting it
        _ = PandasTableImpl

        test_list = [SQLTableImpl, PandasTableImpl]
    except ImportError:
        try:
            from pydiverse.transform.extended import Pandas, Polars, SqlAlchemy, mutate

            test_list = [SqlAlchemy, Polars, Pandas]
        except ImportError:
            raise NotImplementedError("pydiverse.transform 0.2.0 - 0.2.2 isn't supported") from None
except ImportError:
    test_list = []


@pytest.mark.parametrize(
    "impl_type",
    test_list,
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
