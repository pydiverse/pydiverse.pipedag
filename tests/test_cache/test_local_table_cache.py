from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import *
from tests.fixtures.instances import with_instances


@with_instances(
    "local_table_cache",
    "local_table_cache_inout",
    "local_table_cache_inout_numpy",
    "local_table_store",
)
def test_local_table_cache(mocker):
    input_val_ = 0

    @materialize()
    def input_val():
        return input_val_

    @materialize(version="1.0")
    def select_pandas(x):
        # Supported by local caching
        return Table(pd.DataFrame({"x": [x]}), "pandas")

    @materialize(lazy=True)
    def select_sql(x):
        # Not supported by local caching
        return Table(sa.select(sa.literal(x).label("x")), "sql")

    @materialize(version="1.0", input_type=pd.DataFrame)
    def sink(*args):
        for arg in args:
            assert arg["x"][0] == input_val_

    with Flow() as f:
        with Stage("stage"):
            x = input_val()

            s_pandas = select_pandas(x)
            s_sql = select_sql(x)

            _ = sink(s_pandas, s_sql)

    # Initial run to invalidate cache
    input_val_ = -1
    f.run()
    input_val_ = 0

    # Spy Setup
    config_context = ConfigContext.get()
    local_table_cache = config_context.store.local_table_cache

    si = int(local_table_cache.should_store_input)
    so = int(local_table_cache.should_store_output)
    siac = int(local_table_cache.should_use_stored_input_as_cache)

    store_table_spy = mocker.spy(local_table_cache, "store_table")
    store_input_spy = mocker.spy(local_table_cache, "store_input")
    _store_table_spy = mocker.spy(local_table_cache, "_store_table")

    retrieve_table_obj_spy = mocker.spy(local_table_cache, "retrieve_table_obj")
    _retrieve_table_obj_spy = mocker.spy(local_table_cache, "_retrieve_table_obj")

    # Initial Run
    f.run()

    expected_retrieve_table_obj = 2
    expected_successful_retrieve_table_obj = 1 * so * siac  # pandas
    expected_store_table = 2
    expected_store_input = 2 - expected_successful_retrieve_table_obj

    assert store_table_spy.call_count == expected_store_table
    assert store_input_spy.call_count == expected_store_input
    assert retrieve_table_obj_spy.call_count == expected_retrieve_table_obj

    assert _store_table_spy.call_count == (expected_store_input * si) + (
        expected_store_table * so
    )
    assert _retrieve_table_obj_spy.call_count == expected_successful_retrieve_table_obj

    # Second Run
    store_table_spy.reset_mock()
    store_input_spy.reset_mock()
    _store_table_spy.reset_mock()
    retrieve_table_obj_spy.reset_mock()
    _retrieve_table_obj_spy.reset_mock()

    f.run()

    # Everything should be cache valid, thus no task should get executed.
    assert store_table_spy.call_count == 0
    assert store_input_spy.call_count == 0
    assert retrieve_table_obj_spy.call_count == 0
