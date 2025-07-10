# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pandas as pd
import polars as pl
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, Table, materialize
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

    @materialize(version="1.0")
    def select_polars(x):
        # Supported by local caching
        return Table(pl.DataFrame({"x": [x]}), "polars")

    @materialize(lazy=True)
    def select_sql(x):
        # Not supported by local caching
        return Table(sa.select(sa.literal(x).label("x")), "sql")

    @materialize(version="1.0", input_type=pd.DataFrame)
    def sink_pandas(*args):
        for arg in args:
            assert arg["x"][0] == input_val_

    @materialize(version="1.0", input_type=pl.DataFrame)
    def sink_polars(*args):
        for arg in args:
            assert arg["x"][0] == input_val_

    @materialize(version="1.0", input_type=sa.Table)
    def sink_sql(*args):
        for arg in args:
            # check that column x exists
            _ = arg.c.x

    with Flow() as f:
        with Stage("stage"):
            x = input_val()

            s_pandas = select_pandas(x)
            s_polars = select_polars(x)
            s_sql = select_sql(x)

            _ = sink_pandas(s_pandas, s_polars, s_sql)
            _ = sink_polars(s_pandas, s_polars, s_sql)
            _ = sink_sql(s_pandas, s_polars, s_sql)

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

    expected_retrieve_table_obj = 3 * 3
    expected_successful_retrieve_table_obj = 2 * so * siac + 3 * siac
    expected_store_table = 3
    expected_store_input = expected_store_table * 3 - expected_successful_retrieve_table_obj

    assert store_table_spy.call_count == expected_store_table
    assert store_input_spy.call_count == expected_store_input
    assert retrieve_table_obj_spy.call_count == expected_retrieve_table_obj

    assert _store_table_spy.call_count == (expected_store_input * si) + (expected_store_table * so)
    assert _retrieve_table_obj_spy.call_count == expected_successful_retrieve_table_obj + siac * expected_store_table

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
