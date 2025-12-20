# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import pandas as pd
import polars as pl
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, StageLockContext, Table, materialize
from tests.fixtures.instances import with_instances


@with_instances(
    "local_table_cache",
    "local_table_cache_inout",
    "local_table_cache_inout_numpy",
    "local_table_store",
)
@pytest.mark.parametrize(
    "write_local_table_cache", [True, False], ids=["write_local_table_cache", "no_write_local_table_cache"]
)
def test_local_table_cache(mocker, write_local_table_cache):
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

    @materialize(lazy=True)
    def select_sql_2(x):
        # Not supported by local caching
        return Table(sa.select(sa.literal(x).label("x")), "sql_2")

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
        with Stage("stage") as s:
            x = input_val()

            s_pandas = select_pandas(x)
            s_polars = select_polars(x)
            s_sql = select_sql(x)
            s_sql_2 = select_sql_2(x)

            _ = sink_pandas(s_pandas, s_polars, s_sql)
            _ = sink_polars(s_pandas, s_polars, s_sql)
            _ = sink_sql(s_pandas, s_polars, s_sql)

    # Initial run to invalidate cache
    input_val_ = -1
    with StageLockContext():
        res = f.run()
        df_pl_s_sql_2 = res.get(s_sql_2, as_type=pl.DataFrame, write_local_table_cache=write_local_table_cache)
        df_pd_s_sql_2 = res.get(s_sql_2, as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache)
        _ = res.get(s_sql_2, as_type=sa.Table, write_local_table_cache=write_local_table_cache)
    assert df_pl_s_sql_2.item() == input_val_
    assert df_pd_s_sql_2.iloc[0]["x"] == input_val_
    input_val_ = 0

    config_context = ConfigContext.get()
    local_table_cache = config_context.store.local_table_cache
    # We need to clear the cache to ensure that it does not contain results from the previous run
    # when write_local_table_cache was True.
    # These outputs are not automatically overwritten, if we have not set write_local_table_cache to False.
    local_table_cache.clear_cache(s)
    # Spy Setup

    si = int(local_table_cache.should_store_input)
    so = int(local_table_cache.should_store_output)
    siac = int(local_table_cache.should_use_stored_input_as_cache)
    wltc = int(write_local_table_cache)

    store_table_spy = mocker.spy(local_table_cache, "store_table")
    store_input_spy = mocker.spy(local_table_cache, "store_input")
    _store_table_spy = mocker.spy(local_table_cache, "_store_table")

    retrieve_table_obj_spy = mocker.spy(local_table_cache, "retrieve_table_obj")
    _retrieve_table_obj_spy = mocker.spy(local_table_cache, "_retrieve_table_obj")

    # Initial Run
    with StageLockContext():
        res = f.run()
        df_pl_s_sql_2 = res.get(s_sql_2, as_type=pl.DataFrame, write_local_table_cache=write_local_table_cache)
        df_pd_s_sql_2 = res.get(s_sql_2, as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache)
        _ = res.get(s_sql_2, as_type=sa.Table, write_local_table_cache=write_local_table_cache)
    assert df_pl_s_sql_2.item() == input_val_
    assert df_pd_s_sql_2.iloc[0]["x"] == input_val_

    expected_retrieve_table_obj = 3 * 3 + 3
    expected_successful_retrieve_table_obj = 2 * so * siac + 3 * siac + 1 * wltc * siac
    expected_store_table_no_sql_2 = 3
    expected_store_table_sql_2 = 1
    table_exists_but_store_incompatible = 3 + 1 * wltc  # 3 from sink_sql, 1 from retrieving s_sql_2 as sa.Table
    expected_store_table = expected_store_table_no_sql_2 + expected_store_table_sql_2
    expected_store_input = (
        expected_store_table_no_sql_2 * 3 + expected_store_table_sql_2 * 3 - expected_successful_retrieve_table_obj
    )

    assert store_table_spy.call_count == expected_store_table
    assert store_input_spy.call_count == expected_store_input
    assert retrieve_table_obj_spy.call_count == expected_retrieve_table_obj

    assert _store_table_spy.call_count == (expected_store_input * si) + (expected_store_table * so)
    assert (
        _retrieve_table_obj_spy.call_count
        == expected_successful_retrieve_table_obj + siac * table_exists_but_store_incompatible
    )

    # Second Run
    store_table_spy.reset_mock()
    store_input_spy.reset_mock()
    _store_table_spy.reset_mock()
    retrieve_table_obj_spy.reset_mock()
    _retrieve_table_obj_spy.reset_mock()

    with StageLockContext():
        res = f.run()
        df_pl_s_sql_2 = res.get(s_sql_2, as_type=pl.DataFrame, write_local_table_cache=write_local_table_cache)
        df_pd_s_sql_2 = res.get(s_sql_2, as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache)
        _ = res.get(s_sql_2, as_type=sa.Table, write_local_table_cache=write_local_table_cache)
    assert df_pl_s_sql_2.item() == input_val_
    assert df_pd_s_sql_2.iloc[0]["x"] == input_val_

    # Everything should be cache valid, thus no task should get executed.
    # However, we still load the table from the cache.
    expected_retrieve_table_obj = 3
    expected_successful_retrieve_table_obj = 2 * wltc * siac
    expected_store_input = expected_retrieve_table_obj - expected_successful_retrieve_table_obj
    assert store_table_spy.call_count == 0
    assert store_input_spy.call_count == expected_store_input
    assert retrieve_table_obj_spy.call_count == expected_retrieve_table_obj
