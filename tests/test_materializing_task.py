# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import copy

import pandas as pd
import pytest
import sqlalchemy as sa
import structlog
from pandas.testing import assert_frame_equal

from pydiverse.pipedag import ConfigContext, Flow, Stage, StageLockContext, Table
from pydiverse.pipedag.container import Schema
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.core.config import PipedagConfig, create_basic_pipedag_config
from tests.fixtures.instances import with_instances
from tests.util import tasks_library as m
from tests.util import tasks_library_imperative as m2


@with_instances(
    "postgres",
    "local_table_cache",
    "local_table_cache_inout",
    "local_table_cache_inout_numpy",
)
@pytest.mark.lock_tests  # this test seems to hang easily in ColSpec tests
@pytest.mark.parametrize("imperative", [False, True], ids=["imperative", "not_imperative"])
@pytest.mark.parametrize(
    "write_local_table_cache", [True, False], ids=["write_local_table_cache", "no_write_local_table_cache"]
)
def test_get_output_from_store(mocker, imperative, write_local_table_cache):
    _m = m2 if imperative else m
    with Flow() as f:
        with Stage("stage_1") as s:
            df1 = _m.pd_dataframe({"x": [0, 1, 2, 3]})
            df2 = _m.pd_dataframe({"y": [0, 1, 2, 3]})
            dataframes = _m.create_tuple(df1, df2)

    # We only use the StageLockContext for testing
    with StageLockContext():
        # First run to ensure cache-validity -> No local cache writes during the second flow run
        f.run()
        local_table_cache = ConfigContext.get().store.local_table_cache
        # A local table cache only exists when configured
        if local_table_cache:
            local_table_cache.clear_cache(s)
            store_input_spy = mocker.spy(local_table_cache, "store_input")
            _store_table_spy = mocker.spy(local_table_cache, "_store_table")
            retrieve_table_obj_spy = mocker.spy(local_table_cache, "retrieve_table_obj")
            _retrieve_table_obj_spy = mocker.spy(local_table_cache, "_retrieve_table_obj")
        result = f.run()

        # Call on MaterializingTask
        pd.testing.assert_frame_equal(
            df1.get_output_from_store(as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache),
            result.get(df1, as_type=pd.DataFrame),
        )

        pd.testing.assert_frame_equal(
            df2.get_output_from_store(as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache),
            result.get(df2, as_type=pd.DataFrame),
        )

        pd.testing.assert_frame_equal(
            dataframes.get_output_from_store(as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache)[1],
            result.get(dataframes, as_type=pd.DataFrame)[1],
        )

        # since df1 and df2 are outputs from tasks with the same name (pd_dataframe)
        # ignoring the position hash when retrieving
        # should just return the output from the latest call
        pd.testing.assert_frame_equal(
            df1.get_output_from_store(
                as_type=pd.DataFrame, ignore_position_hashes=True, write_local_table_cache=write_local_table_cache
            ),
            result.get(df2, as_type=pd.DataFrame),
        )

        # Call on MaterializingTaskGetItem
        pd.testing.assert_frame_equal(
            dataframes[0].get_output_from_store(as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache),
            dataframes.get_output_from_store(as_type=pd.DataFrame, write_local_table_cache=write_local_table_cache)[0],
        )

        pd.testing.assert_frame_equal(
            dataframes[0].get_output_from_store(
                as_type=pd.DataFrame, ignore_position_hashes=True, write_local_table_cache=write_local_table_cache
            ),
            dataframes.get_output_from_store(
                as_type=pd.DataFrame, ignore_position_hashes=True, write_local_table_cache=write_local_table_cache
            )[0],
        )

        if local_table_cache:
            # We try to get a table 16 times after running the flow.
            expected_retrieve_table_obj = 16
            # If write_local_table_cache we attempt to store each of df1, df2, dataframes[0], dataframes[1]
            # once because it was not yet in the cache. Otherwise, storing is never successful, so we attempt it for
            # each attempted retrieval.
            expected_store_input = 4 if write_local_table_cache else expected_retrieve_table_obj
            expected_successful_retrieve_table_obj = expected_retrieve_table_obj - expected_store_input

            assert store_input_spy.call_count == expected_store_input
            assert _store_table_spy.call_count == expected_store_input
            assert retrieve_table_obj_spy.call_count == expected_retrieve_table_obj
            assert _retrieve_table_obj_spy.call_count == expected_successful_retrieve_table_obj


@with_instances("postgres")
@pytest.mark.parametrize("imperative", [False, True])
def test_call_task_outside_flow(imperative):
    _m = m2 if imperative else m
    # Literal Values
    assert m.one() == 1
    assert m.create_tuple(m.one(), m.two()) == (1, 2)
    assert m.noop("foo") == "foo"

    # Table
    df1 = _m.pd_dataframe({"x": [0, 1, 2, 3]})
    df2 = m.noop(df1)
    expected = pd.DataFrame({"x": [0, 1, 2, 3]})

    for df in [df1, df2]:
        pd.testing.assert_frame_equal(df, expected)
        _m.assert_table_equal(df, expected)

    # Lazy Table
    expr = _m.simple_lazy_table()
    expected_expr = (
        "SELECT 0 AS col1, '0' AS col2 "
        "UNION ALL SELECT 1 AS col1, '1' AS col2 "
        "UNION ALL SELECT 2 AS col1, '2' AS col2 "
        "UNION ALL SELECT 3 AS col1, '3' AS col2"
    )
    config_context = ConfigContext.get()
    assert (
        str(
            expr.compile(
                config_context.store.table_store.engine,
                compile_kwargs={"literal_binds": True},
            )
        )
        == expected_expr
    )

    tbl = m.noop(sa.Table("foo", sa.MetaData()).alias("bar"))
    assert tbl.name == "bar"
    assert tbl.original.name == "foo"

    # Blob
    expected_blob = {"a": {"b": 1, "c": 2}, "d": [1, (2, 3)]}
    blob = m.as_blob(expected_blob)

    assert blob is expected_blob
    m.assert_blob_equal(blob, expected_blob)

    # RawSql
    expected_query = "SELECT 1 as x"
    raw_sql = m.as_raw_sql(expected_query)

    assert raw_sql == expected_query


@with_instances("postgres")
def test_imperative_materialize_given_config():
    _m = m2
    df = pd.DataFrame({"x": [0, 1, 2, 3]})
    tbl = Table(df)
    assert tbl.materialize() is df  # should be no-op
    assert tbl.materialize(return_nothing=True) is None

    config_context = ConfigContext.get()
    engine = config_context.store.table_store.engine
    with engine.connect() as conn:
        conn.execute(sa.text("DROP SCHEMA IF EXISTS dummy_schema CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS dummy_schema"))
        if sa.__version__ >= "2.0.0":
            conn.commit()

    # test failure on materialization without schema
    with pytest.raises(ValueError, match="schema must be provided"):
        tbl.materialize(config_context)
    # test materialization with explicit config_context and direct schema
    ref = tbl.materialize(config_context, schema=Schema("dummy_schema"))
    sa_tbl = sa.Table(ref.original.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine)
    assert [c.name for c in sa_tbl.columns] == ["x"]
    sa_tbl2 = _m.noop_lazy(sa_tbl.alias("alias"))
    assert sa_tbl2.name == "alias"
    assert sa_tbl2.original.name == sa_tbl.name

    # test return nothing materialization
    tbl.name = "a"
    assert tbl.materialize(config_context, schema=Schema("dummy_schema"), return_nothing=True) is None
    sa_tbl = sa.Table(tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine)
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context and prefixed schema
    tbl.name = "b"
    tbl.materialize(config_context, schema=Schema("schema", prefix="dummy_"))
    sa_tbl = sa.Table(tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine)
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context and postfixed schema
    tbl.name = "c"
    tbl.materialize(config_context, schema=Schema("dummy", suffix="_schema"))
    sa_tbl = sa.Table(tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine)
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context incl. prefixed schema
    pipedag_config = create_basic_pipedag_config(
        config_context.store.table_store.engine.url.render_as_string(hide_password=False)
    )
    raw_config2 = copy.deepcopy(pipedag_config.raw_config)
    raw_config2["instances"]["__any__"]["table_store"]["args"]["schema_prefix"] = "dummy_"
    cfg2 = PipedagConfig(raw_config2).get()
    tbl.name = "e"
    with pytest.raises(ValueError, match="Schema prefix and postfix must match"):
        tbl.materialize(cfg2, schema=Schema("schema", prefix="dummyX_"))
    tbl.materialize(cfg2, schema=Schema("schema", prefix="dummy_"))
    sa_tbl = sa.Table(tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine)
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context and postfixed schema
    pipedag_config = create_basic_pipedag_config(
        config_context.store.table_store.engine.url.render_as_string(hide_password=False)
    )
    raw_config2 = copy.deepcopy(pipedag_config.raw_config)
    raw_config2["instances"]["__any__"]["table_store"]["args"]["schema_suffix"] = "_schema"
    cfg2 = PipedagConfig(raw_config2).get()
    tbl.name = "e"
    with pytest.raises(ValueError, match="Schema prefix and postfix must match"):
        tbl.materialize(cfg2, schema=Schema("dummy", suffix="_Xschema"))
    tbl.materialize(cfg2, schema=Schema("dummy", suffix="_schema"))
    sa_tbl = sa.Table(tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine)
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name


@with_instances("parquet_s3_backend")
def test_metadata_store_synchronization():
    logger = structlog.get_logger(__name__ + ".test_metadata_store_synchronization")
    x = {"x": [0, 1, 2, 3]}
    y = {"y": [0, 1, 2, 3]}
    constant = dict(c=[0])
    with Flow() as f:
        with Stage("stage_1") as s:
            df1 = m.pd_dataframe(x)
            df2 = m.pd_dataframe(y)
            dataframes = m.create_tuple(df1, df2)
            out = m.noop(constant)  # prevent 100% cache valid stage
            out2 = m.pd_dataframe(out)
            _ = dataframes, out2

    cfg1 = ConfigContext.get()
    # setup second configuration with different local duckdb file
    cfg2 = cfg1.evolve(
        _config_dict=dict(
            table_store=dict(args=dict(url="duckdb:////tmp/pipedag/parquet_duckdb/parquet_s3_backend2.duckdb"))
        ),
        _transfer_cache_=False,
    )

    is_odd = {}
    for mode in [CacheValidationMode.FORCE_CACHE_INVALID, CacheValidationMode.NORMAL]:
        for cfg in (cfg1, cfg2):
            store = cfg.store.table_store  # keep this before f.run() to allow transfer of cached_property store
            logger.info("** Running Pipeline for **", mode=mode, db_file=store.engine.url)
            with StageLockContext():
                f.run(config=cfg, cache_validation_mode=mode)
                out_x, out_y = dataframes.get_output_from_store(as_type=pd.DataFrame)
                assert_frame_equal(out_x, pd.DataFrame(x))
                assert_frame_equal(out_y, pd.DataFrame(y))
                query = sa.text(f"""
                    FROM duckdb_views() SELECT sql WHERE
                        schema_name='{store.get_schema(s.name).get()}'
                """)
                with store.engine.connect() as conn:
                    views = store.execute(query, conn=conn).fetchall()
                cnt_odd = sum(1 if "FROM stage_1__odd." in v[0] else 0 for v in views)
                cnt_even = sum(1 if "FROM stage_1__even." in v[0] else 0 for v in views)
                assert len(views) == 5
                assert cnt_odd + cnt_even == 5
                assert min(cnt_odd, cnt_even) == 0
                is_odd[id(cfg)] = cnt_odd > 0

            if mode == CacheValidationMode.NORMAL:
                constant["c"][0] += 1  # prevent 100% cache valid stage

        assert is_odd[id(cfg1)] != is_odd[id(cfg2)]
