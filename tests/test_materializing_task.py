from __future__ import annotations

import copy

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import ConfigContext, Flow, Stage, StageLockContext, Table
from pydiverse.pipedag.container import Schema
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
@pytest.mark.parametrize("imperative", [False, True])
def test_get_output_from_store(imperative):
    _m = m2 if imperative else m
    with Flow() as f:
        with Stage("stage_1"):
            df1 = _m.pd_dataframe({"x": [0, 1, 2, 3]})
            df2 = _m.pd_dataframe({"y": [0, 1, 2, 3]})
            dataframes = _m.create_tuple(df1, df2)

    # We only use the StageLockContext for testing
    with StageLockContext():
        result = f.run()

        # Call on MaterializingTask
        pd.testing.assert_frame_equal(
            df1.get_output_from_store(as_type=pd.DataFrame),
            result.get(df1, as_type=pd.DataFrame),
        )

        pd.testing.assert_frame_equal(
            df2.get_output_from_store(as_type=pd.DataFrame),
            result.get(df2, as_type=pd.DataFrame),
        )

        pd.testing.assert_frame_equal(
            dataframes.get_output_from_store(as_type=pd.DataFrame)[1],
            result.get(dataframes, as_type=pd.DataFrame)[1],
        )

        # since df1 and df2 are outputs from tasks with the same name (pd_dataframe)
        # ignoring the position hash when retrieving
        # should just return the output from the latest call
        pd.testing.assert_frame_equal(
            df1.get_output_from_store(
                as_type=pd.DataFrame, ignore_position_hashes=True
            ),
            result.get(df2, as_type=pd.DataFrame),
        )

        # Call on MaterializingTaskGetItem
        pd.testing.assert_frame_equal(
            dataframes[0].get_output_from_store(as_type=pd.DataFrame),
            dataframes.get_output_from_store(as_type=pd.DataFrame)[0],
        )

        pd.testing.assert_frame_equal(
            dataframes[0].get_output_from_store(
                as_type=pd.DataFrame, ignore_position_hashes=True
            ),
            dataframes.get_output_from_store(
                as_type=pd.DataFrame, ignore_position_hashes=True
            )[0],
        )


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
    sa_tbl = sa.Table(
        ref.original.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine
    )
    assert [c.name for c in sa_tbl.columns] == ["x"]
    sa_tbl2 = _m.noop_lazy(sa_tbl.alias("alias"))
    assert sa_tbl2.name == "alias"
    assert sa_tbl2.original.name == sa_tbl.name

    # test return nothing materialization
    tbl.name = "a"
    assert (
        tbl.materialize(
            config_context, schema=Schema("dummy_schema"), return_nothing=True
        )
        is None
    )
    sa_tbl = sa.Table(
        tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine
    )
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context and prefixed schema
    tbl.name = "b"
    tbl.materialize(config_context, schema=Schema("schema", prefix="dummy_"))
    sa_tbl = sa.Table(
        tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine
    )
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context and postfixed schema
    tbl.name = "c"
    tbl.materialize(config_context, schema=Schema("dummy", suffix="_schema"))
    sa_tbl = sa.Table(
        tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine
    )
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context incl. prefixed schema
    pipedag_config = create_basic_pipedag_config(
        config_context.store.table_store.engine.url.render_as_string(
            hide_password=False
        )
    )
    raw_config2 = copy.deepcopy(pipedag_config.raw_config)
    raw_config2["instances"]["__any__"]["table_store"]["args"][
        "schema_prefix"
    ] = "dummy_"
    cfg2 = PipedagConfig(raw_config2).get()
    tbl.name = "e"
    with pytest.raises(ValueError, match="Schema prefix and postfix must match"):
        tbl.materialize(cfg2, schema=Schema("schema", prefix="dummyX_"))
    tbl.materialize(cfg2, schema=Schema("schema", prefix="dummy_"))
    sa_tbl = sa.Table(
        tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine
    )
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name

    # test materialization with explicit config_context and postfixed schema
    pipedag_config = create_basic_pipedag_config(
        config_context.store.table_store.engine.url.render_as_string(
            hide_password=False
        )
    )
    raw_config2 = copy.deepcopy(pipedag_config.raw_config)
    raw_config2["instances"]["__any__"]["table_store"]["args"][
        "schema_suffix"
    ] = "_schema"
    cfg2 = PipedagConfig(raw_config2).get()
    tbl.name = "e"
    with pytest.raises(ValueError, match="Schema prefix and postfix must match"):
        tbl.materialize(cfg2, schema=Schema("dummy", suffix="_Xschema"))
    tbl.materialize(cfg2, schema=Schema("dummy", suffix="_schema"))
    sa_tbl = sa.Table(
        tbl.name, sa.MetaData(), schema="dummy_schema", autoload_with=engine
    )
    assert [c.name for c in sa_tbl.columns] == ["x"]
    assert sa_tbl.name == tbl.name
