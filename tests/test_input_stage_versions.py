from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import (
    ConfigContext,
    Flow,
    GroupNode,
    PipedagConfig,
    Stage,
    Table,
    input_stage_versions,
    materialize,
)
from pydiverse.pipedag.context import StageLockContext

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import (
    ALL_INSTANCES,
    ORCHESTRATION_INSTANCES,
    with_instances,
)
from tests.util import swallowing_raises
from tests.util import tasks_library as m

pytestmark = [with_instances(ALL_INSTANCES, ORCHESTRATION_INSTANCES)]


def test_input_versions_literals():
    """
    Check that all json serializable literals get stored and retrieved
    correctly from the table / metadata store.
    """

    @input_stage_versions(pass_args=["value"])
    def noop(transaction_table_dict, other_table_dict, value):
        _ = transaction_table_dict, other_table_dict
        return value

    def materialize_and_retrieve(value):
        v = noop(value=value)
        return m.assert_equal(value, v)

    with Flow("flow") as f:
        with Stage("stage"):
            materialize_and_retrieve(None)

            materialize_and_retrieve(0)
            materialize_and_retrieve(1)

            # max 32bit
            materialize_and_retrieve(-2147483648)
            materialize_and_retrieve(2147483647)

            # max 64bit
            materialize_and_retrieve(-9223372036854775808)
            materialize_and_retrieve(9223372036854775807)

            # in python json we even have infinite precision integers:
            materialize_and_retrieve(-9223372036854775809)
            materialize_and_retrieve(9223372036854775808)

            materialize_and_retrieve(3.14)
            materialize_and_retrieve(-2.71)

            materialize_and_retrieve("")
            materialize_and_retrieve("a string")
            materialize_and_retrieve("a" * 256)

            materialize_and_retrieve([0, 1, 2, 3])

            materialize_and_retrieve({"0": 0, "1": 1})

            materialize_and_retrieve(
                {
                    "key": [0, 1, 2, 3],
                    "numbers": {
                        "zero": 0,
                        "one": 1,
                    },
                    "nested_lists": [[[None]]],
                }
            )

    assert f.run().successful


def test_input_versions_table():
    run = 1
    val = 12

    @input_stage_versions(input_type=pd.DataFrame, ordering_barrier=False)
    def validate_stage(
        tbls: dict[str, pd.DataFrame], other_tbls: dict[str, pd.DataFrame]
    ):
        if run > 1:
            assert len(tbls) == 1
            assert len(other_tbls) == 1
            x = tbls["x"]
            y = other_tbls["x"]
            pd.testing.assert_frame_equal(x, y)

    @materialize
    def pd_dataframe(data: dict[str, list]):
        return Table(pd.DataFrame(data), name="x")

    def get_flow():
        with Flow("flow") as f:
            with Stage("stage"):
                x = pd_dataframe({"a": [val], "b": [24]})
                validate_stage(x)
                m.assert_table_equal(x, x)
                with GroupNode(ordering_barrier=True):
                    validate_stage()
        return f

    with StageLockContext():
        f = get_flow()
        assert f.run().successful
        run += 1
        f = get_flow()
        assert f.run().successful
        run += 1
        val = -5
        f = get_flow()
        with swallowing_raises(
            AssertionError,
            match=r"\[left\]:[ \t]*\[-5(\.0)?\]\n[ \t]*\[right\]:[ \t]*\[12(\.0)?\]",
        ):
            f.run()


@with_instances("postgres")
def test_input_versions_blob():
    run = 1
    val = 12

    @input_stage_versions(input_type=pd.DataFrame)
    def validate_stage(
        tbls: dict[str, sa.Alias],
        other_tbls: dict[str, sa.Alias],
        blobs: dict[str, dict],
        other_blobs: dict[str, dict],
    ):
        if run > 1:
            assert len(tbls) == 0
            assert len(other_tbls) == 0
            assert len(blobs) == 3
            assert len(other_blobs) == 3
            # this exception fails on different input because the hashes change and thus
            # the file name changes
            assert not any(
                isinstance(x, Exception) for x in other_blobs.values()
            ), "expected to fail on third call"
            assert blobs == other_blobs

    def get_flow():
        with Flow("flow") as f:
            with Stage("stage_0"):
                x = m.object_blob({"a": val, "b": 24})

            with Stage("stage_1"):
                y = m.as_blob(x)
                m.assert_blob_equal(x, y)
                one = m.one()
                one_blob = m.as_blob(m.one())
                m.assert_equal(one, one_blob)
                validate_stage([x, y, one_blob])
        return f

    with StageLockContext():
        f = get_flow()
        assert f.run().successful
        run += 1
        f = get_flow()
        assert f.run().successful
        run += 1
        val = -5
        f = get_flow()
        with swallowing_raises(AssertionError, match=r"expected to fail on third call"):
            f.run()


@with_instances("mssql")  # mssql can do cross-database queries
@pytest.mark.parametrize("per_user", [True, False])
def test_input_versions_other_instance_table(per_user):
    run = 1
    val = 12

    @input_stage_versions(input_type=sa.Table, lazy=True, lock_source_stages=False)
    def join_across_stage_versions(
        tbls: dict[str, sa.Alias],
        other_tbls: dict[str, sa.Alias],
        other_cfg: ConfigContext,
    ):
        if run > 1:
            # make a cross-database query to combine tables of both instances
            other_database = other_cfg.store.table_store.engine.url.database
            assert (
                ConfigContext.get().store.table_store.engine.url.database
                != other_database
            )
            other_tbls[
                "x"
            ].original.schema = f'{other_database}.{other_tbls["x"].original.schema}'
            return Table(
                sa.select(
                    tbls["x"].outerjoin(
                        other_tbls["x"], tbls["x"].c.a == other_tbls["x"].c.a
                    )
                ),
                name="res",
            )
        else:
            return Table(tbls["x"], name="res")

    @input_stage_versions(
        input_type=pd.DataFrame, ordering_barrier=False, lock_source_stages=False
    )
    def validate_stage(
        tbls: dict[str, pd.DataFrame],
        other_tbls: dict[str, pd.DataFrame],
        other_cfg: ConfigContext,
    ):
        _ = other_cfg
        if run > 1:
            assert 1 <= len({k for k in tbls.keys() if not k.endswith("__copy")}) <= 2
            assert (
                1
                <= len({k for k in other_tbls.keys() if not k.endswith("__copy")})
                <= 3
            )
            x = tbls["x"]
            y = other_tbls["x"]
            pd.testing.assert_frame_equal(x, y)

    @materialize(version="1.1")
    def pd_dataframe(data: dict[str, list]):
        return Table(pd.DataFrame(data), name="x")

    def get_flow(other_cfg: ConfigContext):
        with Flow("flow") as f:
            with Stage("stage"):
                x = pd_dataframe({"a": [val], "b": [24]})
                res = join_across_stage_versions(other_cfg)
                validate_stage(x, other_cfg)
                m.assert_table_equal(x, x)
                with GroupNode(ordering_barrier=True):
                    validate_stage(config=other_cfg)
                _ = m.noop(res)
        return f

    cfg = ConfigContext.get()
    cfg2 = PipedagConfig.default.get("mssql_pytsql", per_user=per_user)

    with StageLockContext():
        f = get_flow(cfg2)
        assert cfg.store.table_store.engine.url != cfg2.store.table_store.engine.url
        assert f.run(config=cfg).successful
        assert (
            pd.read_sql_table(
                "x",
                schema=cfg.store.table_store.get_schema(f["stage"].name).get(),
                con=cfg.store.table_store.engine,
            ).iloc[0, 0]
            == val
        )
        run += 1
        f = get_flow(cfg)
        assert f.run(config=cfg2).successful
        assert (
            pd.read_sql_table(
                "x",
                schema=cfg2.store.table_store.get_schema(f["stage"].name).get(),
                con=cfg2.store.table_store.engine,
            ).iloc[0, 0]
            == val
        )
        run += 1
        val = -5
        f = get_flow(cfg)
        with swallowing_raises(
            AssertionError, match=r"\[left\]:  \[-5\]\n\[right\]: \[12\]"
        ):
            f.run(config=cfg2)
        assert (
            pd.read_sql_table(
                "x",
                schema=cfg2.store.table_store.get_schema(
                    f["stage"].transaction_name
                ).get(),
                con=cfg2.store.table_store.engine,
            ).iloc[0, 0]
            == val
        )


@with_instances("postgres")
@pytest.mark.parametrize("per_user", [True, False])
def test_input_versions_other_instance_locking(per_user):
    run = 1
    val = 12

    @input_stage_versions(input_type=pd.DataFrame, lazy=True)
    def join_across_stage_versions(
        tbls: dict[str, pd.DataFrame],
        other_tbls: dict[str, pd.DataFrame],
        other_cfg: ConfigContext,
    ):
        _ = other_cfg
        # other_tbls["x"] might be destoryed by a race condition since we can't keep
        # locks between both runs because we lock the other instance again
        if run > 1 and isinstance(other_tbls["x"], pd.DataFrame):
            return Table(tbls["x"].merge(other_tbls["x"], on="a"), name="res")
        else:
            return Table(tbls["x"], name="res")

    @input_stage_versions(input_type=pd.DataFrame, ordering_barrier=False)
    def validate_stage(
        tbls: dict[str, pd.DataFrame],
        other_tbls: dict[str, pd.DataFrame],
        other_cfg: ConfigContext,
    ):
        _ = other_cfg
        # we cannot make assumptions since we did not lock other instance between the
        # two runs
        _ = other_tbls
        if run > 1:
            assert 1 <= len({k for k in tbls.keys() if not k.endswith("__copy")}) <= 2
            x = tbls["x"]
            assert x.iloc[0, 0] == val

    @materialize
    def pd_dataframe(data: dict[str, list]):
        return Table(pd.DataFrame(data), name="x")

    def get_flow(other_cfg: ConfigContext):
        with Flow("flow") as f:
            with Stage("stage"):
                x = pd_dataframe({"a": [val], "b": [24]})
                res = join_across_stage_versions(other_cfg)
                validate_stage(x, other_cfg)
                m.assert_table_equal(x, x)
                with GroupNode(ordering_barrier=True):
                    validate_stage(config=other_cfg)
                _ = m.noop(res)
        return f

    cfg = ConfigContext.get()
    cfg2 = PipedagConfig.default.get("postgres_unlogged", per_user=per_user)

    with StageLockContext():
        f = get_flow(cfg2)
        assert cfg.store.table_store.engine.url != cfg2.store.table_store.engine.url
        assert f.run(config=cfg).successful
        assert (
            pd.read_sql_table(
                "x",
                schema=cfg.store.table_store.get_schema(f["stage"].name).get(),
                con=cfg.store.table_store.engine,
            ).iloc[0, 0]
            == val
        )

    run += 1

    with StageLockContext():
        f = get_flow(cfg)
        assert f.run(config=cfg2).successful
        assert (
            pd.read_sql_table(
                "x",
                schema=cfg2.store.table_store.get_schema(f["stage"].name).get(),
                con=cfg2.store.table_store.engine,
            ).iloc[0, 0]
            == val
        )

    run += 1
    val = -5

    with StageLockContext():
        f = get_flow(cfg)
        assert f.run(config=cfg2).successful
        assert (
            pd.read_sql_table(
                "x",
                schema=cfg2.store.table_store.get_schema(f["stage"].name).get(),
                con=cfg2.store.table_store.engine,
            ).iloc[0, 0]
            == val
        )
