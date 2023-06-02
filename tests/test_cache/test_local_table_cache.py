import shutil
from pathlib import Path

import filelock
import pytest

from pydiverse.pipedag import materialize, Table, Flow, Stage, PipedagConfig
import sqlalchemy as sa
import pandas as pd

from pydiverse.pipedag.backend.table.sql import sa_select
from pydiverse.pipedag.context import StageLockContext


def get_flow():
    @materialize(lazy=True)
    def lazy_task_1():
        return Table(
            sa_select([sa.literal(1).label("x"), sa.literal(2).label("y")]), "lazy_1"
        )

    @materialize(lazy=True, input_type=sa.Table)
    def lazy_task_2(input1: sa.Table, input2: sa.Table):
        query = sa_select([(input1.c.x * 5).label("x5"), input2.c.a]).select_from(
            input1.outerjoin(input2, input2.c.x == input1.c.x)
        )
        return Table(query, name="task_2_out", primary_key=["a"])

    @materialize(lazy=True, input_type=sa.Table)
    def lazy_task_3(input: sa.Table, my_stage: Stage):
        return Table(
            sa.text(f"SELECT * FROM {my_stage.transaction_name}.{input.name}"), "lazy_3"
        )

    @materialize(nout=2)
    def eager_inputs():
        dfA = pd.DataFrame(
            {
                "a": [0, 1, 2, 4],
                "b": [9, 8, 7, 6],
            }
        )
        dfB = pd.DataFrame(
            {
                "a": [2, 1, 0, 1],
                "x": [1, 1, 2, 2],
            }
        )
        return Table(dfA, "dfA"), Table(dfB, "dfB")

    @materialize(input_type=pd.DataFrame)
    def eager_task(tbl1: pd.DataFrame, tbl2: pd.DataFrame):
        return Table(tbl1.merge(tbl2, on="x"), "eager")

    with Flow() as flow:
        with Stage("stage_1"):
            lazy_1 = lazy_task_1()
            a, b = eager_inputs()

        with Stage("stage_2") as stage2:
            eager = eager_task(lazy_1, b)
            lazy_2 = lazy_task_2(lazy_1, b)
            lazy_3 = lazy_task_3(eager, stage2)

    return flow, eager, lazy_2, lazy_3


@pytest.mark.parametrize(
    "instance", ["local_table_cache", "local_table_cache_inout", "local_table_store"]
)
def test_local_table_cache(instance):
    store_only_inputs = instance == "local_table_cache"
    use_cache = instance != "local_table_store"
    flow, eager, lazy_2, lazy_3 = get_flow()
    cfg = PipedagConfig.default.get(instance)

    base_dir = Path("/tmp/pipedag/table_cache/pipedag_default")
    base_dir.mkdir(parents=True, exist_ok=True)
    with filelock.FileLock(base_dir.parent / "lock"):
        shutil.rmtree(base_dir)

        # Run flow 1st time
        with StageLockContext():
            result = flow.run(cfg)
            stage_1_files = [
                p.name
                for p in (base_dir / "stage_1").iterdir()
                if p.name.endswith(".parquet")
            ]
            stage_2_files = [
                p.name
                for p in (base_dir / "stage_2").iterdir()
                if p.name.endswith(".parquet")
            ]
            assert result.successful
            if store_only_inputs:
                # only input tables of eager tasks are cached
                assert sorted(stage_1_files) == ["dfb.parquet", "lazy_1.parquet"]
                assert stage_2_files == []
            else:
                assert sorted(stage_1_files) == [
                    "dfa.parquet",
                    "dfb.parquet",
                    "lazy_1.parquet",
                ]
                assert sorted(stage_2_files) == ["eager.parquet"]
            # attention: the following calls will create further parquet files
            assert result.get(eager, as_type=pd.DataFrame)["a"].sum() == 3
            assert result.get(lazy_2, as_type=pd.DataFrame)["a"].sum() == 3
            assert result.get(lazy_3, as_type=pd.DataFrame)["a"].sum() == 3
            # inject a change in cached file without invalidating it
            path = str(base_dir / "stage_2" / "lazy_3.parquet")
            df = pd.read_parquet(path)
            df["a"] += 1
            df.to_parquet(path)
            # test that result.get also uses cache
            assert (
                result.get(lazy_3, as_type=pd.DataFrame)["a"].sum() == 5
                if use_cache
                else 3
            )

        if use_cache:
            # inject a change in cached file without invalidating it
            path = f"{base_dir}/stage_1/lazy_1.parquet"
            df = pd.read_parquet(path)
            df["x"] += 1  # switches join key from 1 to 2 => a=0,1
            df.to_parquet(path)
            # Run flow 2nd time and expect load from local file cache
            with StageLockContext():
                result = flow.run(cfg)
                assert result.successful
                assert result.get(eager, as_type=pd.DataFrame)["a"].sum() == 1
                assert result.get(lazy_2, as_type=pd.DataFrame)["a"].sum() == 3
                assert result.get(lazy_3, as_type=pd.DataFrame)["a"].sum() == 1
