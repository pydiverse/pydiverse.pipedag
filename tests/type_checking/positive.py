# Copyright (c) QuantCo and pydiverse contributors 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Assertions about the types pipedag's public API infers.

Not a pytest module - see README.md. Everything here must check cleanly, and the
`assert_type` calls pin down the exact inferred types so that a change which widens or
narrows them shows up as a failure.
"""

from typing import Any, assert_type

import pandas as pd
import polars as pl
import sqlalchemy as sa

from pydiverse.pipedag import (
    AUTO_VERSION,
    Blob,
    ConfigContext,
    ExternalTableReference,
    Flow,
    GroupNode,
    MaterializingTask,
    MaterializingTaskGetItem,
    PipedagConfig,
    RawSql,
    Result,
    Schema,
    SortCol,
    SortOrder,
    Stage,
    StageLockContext,
    Table,
    Task,
    TaskGetItem,
    UnboundMaterializingTask,
    View,
    materialize,
)
from pydiverse.pipedag.materialize.materializing_task import MaterializingTask2
from pydiverse.pipedag.materialize.store import BaseTableStore


@materialize(version="1.0", input_type=pd.DataFrame)
def one_frame() -> Table:
    return Table(pd.DataFrame({"x": [1]}))


@materialize(version="1.0", input_type=pd.DataFrame)
def clean(df: pd.DataFrame, drop_na: bool = True) -> Table:
    return Table(df.dropna() if drop_na else df)


@materialize(version="1.0", input_type=pd.DataFrame)
def combine(left: pd.DataFrame, right: pd.DataFrame, suffix: str = "_r") -> Table:
    return Table(left.merge(right, suffixes=("", suffix)))


@materialize(lazy=True, input_type=sa.Table)
def sql_task(tbl: sa.Alias) -> Table:
    return Table(sa.select(tbl))


@materialize(input_type=pl.LazyFrame, version=AUTO_VERSION)
def polars_task(lf: pl.LazyFrame) -> Table:
    return Table(lf)


@materialize
def bare() -> int:
    return 3


@materialize(nout=2)
def two() -> tuple[Table, Table]:
    return Table(pd.DataFrame()), Table(pd.DataFrame())


@materialize(nout=3)
def three() -> tuple[Table, Table, Table]:
    return Table(pd.DataFrame()), Table(pd.DataFrame()), Table(pd.DataFrame())


@materialize(nout=2)
def two_as_list() -> list[Table]:
    return [Table(pd.DataFrame()), Table(pd.DataFrame())]


@materialize(input_type=(pd.DataFrame, "arrow"))
def arrow_backed(df: pd.DataFrame) -> Table:
    return Table(df)


@materialize(input_type={"type": pd.DataFrame, "backend": "numpy"})
def numpy_backed(df: pd.DataFrame) -> Table:
    return Table(df)


@materialize(input_type=sa.Table, lazy=True)
def materialize_imperatively(tbl: sa.Alias) -> Table:
    ref, df = Table(sa.select(tbl)).materialize(return_as_type=[None, pd.DataFrame])
    return Table(ref)


@materialize(nout=10)
def ten() -> list[Table]:
    return [Table(pd.DataFrame()) for _ in range(10)]


@materialize(nout=4)
def four() -> tuple[Table, Table, Table, Table]:
    t = Table(pd.DataFrame())
    return t, t, t, t


NOUT = 4


@materialize(nout=NOUT)
def dynamic() -> tuple[Table, ...]:
    return tuple(Table(pd.DataFrame()) for _ in range(NOUT))


@materialize
def dict_task() -> dict[str, list[int]]:
    return {"x": [0, 1], "y": [2, 3]}


@materialize(input_type=pd.DataFrame)
def takes_container(frames: list[pd.DataFrame], by_name: dict[str, pd.DataFrame]) -> Table:
    return Table(pd.concat([*frames, *by_name.values()]))


# --- The decorator preserves the decorated function's signature -----------------------

assert_type(bare, UnboundMaterializingTask[[], int])
assert_type(one_frame, UnboundMaterializingTask[[], Table])


def declaration_time() -> None:
    with Flow("flow") as flow:
        assert_type(flow, Flow)

        with Stage("stage_1") as stage_1:
            assert_type(stage_1, Stage)

            # Calling a task yields a task object parameterized by the declared return
            # type of the decorated function.
            a = one_frame()
            assert_type(a, MaterializingTask[Table])

            # ... and that task object is accepted wherever the receiving task declares
            # a concrete dematerialized type.
            b = clean(a)
            c = clean(a, drop_na=False)
            _ = combine(b, c, suffix="_right")

            # Cross-input_type wiring is fine: what a task receives depends on the
            # receiving task's input_type, not on the producing task.
            _ = sql_task(a)
            _ = polars_task(a)

            # Containers of tasks, including nested and heterogeneous ones.
            _ = takes_container([a, b], {"a": a, "b": c})
            _ = takes_container([polars_task(a)], {"x": sql_task(a)})

            # Table / Blob objects created at declaration time are valid inputs too.
            _ = sql_task(Table(ExternalTableReference("tbl", schema="ext")))
            _ = clean(Table(pd.DataFrame({"x": [1]})))
            _ = arrow_backed(a)
            _ = numpy_backed(a)
            _ = materialize_imperatively(a)

            # Values annotated with the generic task types are accepted as well.
            src_tbls: dict[str, Task | TaskGetItem] = {"a": a, "x": ten()[0]}
            _ = combine(src_tbls["a"], src_tbls["x"])

        with Stage("stage_2"), GroupNode("group") as group:
            assert_type(group, GroupNode)

            # nout=2 / nout=3 unpacking keeps the element types of the declared tuple.
            x, y = two()
            assert_type(x, MaterializingTaskGetItem[Table])
            assert_type(y, MaterializingTaskGetItem[Table])

            p, q, r = three()
            assert_type(r, MaterializingTaskGetItem[Table])

            # An un-unpacked nout=2 result stays usable as a single task as well.
            whole = two()
            assert_type(whole, MaterializingTask2[Table, Table])

            # nout=2 with a non-tuple return type falls back to the general task type.
            assert_type(two_as_list(), MaterializingTask[list[Table]])
            k, m = two_as_list()
            assert_type(k, MaterializingTaskGetItem[Any])
            _ = combine(k, m)

            # Other nout values fall back to iteration / subscripting: still usable,
            # but each element is opaque and the arity is not checked.
            for item in ten():
                assert_type(item, MaterializingTaskGetItem[Any])
            _ = clean(ten()[0])

            assert_type(four(), MaterializingTask[tuple[Table, Table, Table, Table]])
            e, f, g, h = four()
            assert_type(e, MaterializingTaskGetItem[Any])
            _ = combine(f, g)
            _ = clean(h)

            # A nout that isn't a literal falls back the same way.
            assert_type(dynamic(), MaterializingTask[tuple[Table, ...]])
            i, j = dynamic()
            assert_type(i, MaterializingTaskGetItem[Any])
            _ = combine(i, j)

            # Subscripting and chained subscripting.
            d = dict_task()
            assert_type(d["x"], MaterializingTaskGetItem[Any])
            assert_type(d["x"][1], MaterializingTaskGetItem[Any])

            # Lazy member lookup via __getattr__ (recorded, resolved at run time).
            _ = clean(a.some_field)

            _ = combine(x, y)
            _ = combine(p, q)

    _ = flow.run()
    _ = flow.run(a, stage_1)
    _ = flow.get_subflow(a, x, stage_1)
    inputs = {a: ExternalTableReference("tbl", schema="ext")}
    _ = flow.run(inputs=inputs)


def run_time() -> None:
    flow = Flow("flow")
    with flow, Stage("stage"):
        a = one_frame()

    result = flow.run()
    assert_type(result, Result)
    assert_type(result.successful, bool)

    # The returned object depends on the task's output structure and the table hook
    # (e.g. `as_type=sa.Table` returns an `sa.Alias`), so it is `Any`.
    assert_type(result.get(a, as_type=pd.DataFrame), Any)
    assert_type(result.get(a, (pd.DataFrame, "arrow")), Any)
    assert_type(result.get(a), Any)
    df: pd.DataFrame = result.get(a, as_type=pd.DataFrame)
    _ = df

    assert_type(a.get_output_from_store(as_type=pd.DataFrame), Any)

    with flow, Stage("other"):
        x, _ = two()
        whole = two()
    assert_type(x.get_output_from_store(as_type=pl.DataFrame), Any)
    assert_type(result.get(whole, as_type=pd.DataFrame), Any)


def outside_flow() -> None:
    # Outside a flow declaration, calling a task invokes the original function. That
    # can't be expressed statically, but the `Any` base keeps it from erroring.
    value: int = bare()
    _ = value


def config() -> None:
    cfg = ConfigContext.get()
    assert_type(cfg.instance_name, str)
    assert_type(cfg.evolve(fail_fast=True), ConfigContext)
    assert_type(cfg.store.table_store, BaseTableStore)

    pipedag_config = PipedagConfig.default
    assert_type(pipedag_config.get(instance="x", flow="y"), ConfigContext)

    # Contexts return themselves from __enter__.
    with cfg as entered_cfg, StageLockContext() as lock_ctx:
        assert_type(entered_cfg, ConfigContext)
        assert_type(lock_ctx, StageLockContext)


def containers() -> None:
    tbl = Table(pd.DataFrame(), "name", primary_key=["x"], indexes=[["x"]])
    assert_type(tbl, Table[pd.DataFrame])
    assert_type(tbl.obj, pd.DataFrame | None)
    assert_type(tbl.name, str | None)
    assert_type(tbl.copy_without_obj(), Table[pd.DataFrame])

    blob = Blob({"a": 1})
    assert_type(blob, Blob[dict[str, int]])
    assert_type(blob.name, str | None)

    raw = RawSql("SELECT 1", "raw")
    assert_type(list(raw), list[str])
    assert_type(raw.name, str | None)

    _ = Table(ExternalTableReference("tbl", schema="ext"))
    _ = Table(View(tbl, columns=["x"], sort_by=[SortCol("x", SortOrder.DESC)], limit=10))
    assert_type(Schema("s", prefix="p_").get(), str)


def flow_structure() -> None:
    with Flow("flow") as flow, Stage("stage") as stage:
        _ = one_frame()

    assert_type(flow["stage"], Stage)
    assert_type(flow.get_stage("stage"), Stage)
    assert_type(stage["one_frame"], Task)
    assert_type(stage.get_task("one_frame", 0), Task)
    assert_type(stage.name, str)
    assert_type(stage.current_name, str)
    assert_type(flow.visualize_url(), str)
