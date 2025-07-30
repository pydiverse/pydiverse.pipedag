# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import os
from dataclasses import dataclass

import pandas as pd
import xgboost
import xgboost as xgb

import pydiverse.colspec as cs
import pydiverse.transform as pdt
from pydiverse.common import String
from pydiverse.pipedag import Blob, Flow, Stage, Table, Task, TaskGetItem, materialize
from pydiverse.transform.extended import (
    C,
    alias,
    drop,
    export,
    left_join,
    mutate,
    row_number,
    select,
    summarize,
)


@pdt.verb
def transmute(tbl, **kwargs):
    return tbl >> select() >> mutate(**kwargs)


@pdt.verb
def trim_all_str(tbl):
    changes = {}
    for col in tbl:
        if isinstance(col.dtype(), String):
            changes[col.name] = col.str.strip()
    return tbl >> mutate(**changes)


def pk(x: pdt.Table):
    # This is just a placeholder.
    # Ideally, there would be a global function in pydiverse transform to
    # get the primary key of a table (Information is available in DB).
    return x.pk


def pk_match(x: pdt.Table, y: pdt.Table):
    return pk(x) == pk(y)


def get_pipeline():
    with Flow("flow") as flow:
        with Stage("1_raw_input"):
            raw_tbls = read_input_data()

        with Stage("2_clean_input"):
            clean_tbls = clean(raw_tbls)

        with Stage("3_transformed_data"):
            economic: EconomicRepresentation = transform(clean_tbls)
            _ = check_completeness(economic)

        with Stage("4_features"):
            features = compute_features(economic)

        with Stage("5_model"):
            train_set, test_set = train_and_test_set(economic.aa, features)
            model = model_training(train_set)

        with Stage("6_evaluation"):
            _ = model_evaluation(model, test_set)

    return flow


@materialize(version="1.0.0")
def read_input_data(src_dir="data/pipedag_example_data"):
    src_dir = os.environ.get("DATA_DIR_PREFIX", "") + src_dir
    return [
        Table(pd.read_csv(os.path.join(src_dir, file)), name=file.removesuffix(".csv.gz"))
        for file in os.listdir(src_dir)
        if file.endswith(".csv.gz")
    ]


@materialize(input_type=pdt.SqlAlchemy, lazy=True)
def clean(src_tbls: list[pdt.Table]):
    return {tbl._ast.name: tbl >> trim_all_str() for tbl in src_tbls}


class AaColSpec(cs.ColSpec):
    pk = cs.String(primary_key=True)
    age = cs.Int32(min=0)
    target = cs.UInt8(min=0, max=1)


class BbColSpec(cs.ColSpec):
    pk = cs.String(primary_key=True)
    x = cs.Float64()
    yz = cs.Float64()


class CcColSpec(BbColSpec):
    xz = cs.Float64()


@dataclass
class EconomicRepresentation(cs.Collection):
    aa: AaColSpec
    bb: BbColSpec
    cc: CcColSpec | None


@materialize(input_type=pdt.SqlAlchemy, lazy=True)
def aa(a: pdt.Table, b: pdt.Table) -> AaColSpec:
    return a >> left_join(b >> select(), pk_match(a, b)) >> alias("aa")


@materialize(input_type=pdt.SqlAlchemy, lazy=True)
def bb(b: pdt.Table) -> BbColSpec:
    return b >> transmute(pk=b.pk, x=b.x, yz=b.y * b.z) >> alias("bb")


@materialize(input_type=pdt.SqlAlchemy, lazy=True)
def cc(c: pdt.Table) -> CcColSpec:
    return c >> transmute(pk=c.pk, x=c.x, yz=2 * c.y * c.z, xz=c.x * c.z) >> alias("cc")


def transform(src_tbls: dict[str, Task | TaskGetItem]) -> EconomicRepresentation:
    # Even though EconomicRepresentation is a @dataclass, it might be more convenient
    # to build it table by table instead of calling the constructor. Both is possible though.
    economic = EconomicRepresentation.build()

    # When building the representation for economic reasoning, it is actually nice to
    # have one task per output table and to wire them explicitly to input tables.
    economic.aa = aa(src_tbls["a"], src_tbls["b"])
    economic.bb = bb(src_tbls["b"])
    economic.cc = cc(src_tbls["c"])

    economic.finalize()
    return economic


@materialize(input_type=pdt.SqlAlchemy, version="1.0.0")
def check_completeness(economic: EconomicRepresentation):
    # A colspec Collection can both be used during flow definition code and
    # within tasks because it is a @dataclass. It can be seen as a dictionary
    # where all possible keys are defined upfront.
    has_cc = economic.cc is not None
    cnt_bb = economic.bb >> summarize(cnt=pdt.count()) >> export(pdt.Scalar)
    return pdt.Table(dict(has_cc=has_cc, cnt_bb=cnt_bb), name="completeness_check")


class LazyFeatures(cs.ColSpec):
    pk = cs.String(primary_key=True)
    aiige = cs.Int32(min=0)
    y = cs.Float64()
    z = cs.Float64()


class EagerFeatures(cs.ColSpec):
    pk = cs.String(primary_key=True)
    xx = cs.Float64()
    yy = cs.Float64()
    zz = cs.Float64()


class CombinedFeatures(LazyFeatures, EagerFeatures):
    pass


@materialize(input_type=pdt.SqlAlchemy, lazy=True)
def lazy_features(a: AaColSpec, b: BbColSpec) -> LazyFeatures:
    return (
        a
        >> left_join(b, pk_match(a, b))
        >> transmute(pk=pk(a), aiige=a.age, y=b.yz, z=b.yz * 2)
        >> alias("lazy_features")
    )


@materialize(input_type=pdt.Polars, version="2.3.5")
def eager_features(a: AaColSpec, c: CcColSpec) -> EagerFeatures:
    return (
        a
        >> left_join(c, pk_match(a, c))
        >> transmute(pk=pk(a), xx=c.x, yy=c.yz * 2, zz=c.xz + 3)
        >> alias("eager_features")
    )


@materialize(input_type=pdt.SqlAlchemy, lazy=True)
def combine_features(features1: EagerFeatures, features2: LazyFeatures) -> CombinedFeatures:
    return features1 >> left_join(features2 >> drop(pk(features2)), pk_match(features1, features2)) >> alias("features")


def compute_features(economic: EconomicRepresentation):
    # When working with entities (tables or column groups), it is actually nice
    # to see explicit wiring code (which entities depend on which others).
    features1 = lazy_features(economic.aa, economic.bb)
    features2 = eager_features(economic.aa, economic.cc)
    return combine_features(features1, features2)


class FlatTable(AaColSpec, CombinedFeatures):
    pass


@materialize(input_type=pdt.SqlAlchemy, lazy=True, nout=2)
def train_and_test_set(base_table: AaColSpec, features: CombinedFeatures) -> tuple[FlatTable, FlatTable]:
    tbl = Table(
        base_table
        >> left_join(features >> drop(pk(features)), pk_match(base_table, features))
        >> mutate(row_num=row_number(arrange=[pk(base_table)])),
        name="_prepare_split",
    ).materialize()  # materialize subquery

    training_set = tbl >> pdt.filter(C.row_num % 10 != 0) >> drop(C.row_num) >> alias("training_set")
    test_set = tbl >> pdt.filter(C.row_num % 10 == 0) >> drop(C.row_num) >> alias("test_set")

    return (training_set, test_set)


@materialize(input_type=pd.DataFrame, version="4.5.8")
def model_training(train_set: FlatTable):
    x = train_set.drop(["target", "pk"], axis=1)
    y = train_set["target"]
    dtrain = xgb.DMatrix(x, label=y)

    params = {"max_depth": 2, "eta": 1, "objective": "binary:logistic"}
    model = xgb.train(params, dtrain)

    return Blob(model, "model")


def predict(model: xgboost.Booster, test_set: FlatTable):
    x = test_set.drop(["target", "pk"], axis=1)

    dx = xgb.DMatrix(x)
    predict_col = model.predict(dx)

    return predict_col


@materialize(input_type=pdt.Polars, version="3.4.5")
def model_evaluation(model: xgboost.Booster, test_set: FlatTable):
    test_set_df = test_set >> export(pdt.Pandas)
    prediction = predict(model, test_set_df)
    test_set_df["prediction"] = prediction

    return (
        pdt.Table(test_set_df)
        >> select(C.target, C.prediction)
        >> mutate(abs_error=(C.target - C.prediction).abs())
        >> alias("evaluation")
    )


def main():
    flow = get_pipeline()
    result = flow.run()
    assert result.successful


if __name__ == "__main__":
    import logging

    from pydiverse.common.util.structlog import setup_logging

    setup_logging(log_level=logging.INFO)

    # Run docker-compose in separate shell to launch postgres container:
    # ```shell
    # pixi run docker-compose up
    # ```

    # Run this pipeline with (might take a bit longer on first run in pixi environment):
    # ```shell
    # pixi run python realistic_pipeline.py
    # ```

    main()
