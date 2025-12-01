# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

# This is a somewhat realistic example pipeline showing typical stages in `def get_pipeline()`.
# Currently, pydiverse transform does not support snowflake. That is why this example is comparable to
# example_postgres/realistic_pipeline_sqa.py and uses sqlalchemy instead.

import os
from dataclasses import dataclass

import pandas as pd
import polars as pl
import sqlalchemy as sa
import xgboost
import xgboost as xgb

import pydiverse.colspec as cs
from pydiverse.pipedag import AUTO_VERSION, Blob, ConfigContext, Flow, Stage, Table, Task, TaskGetItem, materialize


def trim_all_str(tbl):
    def trim(col):
        return sa.func.trim(col).label(col.name) if isinstance(col.type, sa.String) else col

    return sa.select(*[trim(c) for c in tbl.c])


def pk(x: sa.Alias):
    # does not reliably work with views used as long as stage is still 100% cache valid
    # (will be fixed by https://github.com/pydiverse/pydiverse.pipedag/issues/298)
    # return x.primary_key[0]
    # workaround:
    return x.c.pk


def pk_match(x: sa.Alias, y: sa.Alias):
    # it would also be possible to determine the match set
    # between the primary keys of the two tables and to create
    # a match expression for all column names in this match set.
    return pk(x) == pk(y)


def pk_names(x: sa.Alias) -> list[str]:
    # unfortunately this doesn't reliably work due to https://github.com/pydiverse/pydiverse.pipedag/issues/298
    # return [c.name for c in x.primary_key]
    # workaround:
    return ["pk"]


def select(*args, **kwargs):
    """Support keyword syntax for sa.select call with sqlalchemy."""
    return sa.select(*args, *[col.label(name) for name, col in kwargs.items()])


def pk_polars(col_spec: cs.ColSpec):
    return [getattr(col_spec, name).polars for name in col_spec.primary_keys()]


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


@materialize(version="1.0.1")
def read_input_data(src_dir="data/pipedag_example_data"):
    src_dir = os.environ.get("DATA_DIR_PREFIX", "") + src_dir
    return [
        Table(pd.read_csv(os.path.join(src_dir, file)), name=file.removesuffix(".csv.gz"), primary_key="pk")
        for file in os.listdir(src_dir)
        if file.endswith(".csv.gz")
    ]


@materialize(input_type=sa.Table, lazy=True)
def clean(src_tbls: list[sa.Alias]):
    return {tbl.name: Table(trim_all_str(tbl), name=tbl.name, primary_key=pk_names(tbl)) for tbl in src_tbls}


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


@materialize(input_type=sa.Table, lazy=True)
def aa(a: sa.Alias, b: sa.Alias) -> Table:
    b_cols = [c for c in b.c if c.name != "pk"]
    return Table(sa.select(a, *b_cols).select_from(a.outerjoin(b, pk_match(a, b))), "aa", primary_key=pk_names(a))


@materialize(input_type=sa.Table, lazy=True)
def bb(b: sa.Alias) -> Table:
    return Table(select(pk=b.c.pk, x=b.c.x, yz=b.c.y * b.c.z).select_from(b), "bb", primary_key=pk_names(b))


@materialize(input_type=sa.Table, lazy=True)
def cc(c: sa.Alias) -> Table:
    return Table(select(pk=c.c.pk, x=c.c.x, yz=2 * c.c.y * c.c.z, xz=c.c.x * c.c.z), "cc", primary_key=pk_names(c))


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


@materialize(input_type=sa.Table, version="1.0.0")
def check_completeness(economic: EconomicRepresentation):
    # A colspec Collection can both be used during flow definition code and
    # within tasks because it is a @dataclass. It can be seen as a dictionary
    # where all possible keys are defined upfront.
    has_cc = economic.cc is not None
    cfg = ConfigContext.get()
    query = sa.select(sa.func.count(sa.text("*")).label("cnt")).select_from(economic.bb)
    with cfg.store.table_store.engine_connect() as conn:
        res = conn.execute(query)
        cnt_bb = res.fetchone()[0]
    return Table(pl.DataFrame(dict(has_cc=has_cc, cnt_bb=cnt_bb)), "completeness_check")


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


@materialize(input_type=sa.Table, lazy=True)
def lazy_features(a: AaColSpec, b: BbColSpec) -> Table:  # LazyFeatures
    return Table(
        select(pk=pk(a), aiige=a.c.age, y=b.c.yz, z=b.c.yz * 2).select_from(a.outerjoin(b, pk_match(a, b))),
        "lazy_features",
        primary_key=pk_names(a),
    )


@materialize(input_type=pl.LazyFrame, version=AUTO_VERSION)
def eager_features(a: AaColSpec, c: CcColSpec) -> Table:  # EagerFeatures:
    return Table(
        a.join(c, on=AaColSpec.primary_keys()).select(
            pk=pk_polars(AaColSpec)[0], xx=CcColSpec.x.polars, yy=CcColSpec.yz.polars * 2, zz=CcColSpec.xz.polars + 3
        ),
        "eager_features",
        primary_key=AaColSpec.primary_keys(),
    )


@materialize(input_type=sa.Table, lazy=True)
def combine_features(features1: EagerFeatures, features2: LazyFeatures) -> Table:  # CombinedFeatures:
    features2_cols = [c for c in features2.c if c.name != "pk"]
    return Table(
        sa.select(features1, *features2_cols).select_from(
            features1.outerjoin(features2, pk_match(features1, features2))
        ),
        "features",
        primary_key=pk_names(features1),
    )


def compute_features(economic: EconomicRepresentation):
    # When working with entities (tables or column groups), it is actually nice
    # to see explicit wiring code (which entities depend on which others).
    features1 = lazy_features(economic.aa, economic.bb)
    features2 = eager_features(economic.aa, economic.cc)
    return combine_features(features1, features2)


class FlatTable(AaColSpec, CombinedFeatures):
    pass


@materialize(input_type=sa.Table, lazy=True, nout=2)
def train_and_test_set(
    base_table: AaColSpec, features: CombinedFeatures
) -> tuple[Table, Table]:  # tuple[FlatTable, FlatTable]:
    features_cols = [c for c in features.c if c.name != "pk"]
    tbl = Table(
        sa.select(
            base_table, *features_cols, sa.func.row_number().over(order_by=pk(base_table)).label("row_num")
        ).select_from(base_table.outerjoin(features, pk_match(base_table, features))),
        name="_prepare_split",
    ).materialize()  # materialize subquery

    cols = [c for c in tbl.c if c.name != "row_num"]
    training_set = Table(sa.select(*cols).where(tbl.c.row_num % 10 != 0), "training_set")
    test_set = Table(sa.select(*cols).where(tbl.c.row_num % 10 == 0), "test_set")

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


@materialize(input_type=pd.DataFrame, version="3.4.5")
def model_evaluation(model: xgboost.Booster, test_set: pd.DataFrame):
    prediction = predict(model, test_set)
    test_set["prediction"] = prediction
    result = test_set[["target", "prediction"]].copy()
    result["abs_error"] = (result["prediction"] - result["target"]).abs()

    return Table(result, "evaluation")


def main():
    flow = get_pipeline()
    result = flow.run()
    assert result.successful


if __name__ == "__main__":
    import logging

    from pydiverse.common.util.structlog import setup_logging

    setup_logging(log_level=logging.INFO)

    # Run this pipeline with (might take a bit longer on first run in pixi environment):
    # ```shell
    # export SNOWFLAKE_ACCOUNT=<use your snowflake instance>;
    # export SNOWFLAKE_PASSWORD=<use secret token>;
    # export SNOWFLAKE_USER=<username>;
    # export SNOWFLAKE_DB_SUFFIX=_adhoc;  # chose unique suffix to avoid collisions
    # pixi run python realistic_pipeline.py
    # ```

    main()
