# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

# Compared with realistic_pipeline_sqa_instances.py, this version implements stable pipelines
# simply by filtering on a date column. The assumption is that source table only get new rows
# with newer dates in this column. Please note, that in practice, fully historized tables
# typically have four date columns: valid_from, valid_to, known_from, known_to.
# In this case, known_from would be the right column to filter on for stable pipelines. The
# stable pipeline has to produce output independent of modified known_to dates in the future.

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import polars as pl
import sqlalchemy as sa
import structlog
import xgboost
import xgboost as xgb

import pydiverse.colspec as cs
from pydiverse.pipedag import (
    AUTO_VERSION,
    Blob,
    ConfigContext,
    ExternalTableReference,
    Flow,
    PipedagConfig,
    Stage,
    Table,
    Task,
    TaskGetItem,
    materialize,
)
from pydiverse.pipedag.context.context import CacheValidationMode, StageLockContext


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


def get_pipeline(external_schema, cfg: ConfigContext):
    with Flow("flow") as flow:
        with Stage("1_raw_input"):
            table_references = get_external_references(external_schema, cfg)
            raw_tbls = get_input_data(table_references, cfg)

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


def get_external_references(schema: str, cfg: ConfigContext):
    engine = cfg.store.table_store.engine
    inspector = sa.inspect(engine)
    tables = inspector.get_table_names(schema=schema)
    refs = [Table(ExternalTableReference(name, schema=schema)) for name in sorted(tables)]
    return refs


def get_filter_query(tbl: sa.Alias, cfg: ConfigContext | None = None):
    if cfg is None:
        cfg = ConfigContext.get()
    attrs = cfg.attrs

    query = sa.select(sa.text("*")).select_from(tbl)
    if attrs["src_filtered_input"]:
        # This is oversimplistic filtering with a View that limits number of rows according to src_filter_cnt.
        # Better would be stable hash based sampling on some primary key columns. The relevant primary key
        # must be joined to all large tables for this to work.
        # Another alternative is a filter table in the repo with filtering IDs for choosing rows in
        # mini/midi pipeline instance tables.
        query = query.limit(attrs["src_filter_cnt"])
    if attrs["stable_date"] > "1970-01-01":
        query = query.where(tbl.c.known_from <= attrs["stable_date"])
    return query


def _new_input_hash(tbls: list[Table], cfg: ConfigContext | None = None):
    store = ConfigContext.get().store.table_store

    def tbl_hash(ref: ExternalTableReference, conn) -> int:
        tbl = sa.Table(ref.name, sa.MetaData(), schema=ref.schema, autoload_with=store.engine)
        subquery = get_filter_query(tbl, cfg).alias("sub")
        query = sa.select(
            sa.func.count(sa.text("*")),
            sa.func.sum(sa.func.length(sa.text("pk"))),
            sa.func.max(sa.text("known_from")),
            sa.func.max(sa.text("valid_from")),
        ).select_from(subquery)
        with store.engine_connect() as conn:
            res = conn.execute(query)
            summary_row = tuple(res.fetchone())
        return summary_row

    with store.engine_connect() as conn:
        return tuple({tbl.name: tbl_hash(tbl.obj, conn) for tbl in tbls}.items())


@materialize(lazy=True, input_type=sa.Table, cache=_new_input_hash)
def get_input_data(tbls: list[sa.Alias], cfg: ConfigContext | None = None):
    """
    Get input tables for this pipeline instance direct, filtered, or copied from external_schema.

    It is fine to use radically different implementation, but it is good practice to think in
    fresh/stable data and full/midi/mini input sizes. At least having full_fresh, mini_fresh,
    and full, midi, mini instances are recommended. Using stable data that only changes every n
    months should be used for most of the data pipeline development work.
    See https://pydiversepipedag.readthedocs.io/en/latest/examples/best_practices_instances.html
    """
    return [Table(get_filter_query(tbl, cfg), name=tbl.name) for tbl in tbls]


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
    extra: cs.ColSpec | None


@materialize(input_type=sa.Table, lazy=True)
def aa(a: sa.Alias, b: sa.Alias) -> Table:
    b_cols = [c for c in b.c if c.name != "pk" and not c.name.startswith("valid_") and not c.name.startswith("known_")]
    return Table(sa.select(a, *b_cols).select_from(a.outerjoin(b, pk_match(a, b))), "aa", primary_key=pk_names(a))


@materialize(input_type=sa.Table, lazy=True)
def bb(b: sa.Alias) -> Table:
    return Table(select(pk=b.c.pk, x=b.c.x, yz=b.c.y * b.c.z).select_from(b), "bb", primary_key=pk_names(b))


@materialize(input_type=sa.Table, lazy=True)
def cc(c: sa.Alias) -> Table:
    return Table(select(pk=c.c.pk, x=c.c.x, yz=2 * c.c.y * c.c.z, xz=c.c.x * c.c.z), "cc", primary_key=pk_names(c))


def extra_input_hash(rel_path: str):
    path = os.path.join(os.environ.get("DATA_DIR_PREFIX", ""), rel_path)
    with open(path, "rb") as f:
        data = f.read()
    import hashlib

    return hashlib.md5(data).hexdigest()


@materialize(input_type=sa.Table, cache=extra_input_hash, lazy=True, allow_fresh_input=True)
def load_extra_input(rel_path: str):
    # pretend we want to load an extra CSV file which should be updated even in the stable instances
    path = os.path.join(os.environ.get("DATA_DIR_PREFIX", ""), rel_path)
    tbl = pd.read_csv(path)
    return Table(tbl, "extra_input", primary_key="pk").materialize()


def transform(src_tbls: dict[str, Task | TaskGetItem]) -> EconomicRepresentation:
    # Even though EconomicRepresentation is a @dataclass, it might be more convenient
    # to build it table by table instead of calling the constructor. Both is possible though.
    economic = EconomicRepresentation.build()

    # When building the representation for economic reasoning, it is actually nice to
    # have one task per output table and to wire them explicitly to input tables.
    economic.aa = aa(src_tbls["a"], src_tbls["b"])
    economic.bb = bb(src_tbls["b"])
    economic.cc = cc(src_tbls["c"])

    file = os.path.join("data", "pipedag_example_data", "c.csv.gz")
    economic.extra = load_extra_input(file)

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
    training_set = Table(sa.select(*cols).where((tbl.c.row_num + 9) % 10 != 0), "training_set")
    test_set = Table(sa.select(*cols).where((tbl.c.row_num + 9) % 10 == 0), "test_set")

    return (training_set, test_set)


@materialize(input_type=pd.DataFrame, version="3.6.9")
def model_training(train_set: FlatTable):
    x = train_set.drop(["target", "pk", "valid_from", "valid_to", "known_from", "known_to"], axis=1)
    y = train_set["target"]
    if y.min() == y.max():
        model = "No Model because training target had no variance!"
    else:
        dtrain = xgb.DMatrix(x, label=y)

        params = {"max_depth": 2, "eta": 1, "objective": "binary:logistic"}
        model = xgb.train(params, dtrain)

    return Blob(model, "model")


def predict(model: xgboost.Booster, test_set: FlatTable):
    x = test_set.drop(["target", "pk", "valid_from", "valid_to", "known_from", "known_to"], axis=1)

    if isinstance(model, str):
        logger = structlog.get_logger(__name__)
        logger.warning("no model available for prediction", msg=model)
        predict_col = pd.Series(pd.NA * len(x))[0 : len(x)]
    else:
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


def run_pipeline(instance_id: str, external_schema: str | None = None):
    logger = structlog.get_logger(__name__)
    pipedag_config = PipedagConfig(Path(__file__).parent / "pipedag_with_instances_date.yaml")
    cfg = pipedag_config.get(instance_id)
    logger.info("### Running pipeline ###", instance_id=instance_id, attrs=cfg.attrs)
    flow = get_pipeline(external_schema, cfg)
    result = flow.run(config=cfg)
    assert result.successful


def init_external_input():
    pipedag_config = PipedagConfig(Path(__file__).parent / "pipedag_with_instances_date.yaml")
    instance_id = "full_fresh"  # just use this instance to get working config

    """Simulate external input that can be sourced by the pipeline via its internal database connection."""
    with Flow("external_input_init") as flow:
        with Stage("external_input"):
            tbls = read_input_data()

    cfg = pipedag_config.get(instance_id)
    with StageLockContext():
        result = flow.run(config=cfg, cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
        assert result.successful
        external_schema = result.get(tbls[0], as_type=sa.Table).original.schema

    # attention: this includes schema_prefix="{instance_id}_" from pipedag config
    return external_schema


@materialize(version="1.0.0")
def read_input_data(src_dir="data/pipedag_example_data"):
    src_dir = os.environ.get("DATA_DIR_PREFIX", "") + src_dir

    def read_file(file: str) -> pd.DataFrame:
        path = os.path.join(src_dir, file)
        df = pd.read_csv(path)
        df["valid_from"] = pd.to_datetime("2023-01-01") + pd.to_timedelta(df.index, unit="D")
        df["valid_to"] = pd.Series(["9999-01-01"] * len(df)).astype("datetime64[s]")
        df["known_from"] = pd.to_datetime("2023-01-01") + pd.to_timedelta((df.index // 7 + 1) * 7, unit="D")
        df["known_to"] = pd.to_datetime("2023-01-01") + pd.to_timedelta((df.index // 7 + 2) * 7, unit="D")
        return df

    return [
        Table(read_file(file), name=file.removesuffix(".csv.gz"), primary_key="pk")
        for file in os.listdir(src_dir)
        if file.endswith(".csv.gz")
    ]


def bootstrap_pipeline_instances():
    # Often external input comes via some tables which are updated outside of pipedag control.
    # We simulate this with another little pipeline.
    external_schema = init_external_input()
    # This instance can be used to test new incoming data format (e.g. more tables).
    run_pipeline("mini_fresh", external_schema)
    # This instance runs with fresh data.
    run_pipeline("full_fresh", external_schema)
    # In regular intervals, full (stable) instance input stage is updated either from source or from full_fresh
    run_pipeline("full", external_schema)  # this will load fresh data for the first time
    # Now, full stable can work without pulling fresh input data
    run_pipeline("full", external_schema)  # this should be cache valid since no input data changed
    # Mini instance is used as subset of full to debug much faster (debug cases can be made manually selectable)
    run_pipeline("mini", external_schema)
    # Midi instance should be relevant sample of full (stable) instance to test all pipeline code
    run_pipeline("midi", external_schema)


def main():
    # bootstrap multiple pipeline instances that copy data between each other
    bootstrap_pipeline_instances()


if __name__ == "__main__":
    os.environ["POSTGRES_USERNAME"] = "sa"
    os.environ["POSTGRES_PASSWORD"] = "Pydiverse23"
    import logging

    from pydiverse.common.util.structlog import setup_logging

    setup_logging(log_level=logging.INFO)

    # Run docker-compose in separate shell to launch postgres container:
    # ```shell
    # pixi run docker-compose up
    # ```

    # Run this pipeline with (might take a bit longer on first run in pixi environment):
    # ```shell
    # pixi run python realistic_pipeline_sqa_instances_date.py
    # ```

    main()
