from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import StageLockContext
from pydiverse.pipedag.core.config import PipedagConfig


@pytest.fixture(
    scope="function",
    params=[
        "pipedag_complex.yaml",
        "pipedag_anchor.yaml",
    ],
)
def cfg_file_path(request):
    password_cfg_path = str(Path(__file__).parent / "postgres_password.yaml")

    old_environ = dict(os.environ)
    os.environ["POSTGRES_PASSWORD_CFG"] = password_cfg_path

    yield Path(__file__).parent / request.param

    os.environ.clear()
    os.environ.update(old_environ)


########################################################################################


dfA_source = pd.DataFrame(
    {
        "a": [0, 1, 2, 4],
        "b": [9, 8, 7, 6],
    }
)
dfA = dfA_source.copy()
input_hash = hash(str(dfA))


def has_new_input():
    global input_hash
    return input_hash


@materialize(nout=2, cache=has_new_input, version="1.0")
def input_task():
    global dfA
    return Table(dfA, "dfA"), Table(dfA, "dfB")


def has_copy_source_fresh_input(
    stage: Stage, attrs: dict[str, Any], pipedag_config: PipedagConfig
):
    source = attrs["copy_source"]
    per_user = attrs["copy_per_user"]
    source_cfg = pipedag_config.get(instance=source, per_user=per_user)
    with source_cfg:
        _hash = source_cfg.store.table_store.get_stage_hash(stage)
    return _hash


@materialize(input_type=pd.DataFrame, cache=has_copy_source_fresh_input, version="1.0")
def copy_filtered_inputs(
    stage: Stage, attrs: dict[str, Any], pipedag_config: PipedagConfig
):
    source = attrs["copy_source"]
    per_user = attrs["copy_per_user"]
    filter_cnt = attrs["copy_filter_cnt"]
    tbls = _get_source_tbls(source, per_user, stage, pipedag_config)
    ret = [Table(tbl.head(filter_cnt), name) for name, tbl in tbls.items()]
    return ret


def _get_source_tbls(source, per_user, stage, pipedag_config):
    source_cfg = pipedag_config.get(instance=source, per_user=per_user)
    with source_cfg:
        # This is just quick hack code to copy data from one pipeline instance to
        # another in a filtered way. It justifies actually a complete pydiverse
        # package called pydiverse.testdata. We want to achieve loose coupling by
        # pipedag transporting uninterpreted attrs with user code feeding the
        # attributes in testdata functionality
        engine = source_cfg.store.table_store.engine
        schema = source_cfg.store.table_store.get_schema(stage.name)
        meta = sa.MetaData()
        meta.reflect(bind=engine, schema=schema.name)
        tbls = {
            tbl.name: pd.read_sql_table(tbl.name, con=engine, schema=schema.name)
            for tbl in meta.tables.values()
        }
    return tbls


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    return Table(df.transform(lambda x: x * 2))


@materialize(nout=2, input_type=sa.Table, lazy=True)
def extract_a_b(tbls: list[sa.Table]):
    a = [tbl for tbl in tbls if tbl.original.name == "dfa"][0]
    b = [tbl for tbl in tbls if tbl.original.name == "dfb"][0]
    return a, b


# noinspection PyTypeChecker
def get_flow(attrs: dict[str, Any], pipedag_config):
    with Flow("test_instance_selection") as flow:
        with Stage("stage_1") as stage:
            if not attrs["copy_filtered_input"]:
                a, b = input_task()
            else:
                tbls = copy_filtered_inputs(stage, attrs, pipedag_config)
                a, b = extract_a_b(tbls)
            a2 = double_values(a)

        with Stage("stage_2"):
            b2 = double_values(b)
            a3 = double_values(a2)

    return flow, b2, a3


def test_instance_selection(cfg_file_path):
    # At this point, an instance is chosen from multi-pipedag-instance
    # configuration file
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance="full")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    with StageLockContext():
        result = flow.run(config=cfg)
        check_result(result, out1, out2)

    cfg = pipedag_config.get(instance="midi")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    with StageLockContext():
        result = flow.run(config=cfg)
        check_result(result, out1, out2, head=2)

    cfg = pipedag_config.get(instance="mini")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    with StageLockContext():
        result = flow.run(config=cfg)
        check_result(result, out1, out2, head=1)


def check_result(result, out1, out2, *, head=999):
    assert result.successful
    v_out1, v_out2 = result.get(out1), result.get(out2)
    pd.testing.assert_frame_equal(dfA_source.head(head) * 2, v_out1, check_dtype=False)
    pd.testing.assert_frame_equal(dfA_source.head(head) * 4, v_out2, check_dtype=False)


# In the future, the following test functions should be auto-generatable
# via pydiverse.pipetest library based on tags in pipedag_complex.yaml


@pytest.mark.slow5
def test_run_full_instance(cfg_file_path):
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance="full")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    flow.run(config=cfg)


@pytest.mark.slow4
def test_run_midi_instance(cfg_file_path):
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance="midi")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    flow.run(config=cfg)


@pytest.mark.slow3
def test_midi_instance_stages(cfg_file_path):
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance="midi")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    flow.run(config=cfg, stages=["simple_flow_stage2"])


@pytest.mark.slow2
def test_run_mini_instance(cfg_file_path):
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance="mini")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    flow.run(config=cfg)


@pytest.mark.slow1
def test_mini_instance_stages(cfg_file_path):
    pipedag_config = PipedagConfig(cfg_file_path)
    cfg = pipedag_config.get(instance="mini")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    flow.run(config=cfg, stages=["simple_flow_stage2"])
