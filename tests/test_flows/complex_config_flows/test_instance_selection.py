from __future__ import annotations

from typing import Any

import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.pipedag.context import DAGContext
from pydiverse.pipedag.util.config import PipedagConfig

dfA_source = pd.DataFrame(
    {
        "a": [0, 1, 2, 4],
        "b": [9, 8, 7, 6],
    }
)
dfA = dfA_source.copy()
input_hash = hash(str(dfA))


def has_new_input():
    """
    Returns whether new input is available via input hash.

    It is a test implementation which can be controlled via package scoped variable like is_changed.

    :return:
        hash value of input (stored hash must not exactly be input hash)
    """
    global input_hash
    return input_hash


# noinspection DuplicatedCode
@materialize(nout=2, cache=has_new_input, version="1.0")
def input_task():
    global dfA
    print(f"dfA=\n{dfA}")
    return Table(dfA, "dfA"), Table(dfA, "dfB")


def has_copy_source_fresh_input(attrs: dict[str, Any], pipedag_config: PipedagConfig):
    source = attrs["copy_source"]
    per_user = attrs["copy_per_user"]
    source_cfg = pipedag_config.get(instance=source, per_user=per_user)
    source_stage = DAGContext.get().stage
    flow = DAGContext.get().flow
    return flow.get_cache_hash(source_cfg, source_stage)


@materialize(input_type=pd.DataFrame, cache=has_copy_source_fresh_input, version="1.0")
def copy_filtered_inputs(attrs: dict[str, Any], pipedag_config: PipedagConfig):
    source = attrs["copy_source"]
    per_user = attrs["copy_per_user"]
    filter_cnt = attrs["copy_filter_cnt"]
    source_cfg = pipedag_config.get(instance=source, per_user=per_user)
    source_stage = DAGContext.get().stage
    with source_cfg:
        # This is just quick hack code to copy data from one pipeline instance to another in a filtered way.
        # It justifies actually a complete pydiverse package called pydiverse.testdata. We want to achieve
        # loose coupling by pipedag transporting uninterpreted attrs with user code feeding the
        # attributes in testdata functionality
        engine = source_cfg.store.table_store.orchestration_engine
        schema = source_cfg.store.table_store.get_schema(source_stage.name)
        meta = sa.MetaData()
        meta.reflect(bind=engine, schema=schema)
        tbls = [
            pd.read_sql_table(tbl, con=engine, schema=schema) for tbl in meta.tables
        ]
        tbls = [tbl.head(filter_cnt) for tbl in tbls]
    return tbls


@materialize(input_type=pd.DataFrame, version="1.0")
def double_values(df: pd.DataFrame):
    return Table(df.transform(lambda x: x * 2))


@materialize(nout=2, input_type=sa.Table, lazy=True)
def extract_a_b(tbls: list[sa.Table]):
    a = [tbl for tbl in tbls if tbl.name == "dfA"][0]
    b = [tbl for tbl in tbls if tbl.name == "dfB"][0]
    return a, b


# noinspection PyTypeChecker
def get_flow(attrs: dict[str, Any], pipedag_config):
    with Flow("test_instance_selection") as flow:
        with Stage("stage_1"):
            if not attrs["copy_filtered_input"]:
                a, b = input_task()
            else:
                tbls = copy_filtered_inputs(attrs, pipedag_config)
                a, b = extract_a_b(tbls)
            a2 = double_values(a)

        with Stage("stage_2"):
            b2 = double_values(b)
            a3 = double_values(a2)

    return flow, b2, a3


def test_instance_selection(cfg_file_base_name):
    # at this point, an instance is chosen from multi-pipedag-instance configuration file
    pipedag_config = PipedagConfig.load(base_name=cfg_file_base_name)
    cfg = pipedag_config.get(instance="full")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    result = flow.run(cfg)
    _check_result(result, out1, out2)

    pipedag_config = PipedagConfig.load()
    cfg = pipedag_config.get(instance="full")

    flow, out1, out2 = get_flow(cfg)

    result = flow.run()
    _check_result(result, out1, out2)

    cfg = pipedag_config.get(instance="midi")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    result = flow.run(cfg)
    _check_result(result, out1, out2, head=2)

    cfg = pipedag_config.get(instance="mini")

    flow, out1, out2 = get_flow(cfg.attrs, pipedag_config)

    result = flow.run(cfg)
    _check_result(result, out1, out2, head=1)


def _check_result(result, out1, out2, *, head=999):
    assert result.successful
    v_out1, v_out2 = result.get(out1), result.get(out2)
    pd.testing.assert_frame_equal(dfA_source.head(head) * 2, v_out1)
    pd.testing.assert_frame_equal(dfA_source.head(head) * 4, v_out2)


if __name__ == "__main__":
    test_instance_selection()
