# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import contextlib
import datetime as dt
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from types import GenericAlias
from typing import get_args, get_origin

import pytest
import structlog

from pydiverse.pipedag import (
    Blob,
    ConfigContext,
    Flow,
    PipedagConfig,
    RawSql,
    Stage,
    Table,
)
from pydiverse.pipedag.context import RunContext, RunContextServer, default_config_dict
from pydiverse.pipedag.context.context import test_store_config_dict
from pydiverse.pipedag.context.trace_hook import TraceHook
from pydiverse.pipedag.util.json import PipedagJSONDecoder, PipedagJSONEncoder


def deep_cmp(a, b):
    if a != b:
        logger = structlog.get_logger("test_json.deep_cmp")
        logger.info("Found difference", a=a, b=b)
    if isinstance(a, str):
        return a == b
    if isinstance(a, dict):
        return all([deep_cmp(v, b[k]) for k, v in a.items()])
    if isinstance(a, GenericAlias):
        base_cmp = get_origin(a) == get_origin(b)
        return base_cmp and all([deep_cmp(v, w) for v, w in zip(get_args(a), get_args(b))])
    if isinstance(a, Iterable):
        return all([deep_cmp(v, w) for v, w in zip(a, b)])
    if hasattr(a, "__dict__"):

        def fields(x):
            return {k: v for k, v in x.__dict__.items() if not k.startswith("_") and k != "logger"}

        return deep_cmp(fields(a), fields(b))
    return a == b


def check(x, expected_result=None, regex: str | None = None, regex_replace: str | None = None):
    expected_result = expected_result or x
    json_encoder = PipedagJSONEncoder()
    json_decoder = PipedagJSONDecoder()
    y = json_encoder.encode(x)
    if regex:
        regex_replace = regex_replace or ""
        y = re.sub(regex, regex_replace, y, flags=re.MULTILINE)
    z = json_decoder.decode(y)
    assert deep_cmp(expected_result, z)


def test_json_coder_primitive():
    x = {
        "a": 1,
        "b": 2.0,
        "c": "3",
        "d": True,
        "e": None,
        "f": "45",
        "g": "",
    }
    check(x)


def set_stage(x, s: Stage):
    x.stage = s
    return x


def set_cache_key(x, k: str):
    x.cache_key = k
    return x


def _cfg_ctx():
    return ConfigContext.new(default_config_dict | test_store_config_dict, "x", "y", "z")


@contextlib.contextmanager
def _with_dummy_context(*, stage_name: str = "x"):
    # Setup flow so Stage("h") exists
    flow = Flow()
    flow.stages[stage_name] = Stage(stage_name)
    with _cfg_ctx(), RunContext(RunContextServer(flow.get_subflow(), TraceHook())):
        yield


def test_json_coder_table():
    class C:
        t: dt.datetime

    x = {
        "a": Table(),
        "b": Table(name="b"),
        "c": Table(primary_key=["c"]),
        "d": Table(annotation=C.__annotations__["t"]),
        "e": Table(annotation=C.__annotations__),
        "f": Table(annotation=list[int]),
        "g": Table(annotation=dict[str, dt.datetime]),
        "h": set_stage(Table(), Stage("h")),
        "i": set_cache_key(Table(), "i"),
    }
    with _with_dummy_context(stage_name="h"):
        check(x)


def test_json_coder_table_fallback():
    class C:
        t: dt.datetime

    x = {
        "a": Table(),
        "d": Table(name="d", annotation=C.__annotations__["t"]),
        "e": Table(annotation=C.__annotations__),
    }
    expected_result = {
        "a": Table(),
        "d": Table(name="d"),
        "e": Table(),
    }
    check(
        x,
        expected_result,
        regex=r'"annotation":([^,]*{[^}]*(}(?!,)[^}]*)?},|[^,{}]*,)',
        regex_replace="",
    )


def test_json_coder_table_fallback2():
    check(
        Table(annotation=list[int]),
        Table(),
        regex=r'"annotation":[^}]*}[^}]*}[^}]*},',
        regex_replace="",
    )
    check(
        Table(annotation=dict[str, dt.datetime]),
        Table(),
        regex=r'"annotation":[^}]*}[^}]*}[^}]*}[^}]*},',
        regex_replace="",
    )


def test_json_coder_table_obj_remove():
    x = {
        "a": Table(1),
    }
    with pytest.raises(AssertionError):
        check(x)


def test_json_coder_blob():
    class C:
        t: dt.datetime

    x = {
        "a": Blob(),
        "b": Blob(name="b"),
        "c": set_stage(Blob(), Stage("c")),
        "d": set_cache_key(Blob(), "dd"),
    }
    with _with_dummy_context(stage_name="c"):
        check(x)


def test_json_coder_blob_obj_remove():
    x = {
        "a": Blob(1),
    }
    with pytest.raises(AssertionError):
        check(x)


def test_json_coder_raw_sql():
    class C:
        t: dt.datetime

    x = {
        "a": RawSql(),
        "b": RawSql(name="b"),
        "c": set_stage(RawSql(), Stage("c")),
        "d": set_cache_key(RawSql(), "dd"),
        "e": RawSql(separator=" GO "),
        "f": RawSql("select 1"),
    }
    with _with_dummy_context(stage_name="c"):
        check(x)


def test_json_coder_classes():
    x = {
        "a": Stage("a"),
        "c": PipedagConfig.default.get(),
        "d": _cfg_ctx(),
        "e": Path(),
        "f": dt.datetime(1, 2, 3),
        "g": dt.datetime(1, 2, 3, 4, 5),
        "h": dt.datetime(1, 2, 3, 4, 5, 6),
        "i": dt.date(1, 2, 3),
    }
    with _with_dummy_context(stage_name="a"):
        check(x)


def test_json_coder_pipedag_config_only_input():
    # PipedagConfig is not supposed to be deserialized well. It is only needed
    # as input for getting another instance ConfigContext
    x = {
        "b": PipedagConfig.default,
    }
    with pytest.raises(KeyError, match="table_store_connection"):
        check(x)


@dataclass
class C:
    m: int
    n: str
    o: list[int]


@dataclass
class D:
    m: Table
    n: Table[dt.datetime]
    o: Table[int]


def test_json_coder_dataclass():
    x = {
        "a": C(1, "2", [3, 4, 5]),
        "b": D(Table(), Table(), Table()),
        "c": D(Table(), Table(annotation=dt.datetime), Table(annotation=int)),
    }
    check(x)


def test_json_coder_types():
    x = {
        "a": C,
        "b": D,
        "c": int,
        "d": dt.datetime,
        "e": str,
    }
    check(x)


def test_json_coder_fail_local_dataclass():
    @dataclass
    class C:
        m: int
        n: str
        o: list[int]

    x = {
        "a": C(1, "2", [3, 4, 5]),
    }
    with pytest.raises(AttributeError, match="'function' object has no attribute '<locals>'"):
        check(x)
