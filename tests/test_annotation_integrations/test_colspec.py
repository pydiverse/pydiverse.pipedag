# Copyright (c) QuantCo and pydiverse contributors 2024-2025
# SPDX-License-Identifier: BSD-3-Clause
import functools
import operator
import types
from dataclasses import dataclass

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import pydiverse.pipedag as dag
from pydiverse.pipedag import Flow, Stage, materialize
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances

try:
    import pydiverse.colspec as cs
    import pydiverse.common as pdc
    import pydiverse.transform as pdt
except ImportError:
    cs = types.ModuleType("pydiverse.colspec")
    fn = lambda *args, **kwargs: (lambda *args, **kwargs: None)  # noqa: E731
    cs.__getattr__ = lambda name: object if name in ["ColSpec", "Collection"] else fn
    pdt = types.ModuleType("pydiverse.colspec")
    pdt.Table = None
    pdt.verb = lambda fn: fn
    pdt.SqlAlchemy = None
    pdt.Polars = None
    pdc = None


pytestmark = [
    pytest.mark.pdtransform,
    with_instances(DATABASE_INSTANCES),
]

# ------------------------------------------------------------------------------------ #
#                                        SCHEMA                                        #
# ------------------------------------------------------------------------------------ #


class MyFirstColSpec(cs.ColSpec):
    a = cs.Integer(primary_key=True)
    b = cs.Integer()
    c = cs.String  # cs.Enum(["x", "y"], nullable=True)


class MySecondColSpec(cs.ColSpec):
    a = cs.Integer(primary_key=True)
    b = cs.Integer(min=1)
    c = cs.String(nullable=False)  # cs.Enum(["x", "y"], nullable=False)


@dataclass
class MyCollection(cs.Collection):
    first: MyFirstColSpec
    second: MySecondColSpec

    @cs.filter()
    def equal_primary_keys(self):
        return functools.reduce(
            operator.and_,
            (self.first[key] == self.second[key] for key in self.common_primary_keys()),
        )

    @cs.filter()
    def first_b_greater_second_b(self):
        return (
            (self.first.b > self.second.b)
            | self.pk_is_null(self.first)
            | self.pk_is_null(self.second)
        )


@dataclass
class SimpleCollection(cs.Collection):
    first: MyFirstColSpec
    second: MySecondColSpec


# ------------------------------------------------------------------------------------ #
#                                         TESTS                                        #
# ------------------------------------------------------------------------------------ #


def data_without_filter_without_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table(
        {"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", None]}, name="first"
    )
    second = pdt.Table(
        {"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", "x"]}, name="second"
    )
    return first, second


def data_without_filter_with_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table(
        {"a": [1, 2, 1], "b": [1, 2, 3], "c": ["x", "y", None]}, name="first"
    )
    second = pdt.Table(
        {"a": [1, 2, 3], "b": [0, 1, 2], "c": [None, "y", "x"]}, name="second"
    )
    return first, second


def data_with_filter_without_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table(
        {"a": [1, 2, 3], "b": [1, 1, 3], "c": ["x", "y", None]}, name="first"
    )
    second = pdt.Table(
        {"a": [2, 3, 4, 5], "b": [1, 2, 3, 4], "c": ["x", "y", "x", "x"]}, name="second"
    )
    return first, second


def data_with_filter_with_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table(
        {"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", None]}, name="first"
    )
    second = pdt.Table(
        {"a": [2, 3, 4, 5, 6], "b": [0, 1, 2, 3, -1], "c": [None, "y", "y", "x", "z"]},
        name="second",
    )
    return first, second


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    pdt.Table is None, reason="pydiverse.transform needs to be installed"
)
def test_dataclass():
    first, second = data_without_filter_without_rule_violation()
    c = SimpleCollection(first, second)
    assert_frame_equal(
        c.first >> pdt.export(pdt.Polars), first >> pdt.export(pdt.Polars)
    )
    assert_frame_equal(
        c.second >> pdt.export(pdt.Polars), second >> pdt.export(pdt.Polars)
    )


@materialize(nout=2)
def get_data(name: str):
    return globals()[f"data_{name}"]()


@pdt.verb
def materialize_tbl(tbl: pdt.Table, table_prefix: str | None = None):
    # use imperative materialization of pipedag within pydiverse transform task
    name = table_prefix or ""
    name += tbl._ast.name or ""
    name += "%%"
    return dag.Table(tbl, name=name).materialize()


@materialize(nout=3, input_type=pdt.SqlAlchemy)
def exec_filter(c: cs.Collection):
    cfg = cs.config.Config.default
    cfg.materialize_hook = lambda df, table_prefix: df >> materialize_tbl(table_prefix)
    out, failure = c.filter(cfg=cfg, cast=True)
    return (
        out,
        SimpleCollection(
            **{
                name: f.invalid_rows >> pdt.alias(name + "_invalid_rows")
                for name, f in failure.__dict__.items()
            }
        ),
        SimpleCollection(**{name: f.counts() for name, f in failure.__dict__.items()}),
    )


# -------------------------------------- FILTER -------------------------------------- #


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    pdt.Table is None, reason="pydiverse.transform needs to be installed"
)
def test_filter_without_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        first, second = data_without_filter_without_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert_frame_equal(out.first, first >> pdt.export(pdt.Polars(lazy=True)))
        assert_frame_equal(out.second, second >> pdt.export(pdt.Polars(lazy=True)))
        assert failure.first.select(pl.len()).collect().item() == 0
        assert failure.second.select(pl.len()).collect().item() == 0

    with Flow() as flow:
        with Stage("s01"):
            c = SimpleCollection(*get_data("without_filter_without_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    pdt.Table is None, reason="pydiverse.transform needs to be installed"
)
def test_filter_without_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_without_filter_with_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert len(out.first.collect()) == 1
        assert len(out.second.collect()) == 2
        assert failure_counts.first == {"_primary_key_": 2}
        assert failure_counts.second == {"b|min": 1, "c|nullability": 1}

    with Flow() as flow:
        with Stage("s01"):
            c = SimpleCollection(*get_data("without_filter_with_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    pdt.Table is None, reason="pydiverse.transform needs to be installed"
)
def test_filter_with_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_with_filter_without_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(
            out.first,
            pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(c=pl.String)),
        )
        assert_frame_equal(
            out.second,
            pl.LazyFrame({"a": [3], "b": [2], "c": ["y"]}).cast(dict(c=pl.String)),
        )
        assert failure_counts.first == {
            "equal_primary_keys": 1,
            "first_b_greater_second_b": 1,
        }
        assert failure_counts.second == {
            "equal_primary_keys": 2,
            "first_b_greater_second_b": 1,
        }

    with Flow() as flow:
        with Stage("s01"):
            c = MyCollection(*get_data("with_filter_without_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    pdt.Table is None, reason="pydiverse.transform needs to be installed"
)
def test_filter_with_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_with_filter_with_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(
            out.first,
            pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(c=pl.String)),
        )
        assert_frame_equal(
            out.second,
            pl.LazyFrame({"a": [3], "b": [1], "c": ["y"]}).cast(dict(c=pl.String)),
        )
        assert failure_counts.first == {"equal_primary_keys": 2}
        assert failure_counts.second == {
            "b|min": 2,
            "c|nullability": 1,
            # "c|dtype": 1,  # Enum currently doesn't work in pydiverse transform
            "equal_primary_keys": 2,
        }

    with Flow() as flow:
        with Stage("s01"):
            c = MyCollection(*get_data("with_filter_with_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter(c)
            assertions(out, failure, failure_counts)

    flow.run()
