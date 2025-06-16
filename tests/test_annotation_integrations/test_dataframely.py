# Copyright (c) QuantCo and pydiverse contributors 2024-2025
# SPDX-License-Identifier: BSD-3-Clause
import types
from dataclasses import dataclass
from typing import Generic, Mapping, TypeVar

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pydiverse.pipedag import Flow, Stage, materialize
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances

try:
    import dataframely as dy
    from dataframely._polars import FrameType
except ImportError:
    T = TypeVar("T")

    class DyDataFrame(Generic[T]):
        pass

    class DyDummyClass:
        pass

    FrameType = None
    dy = types.ModuleType("dataframely")
    dy.DataFrame = DyDataFrame
    dy.LazyFrame = DyDataFrame
    dy.FailureInfo = None
    dy.Column = None
    dy.Collection = None
    for _type in [
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "Bool",
        "String",
        "Decimal",
        "Enum",
        "Struct",
        "List",
        "Date",
        "Datetime",
        "Time",
        "Duration",
        "Float",
        "Integer",
    ]:
        setattr(dy, _type, DyDummyClass)


pytestmark = [
    with_instances(DATABASE_INSTANCES),
]


# ------------------------------------------------------------------------------------ #
#                                        SCHEMA                                        #
# ------------------------------------------------------------------------------------ #


class MyFirstColSpec(dy.Schema):
    a = dy.Integer(primary_key=True)
    b = dy.Integer()


class MySecondColSpec(dy.Schema):
    a = dy.Integer(primary_key=True)
    b = dy.Integer(min=1)


@dataclass
class MyCollection(dy.Collection):
    first: dy.LazyFrame[MyFirstColSpec]
    second: dy.LazyFrame[MySecondColSpec]

    @dy.filter()
    def equal_primary_keys(self) -> pl.LazyFrame:
        return self.first.join(self.second, on=self.common_primary_keys())

    @dy.filter()
    def first_b_greater_second_b(self) -> pl.LazyFrame:
        return self.first.join(
            self.second, on=self.common_primary_keys(), how="full", coalesce=True
        ).filter((pl.col("b") > pl.col("b_right")).fill_null(True))

    @classmethod
    def _init(cls, data: Mapping[str, FrameType], /):
        return cls(**{k: v.lazy() for k, v in data.items()})


@dataclass
class SimpleCollection(dy.Collection):
    first: dy.LazyFrame[MyFirstColSpec]
    second: dy.LazyFrame[MySecondColSpec]

    @classmethod
    def _init(cls, data: Mapping[str, FrameType], /):
        return cls(**{k: v.lazy() for k, v in data.items()})


# ------------------------------------------------------------------------------------ #
#                                         TESTS                                        #
# ------------------------------------------------------------------------------------ #


def data_without_filter_without_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    second = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    return first, second


def data_without_filter_with_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 1], "b": [1, 2, 3]})
    second = pl.LazyFrame({"a": [1, 2, 3], "b": [0, 1, 2]})
    return first, second


def data_with_filter_without_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 1, 3]})
    second = pl.LazyFrame({"a": [2, 3, 4, 5], "b": [1, 2, 3, 4]})
    return first, second


def data_with_filter_with_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    second = pl.LazyFrame({"a": [2, 3, 4, 5], "b": [0, 1, 2, 3]})
    return first, second


def test_dataclass():
    first, second = data_without_filter_without_rule_violation()
    c = SimpleCollection(first, second)
    assert_frame_equal(c.first, first)
    assert_frame_equal(c.second, second)


@materialize(nout=2)
def get_data(name: str):
    return globals()[f"data_{name}"]()


@materialize(nout=3, input_type=pl.LazyFrame)
def exec_filter_polars(c: dy.Collection):
    out, failure = c.filter(c.__dict__)
    return (
        out,
        SimpleCollection(**{name: f._df for name, f in failure.items()}),
        SimpleCollection(**{name: f.counts() for name, f in failure.items()}),
    )


# -------------------------------------- FILTER -------------------------------------- #


@pytest.mark.skipif(dy.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(dy is None, reason="dataframely needs to be installed")
def test_filter_without_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        first, second = data_without_filter_without_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert_frame_equal(out.first, first)
        assert_frame_equal(out.second, second)
        assert failure.first.select(pl.len()).collect().item() == 0
        assert failure.second.select(pl.len()).collect().item() == 0

    with Flow() as flow:
        with Stage("s01"):
            c = SimpleCollection(*get_data("without_filter_without_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(dy.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(dy is None, reason="dataframely needs to be installed")
def test_filter_without_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_without_filter_with_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert len(out.first.collect()) == 1
        assert len(out.second.collect()) == 2
        assert failure_counts.first == {"primary_key": 2}
        assert failure_counts.second == {"b|min": 1}

    with Flow() as flow:
        with Stage("s01"):
            c = SimpleCollection(*get_data("without_filter_with_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(dy.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(dy is None, reason="dataframely needs to be installed")
def test_filter_with_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_with_filter_without_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(out.first, pl.LazyFrame({"a": [3], "b": [3]}))
        assert_frame_equal(out.second, pl.LazyFrame({"a": [3], "b": [2]}))
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
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(dy.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(dy is None, reason="dataframely needs to be installed")
def test_filter_with_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_with_filter_with_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(out.first, pl.LazyFrame({"a": [3], "b": [3]}))
        assert_frame_equal(out.second, pl.LazyFrame({"a": [3], "b": [1]}))
        assert failure_counts.first == {"equal_primary_keys": 2}
        assert failure_counts.second == {"b|min": 1, "equal_primary_keys": 2}

    with Flow() as flow:
        with Stage("s01"):
            c = MyCollection(*get_data("with_filter_with_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()
