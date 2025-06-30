# Copyright (c) QuantCo and pydiverse contributors 2024-2025
# SPDX-License-Identifier: BSD-3-Clause
import types
from dataclasses import dataclass
from typing import Generic, Mapping, TypeVar

import polars as pl
import pytest
import sqlalchemy as sa
import structlog
from polars.testing import assert_frame_equal

from pydiverse.pipedag import ConfigContext, Flow, Stage, materialize
from pydiverse.pipedag.context.context import CacheValidationMode
from pydiverse.pipedag.errors import HookCheckException
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances

try:
    import dataframely as dy
    from dataframely._polars import FrameType
except ImportError:
    T = TypeVar("T")

    class DyDataFrame(Generic[T]):
        pass

    class DyDummyClass:
        def __init__(self, *args, **kwargs):
            pass

    FrameType = None
    dy = types.ModuleType("dataframely")
    dy.DataFrame = DyDataFrame
    dy.LazyFrame = DyDataFrame
    dy.FailureInfo = None
    dy.Column = None
    dy.Collection = object
    dy.Schema = object
    dy.filter = lambda: lambda fn: fn  # noqa
    dy.rule = lambda: lambda fn: fn  # noqa
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
    b = dy.Int16()
    c = dy.Enum(["x", "y"], nullable=True)


class MySecondColSpec(dy.Schema):
    a = dy.Integer(primary_key=True)
    b = dy.Integer(min=1)
    c = dy.Enum(["x", "y"], nullable=False)


@dataclass
class MyCollection(dy.Collection):
    first: dy.LazyFrame[MyFirstColSpec]
    second: dy.LazyFrame[MySecondColSpec]

    @dy.filter()
    def equal_primary_keys(self) -> pl.LazyFrame:
        return self.first.join(self.second, on=self.common_primary_keys())

    @dy.filter()
    def first_b_greater_second_b(self) -> pl.LazyFrame:
        return self.first.join(self.second, on=self.common_primary_keys(), how="full", coalesce=True).filter(
            (pl.col("b") > pl.col("b_right")).fill_null(True)
        )

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


enum = pl.Enum(["x", "y"])


def data_without_filter_without_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", None]}).cast(dict(c=enum))
    second = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", "x"]}).cast(dict(c=enum))
    return first, second


def data_without_filter_with_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 1], "b": [1, 2, 3], "c": ["x", "y", None]}).cast(dict(c=enum))
    second = pl.LazyFrame({"a": [1, 2, 3], "b": [0, 1, 2], "c": [None, "y", "x"]}).cast(dict(c=enum))
    return first, second


def data_with_filter_without_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 1, 3], "c": ["x", "y", None]}).cast(dict(c=enum))
    second = pl.LazyFrame({"a": [2, 3, 4, 5], "b": [1, 2, 3, 4], "c": ["x", "y", "x", "x"]}).cast(dict(c=enum))
    return first, second


def data_with_filter_with_rule_violation() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    first = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", None]}).cast(dict(c=enum))
    second = pl.LazyFrame({"a": [2, 3, 4, 5, 6], "b": [0, 1, 2, 3, -1], "c": [None, "y", "y", "x", "z"]})
    return first, second


def test_dataclass():
    first, second = data_without_filter_without_rule_violation()
    c = SimpleCollection(first, second)
    assert_frame_equal(c.first, first)
    assert_frame_equal(c.second, second)


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
def test_enum_violation():
    second = pl.LazyFrame({"a": [2, 3, 4, 5], "b": [0, 1, 2, 3], "c": ["z", "y", "y", "x"]})
    # it is expected that cast fails on invalid enum value
    with pytest.raises(
        pl.exceptions.InvalidOperationError,
        match="conversion from `str` to `enum` failed in column 'c' for 1 out of 4 values",
    ):
        MySecondColSpec.cast(second).collect()
    x, y = MySecondColSpec.filter(second, cast=True)
    assert len(x) == 3
    assert len(y.invalid()) == 1

    class DummyCollection(dy.Collection):
        second: dy.LazyFrame[MySecondColSpec]

    x, y = DummyCollection.filter(dict(second=second), cast=True)
    x = x.second.collect()
    y = y["second"]
    assert len(x) == 3
    assert len(y.invalid()) == 1


@materialize(nout=2)
def get_data(name: str):
    return globals()[f"data_{name}"]()


@materialize(nout=3, input_type=pl.LazyFrame)
def exec_filter_polars(c: dy.Collection):
    out, failure = c.filter(c.__dict__, cast=True)
    return (
        out,
        SimpleCollection(**{name: f._df for name, f in failure.items()}),
        SimpleCollection(**{name: f.counts() for name, f in failure.items()}),
    )


# -------------------------------------- FILTER -------------------------------------- #


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
def test_filter_without_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        out = out.cast(out.__dict__)
        first, second = data_without_filter_without_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert_frame_equal(out.first, first.cast(dict(b=pl.Int16)))
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


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
def test_filter_without_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        out = out.cast(out.__dict__)
        # first, second = data_without_filter_with_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert len(out.first.collect()) == 1
        assert len(out.second.collect()) == 2
        assert failure_counts.first == {"primary_key": 2}
        assert failure_counts.second == {"b|min": 1, "c|nullability": 1}

    with Flow() as flow:
        with Stage("s01"):
            c = SimpleCollection(*get_data("without_filter_with_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
def test_filter_with_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        out = out.cast(out.__dict__)
        # first, second = data_with_filter_without_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(
            out.first,
            pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(b=pl.Int16, c=enum)),
        )
        assert_frame_equal(
            out.second,
            pl.LazyFrame({"a": [3], "b": [2], "c": ["y"]}).cast(dict(c=enum)),
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
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
def test_filter_with_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        out = out.cast(out.__dict__)
        # first, second = data_with_filter_with_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(
            out.first,
            pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(b=pl.Int16, c=enum)),
        )
        assert_frame_equal(
            out.second,
            pl.LazyFrame({"a": [3], "b": [1], "c": ["y"]}).cast(dict(c=enum)),
        )
        assert failure_counts.first == {"equal_primary_keys": 2}
        assert failure_counts.second == {
            "b|min": 1,
            "c|nullability": 1,
            "c|dtype": 1,
            "equal_primary_keys": 2,
        }

    with Flow() as flow:
        with Stage("s01"):
            c = MyCollection(*get_data("with_filter_with_rule_violation"))
        with Stage("s02"):
            out, failure, failure_counts = exec_filter_polars(c)
            assertions(out, failure, failure_counts)

    flow.run()


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations(with_filter: bool, with_violation: bool, validate_get_data: bool):
    if validate_get_data:

        @materialize(nout=2)
        def get_anno_data(
            name: str,
        ) -> tuple[dy.LazyFrame[MyFirstColSpec], dy.LazyFrame[MySecondColSpec]]:
            return globals()[f"data_{name}"]()
    else:

        @materialize(nout=2)
        def get_anno_data(name: str) -> tuple[pl.LazyFrame, pl.LazyFrame]:
            return globals()[f"data_{name}"]()

    @materialize(input_type=pl.LazyFrame)
    def consumer(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert first.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int16), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert second.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int64), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert len(first.collect()) == 3
        assert len(second.collect()) in [3, 4, 5]

        if not validate_get_data and with_violation:
            with pytest.raises(dy.exc.RuleValidationError, match="1 rules failed validation"):
                MyFirstColSpec.validate(first)
            with pytest.raises(dy.exc.RuleValidationError, match="2 rules failed validation"):
                MySecondColSpec.validate(second)
        else:
            assert MyFirstColSpec.is_valid(first)
            assert MySecondColSpec.is_valid(second)

    @materialize(input_type=dy.LazyFrame)
    def consumer2(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert first.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int16), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert second.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int64), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert len(first.collect()) == 3
        assert len(second.collect()) in [3, 4, 5]

    with Flow() as flow:
        name = f"with{'out' if not with_filter else ''}_filter_with{'out' if not with_violation else ''}_rule_violation"
        with Stage("s01"):
            first, second = get_anno_data(name)
            consumer(first, second)
        with Stage("s02"):
            consumer2(first, second)

    if with_violation and validate_get_data:
        # Validation at end of get_anno_data task fails
        with pytest.raises(
            HookCheckException,
            match="failed validation with MyFirstColSpec; Failure counts: "
            "{'b|min': 1, 'c|nullability': 1, 'c|dtype': 1};"
            if with_filter
            else "{'primary_key': 2};",
        ):
            flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    elif with_violation and not validate_get_data and with_filter:
        # Due to the enum failure the dematerialization hook for consumer
        # task fails with ValueError (triggers RuntimeError in Flow)
        with pytest.raises(
            RuntimeError,
            match="Failed to retrieve table '<Table 'get_anno_data",
        ):
            flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    else:
        ret = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
        assert ret.successful


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations_not_fail_fast(with_filter: bool, with_violation: bool, validate_get_data: bool):
    if validate_get_data:

        @materialize(nout=2)
        def get_anno_data(
            name: str,
        ) -> tuple[dy.LazyFrame[MyFirstColSpec], dy.LazyFrame[MySecondColSpec]]:
            return globals()[f"data_{name}"]()
    else:

        @materialize(nout=2)
        def get_anno_data(name: str) -> tuple[pl.LazyFrame, pl.LazyFrame]:
            return globals()[f"data_{name}"]()

    @materialize(input_type=pl.LazyFrame)
    def consumer(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert first.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int16), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert second.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int64), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert len(first.collect()) == 3
        assert len(second.collect()) in [3, 4, 5]

        assert MyFirstColSpec.is_valid(first)
        assert MySecondColSpec.is_valid(second)

    @materialize(input_type=dy.LazyFrame)
    def consumer2(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert first.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int16), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert second.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int64), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert len(first.collect()) == 3
        assert len(second.collect()) in [3, 4, 5]

    with Flow() as flow:
        name = f"with{'out' if not with_filter else ''}_filter_with{'out' if not with_violation else ''}_rule_violation"
        with Stage("s01"):
            first, second = get_anno_data(name)
            consumer(first, second)
        with Stage("s02"):
            consumer2(first, second)

    with ConfigContext.get().evolve(fail_fast=False):
        result = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    if with_violation:
        assert not result.successful
    else:
        assert result.successful


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations_fault_tolerant(with_filter: bool, with_violation: bool, validate_get_data: bool):
    if validate_get_data:

        @materialize(nout=2)
        def get_anno_data(
            name: str,
        ) -> tuple[dy.LazyFrame[MyFirstColSpec], dy.LazyFrame[MySecondColSpec]]:
            return globals()[f"data_{name}"]()
    else:

        @materialize(nout=2)
        def get_anno_data(name: str) -> tuple[pl.LazyFrame, pl.LazyFrame]:
            return globals()[f"data_{name}"]()

    @materialize(input_type=pl.LazyFrame)
    def consumer(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert first.collect_schema() == pl.Schema(
            [("a", pl.Int64), ("b", pl.Int16), ("c", pl.Enum(categories=["x", "y"]))]
        )
        assert second.collect_schema() == pl.Schema(
            [
                ("a", pl.Int64),
                ("b", pl.Int64),
                (
                    "c",
                    pl.Enum(categories=["x", "y"]) if not with_filter or not with_violation else pl.String,
                ),
            ]
        )
        assert len(first.collect()) == 3
        assert len(second.collect()) in [3, 4, 5]

        if with_violation:
            if with_filter:
                MyFirstColSpec.validate(first)
                with pytest.raises(dy.exc.RuleValidationError, match="3 rules failed validation"):
                    MySecondColSpec.validate(second, cast=True)
            else:
                with pytest.raises(dy.exc.RuleValidationError, match="1 rules failed validation"):
                    MyFirstColSpec.validate(first)
                with pytest.raises(dy.exc.RuleValidationError, match="2 rules failed validation"):
                    MySecondColSpec.validate(second)
        else:
            assert MyFirstColSpec.is_valid(first)
            assert MySecondColSpec.is_valid(second)

    @materialize(input_type=dy.LazyFrame)
    def consumer2(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert len(first.collect()) == 3
        assert len(second.collect()) in [3, 4, 5]

    with Flow() as flow:
        name = f"with{'out' if not with_filter else ''}_filter_with{'out' if not with_violation else ''}_rule_violation"
        with Stage("s01"):
            first, second = get_anno_data(name)
            consumer(first, second)
        with Stage("s02"):
            consumer2(first, second)

    with structlog.testing.capture_logs() as logs:
        with ConfigContext.get().evolve(table_hook_args=dict(polars=dict(fault_tolerant_annotation_action=True))):
            result = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    assert result.successful
    failures = [c for c in logs if c["event"] == "Failed to apply materialize annotation for table"]
    if with_violation and validate_get_data:
        assert len(failures) == 1 if with_filter else 2
        assert all("failed validation with My" in failure["exception"] for failure in failures)


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_collections(with_filter: bool, with_violation: bool, validate_get_data: bool):
    CollectionType = MyCollection if with_filter else SimpleCollection

    if validate_get_data:

        @materialize()
        def get_anno_collection(name: str) -> CollectionType:
            first, second = globals()[f"data_{name}"]()
            return CollectionType(first=first, second=second)
    else:

        @materialize()
        def get_anno_collection(name: str):
            first, second = globals()[f"data_{name}"]()
            return CollectionType(first=first, second=second)

    @materialize(input_type=pl.LazyFrame)
    def consumer_collection(coll: CollectionType):
        # # collections are currently not passed on as annotations to individual tables
        # # thus no cast is happening
        # assert coll.first.collect_schema() == pl.Schema([('a', pl.Int64),
        # ('b', pl.Int64), ('c', pl.Enum(categories=['x', 'y']))])
        # assert coll.second.collect_schema() == pl.Schema([('a', pl.Int64),
        # ('b', pl.Int64), ('c', pl.Enum(categories=['x', 'y']))])
        assert len(coll.first.collect()) == 3
        assert len(coll.second.collect()) in [3, 4, 5]

        if with_violation:
            with pytest.raises(dy.exc.MemberValidationError, match="2 members failed validation"):
                coll.validate(coll.__dict__, cast=True)
        else:
            if with_filter:
                # it is not really without violation
                out, _ = coll.filter(coll.__dict__, cast=True)
                assert_frame_equal(
                    out.first,
                    pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(b=pl.Int16, c=enum)),
                )
                assert_frame_equal(
                    out.second,
                    pl.LazyFrame({"a": [3], "b": [2], "c": ["y"]}).cast(dict(c=enum)),
                )
            else:
                assert coll.is_valid(coll.__dict__, cast=True)

    @materialize(input_type=dy.LazyFrame)
    def consumer2_collection(coll: CollectionType):
        # # collections are currently not passed on as annotations to individual tables
        # # thus no cast is happening
        # assert coll.first.collect_schema() == pl.Schema([('a', pl.Int64),
        # ('b', pl.Int64), ('c', pl.Enum(categories=['x', 'y']))])
        # assert coll.second.collect_schema() == pl.Schema([('a', pl.Int64),
        # ('b', pl.Int64), ('c', pl.Enum(categories=['x', 'y']))])
        assert len(coll.first.collect()) == 3
        assert len(coll.second.collect()) in [3, 4, 5]

    with Flow() as flow:
        name = f"with{'out' if not with_filter else ''}_filter_with{'out' if not with_violation else ''}_rule_violation"
        with Stage("s01"):
            collection = get_anno_collection(name)
            consumer_collection(collection)
        with Stage("s02"):
            consumer2_collection(collection)

    # # collections are currently not passed on as annotations to individual tables
    # # thus no cast is happening
    # if with_violation:
    #     from dataframely.exc import RuleValidationError
    #     with pytest.raises(RuleValidationError):
    #         flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    # else:
    ret = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    assert ret.successful


@pytest.mark.skipif(dy.Collection is object, reason="dataframely needs to be installed")
def test_type_mapping():
    @materialize(nout=2)
    def get_anno_data() -> tuple[dy.LazyFrame[MyFirstColSpec], dy.LazyFrame[MySecondColSpec]]:
        return data_with_filter_without_rule_violation()

    @materialize(input_type=sa.Table)
    def consumer(first: dy.LazyFrame[MyFirstColSpec], second: dy.LazyFrame[MySecondColSpec]):
        assert isinstance(first.c.b.type, sa.SmallInteger)
        assert isinstance(second.c.b.type, sa.BigInteger)
        assert not isinstance(second.c.b.type, sa.SmallInteger)

    with Flow() as flow:
        with Stage("s01"):
            first, second = get_anno_data()
            consumer(first, second)

    flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
