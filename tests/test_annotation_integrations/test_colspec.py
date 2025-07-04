# Copyright (c) QuantCo and pydiverse contributors 2024-2025
# SPDX-License-Identifier: BSD-3-Clause
import functools
import operator
import types
from dataclasses import dataclass

import polars as pl
import pytest
import sqlalchemy as sa
import structlog
from polars.testing import assert_frame_equal

from pydiverse.pipedag import Flow, Stage, materialize, materialize_table
from pydiverse.pipedag.context.context import CacheValidationMode, ConfigContext
from pydiverse.pipedag.errors import HookCheckException
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

try:
    import dataframely as dy
except ImportError:
    dy = None


pytestmark = [
    pytest.mark.pdtransform,
    with_instances(tuple(set(DATABASE_INSTANCES) - {"ibm_db2"})),
]

# ------------------------------------------------------------------------------------ #
#                                        SCHEMA                                        #
# ------------------------------------------------------------------------------------ #


class MyFirstColSpec(cs.ColSpec):
    a = cs.Integer(primary_key=True)
    b = cs.Int16()  # TODO: replace with cs.Int16 once ColSpec 0.2.2 is on conda-forge
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
        return (self.first.b > self.second.b) | self.pk_is_null(self.first) | self.pk_is_null(self.second)


@dataclass
class SimpleCollection(cs.Collection):
    first: MyFirstColSpec
    second: MySecondColSpec


# ------------------------------------------------------------------------------------ #
#                                         TESTS                                        #
# ------------------------------------------------------------------------------------ #


def data_without_filter_without_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", None]}, name="first")
    second = pdt.Table({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", "x"]}, name="second")
    return first, second


def data_without_filter_with_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table({"a": [1, 2, 1], "b": [1, 2, 3], "c": ["x", "y", None]}, name="first")
    second = pdt.Table({"a": [1, 2, 3], "b": [0, 1, 2], "c": [None, "y", "x"]}, name="second")
    return first, second


def data_with_filter_without_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table({"a": [1, 2, 3], "b": [1, 1, 3], "c": ["x", "y", None]}, name="first")
    second = pdt.Table({"a": [2, 3, 4, 5], "b": [1, 2, 3, 4], "c": ["x", "y", "x", "x"]}, name="second")
    return first, second


def data_with_filter_with_rule_violation() -> tuple[pdt.Table, pdt.Table]:
    first = pdt.Table({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["x", "y", None]}, name="first")
    second = pdt.Table(
        {"a": [2, 3, 4, 5, 6], "b": [0, 1, 2, 3, -1], "c": [None, "y", "y", "x", "z"]},
        name="second",
    )
    return first, second


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(pdt.Table is None, reason="pydiverse.transform needs to be installed")
def test_dataclass():
    first, second = data_without_filter_without_rule_violation()
    c = SimpleCollection(first, second)
    assert_frame_equal(c.first >> pdt.export(pdt.Polars), first >> pdt.export(pdt.Polars))
    assert_frame_equal(c.second >> pdt.export(pdt.Polars), second >> pdt.export(pdt.Polars))


@materialize(nout=2)
def get_data(name: str):
    return globals()[f"data_{name}"]()


@materialize(nout=3, input_type=pdt.SqlAlchemy)
def exec_filter(c: cs.Collection):
    cfg = cs.config.Config.default

    def materialize_hook(tbl, table_prefix):
        return tbl >> materialize_table(table_prefix)

    store = ConfigContext.get().store.table_store
    cfg.dialect_name = store.engine.dialect.name
    cfg.materialize_hook = materialize_hook
    out, failure = c.filter(cfg=cfg, cast=True)
    return (
        out,
        SimpleCollection(
            **{name: f.invalid_rows >> pdt.alias(name + "_invalid_rows") for name, f in failure.__dict__.items()}
        ),
        SimpleCollection(**{name: f.counts() for name, f in failure.__dict__.items()}),
    )


# -------------------------------------- FILTER -------------------------------------- #


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(pdt.Table is None, reason="pydiverse.transform needs to be installed")
def test_filter_without_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        first, second = data_without_filter_without_rule_violation()

        assert isinstance(out, SimpleCollection)
        assert_frame_equal(
            out.first.sort(by="a"),
            (first >> pdt.export(pdt.Polars(lazy=True))).cast(dict(b=pl.Int16)),
        )
        assert_frame_equal(out.second.sort(by="a"), second >> pdt.export(pdt.Polars(lazy=True)))
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
@pytest.mark.skipif(pdt.Table is None, reason="pydiverse.transform needs to be installed")
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
@pytest.mark.skipif(pdt.Table is None, reason="pydiverse.transform needs to be installed")
def test_filter_with_filter_without_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_with_filter_without_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(
            out.first,
            pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(b=pl.Int16, c=pl.String)),
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
@pytest.mark.skipif(pdt.Table is None, reason="pydiverse.transform needs to be installed")
def test_filter_with_filter_with_rule_violation():
    @materialize(input_type=pl.LazyFrame)
    def assertions(out, failure, failure_counts: dict[str, int]):
        # first, second = data_with_filter_with_rule_violation()

        assert isinstance(out, MyCollection)
        assert_frame_equal(
            out.first,
            pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(b=pl.Int16, c=pl.String)),
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


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    dy is not None,
    reason="This test only works if dataframely is not installed since we test a fallback mechanism in polars hook",
)
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations(with_filter: bool, with_violation: bool, validate_get_data: bool):
    if validate_get_data:

        @materialize(nout=2)
        def get_anno_data(
            name: str,
        ) -> tuple[MyFirstColSpec, MySecondColSpec]:
            return globals()[f"data_{name}"]()
    else:

        @materialize(nout=2)
        def get_anno_data(name: str) -> tuple[pdt.Table, pdt.Table]:
            return globals()[f"data_{name}"]()

    @materialize(input_type=pdt.Polars)
    def consumer(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int16() if validate_get_data or dy else pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

        if not validate_get_data and dy is None:
            # colspec will not do casting without dataframely
            # In case of validate_get_data, the type should already be
            # smallint in the database.
            first = first >> pdt.mutate(b=first.b.cast(pdt.Int16()))
        if not validate_get_data and with_violation:
            if with_filter:
                # this case does not occur for polars versions since they abort in cast
                MyFirstColSpec.validate(first)
            else:
                with pytest.raises(cs.exc.RuleValidationError, match="1 rules failed validation"):
                    MyFirstColSpec.validate(first)
            with pytest.raises(cs.exc.RuleValidationError, match="2 rules failed validation"):
                MySecondColSpec.validate(second)
        else:
            if not validate_get_data and dy is None:
                # colspec will not do casting without dataframely
                # In case of validate_get_data, the type should already be
                # smallint in the database.
                first = first >> pdt.mutate(b=first.b.cast(pdt.Int16()))
            assert MyFirstColSpec.is_valid(first)
            assert MySecondColSpec.is_valid(second)

    @materialize(input_type=pdt.Polars)
    def consumer2(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int16() if validate_get_data or dy else pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

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
            match="failed validation with MyFirstColSpec; Failure counts: {'b|min': 1, 'c|nullability': 1};"
            if with_filter
            else "{'_primary_key_': 2};",
        ):
            flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    elif with_violation and not validate_get_data and with_filter:
        # # This only raises with polars operations since no cast is implemented without
        # # dataframely
        # with pytest.raises(
        #     RuntimeError,
        #     match="Failed to retrieve table '<Table 'get_anno_data",
        # ):
        ret = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
        assert ret.successful
    else:
        ret = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
        assert ret.successful


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    dy is not None,
    reason="This test only works if dataframely is not installed since we test a fallback mechanism in polars hook",
)
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations_not_fail_fast(with_filter: bool, with_violation: bool, validate_get_data: bool):
    if validate_get_data:

        @materialize(nout=2)
        def get_anno_data(
            name: str,
        ) -> tuple[MyFirstColSpec, MySecondColSpec]:
            return globals()[f"data_{name}"]()
    else:

        @materialize(nout=2)
        def get_anno_data(name: str) -> tuple[pdt.Table, pdt.Table]:
            return globals()[f"data_{name}"]()

    @materialize(input_type=pdt.Polars)
    def consumer(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int16() if validate_get_data or dy else pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

        if not validate_get_data and dy is None:
            # colspec will not do casting without dataframely
            # In case of validate_get_data, the type should already be
            # smallint in the database.
            first = first >> pdt.mutate(b=first.b.cast(pdt.Int16()))
        assert MyFirstColSpec.is_valid(first)
        assert MySecondColSpec.is_valid(second)

    @materialize(input_type=pdt.Polars)
    def consumer2(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int16() if validate_get_data or dy else pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

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


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    dy is not None,
    reason="This test only works if dataframely is not installed since we test a fallback mechanism in polars hook",
)
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations_fault_tolerant(with_filter: bool, with_violation: bool, validate_get_data: bool):
    if validate_get_data:

        @materialize(nout=2)
        def get_anno_data(
            name: str,
        ) -> tuple[MyFirstColSpec, MySecondColSpec]:
            return globals()[f"data_{name}"]()
    else:

        @materialize(nout=2)
        def get_anno_data(name: str) -> tuple[pdt.Table, pdt.Table]:
            return globals()[f"data_{name}"]()

    @materialize(input_type=pdt.Polars)
    def consumer(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            (
                "b",
                pdc.Int16() if (validate_get_data and (not with_violation or with_filter)) or dy else pdc.Int64(),
            ),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

        if (not validate_get_data or (with_violation and not with_filter)) and dy is None:
            # colspec will not do casting without dataframely
            # In case of validate_get_data, the type should already be
            # smallint in the database.
            first = first >> pdt.mutate(b=first.b.cast(pdt.Int16()))
        if with_violation:
            if with_filter:
                MyFirstColSpec.validate(first)
                with pytest.raises(cs.exc.RuleValidationError, match="2 rules failed validation"):
                    MySecondColSpec.validate(second, cast=True)
            else:
                with pytest.raises(cs.exc.RuleValidationError, match="1 rules failed validation"):
                    MyFirstColSpec.validate(first)
                with pytest.raises(cs.exc.RuleValidationError, match="2 rules failed validation"):
                    MySecondColSpec.validate(second)
        else:
            assert MyFirstColSpec.is_valid(first)
            assert MySecondColSpec.is_valid(second)

    @materialize(input_type=pdt.Polars)
    def consumer2(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            (
                "b",
                pdc.Int16() if (validate_get_data and (not with_violation or with_filter)) or dy else pdc.Int64(),
            ),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

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


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    dy is not None,
    reason="This test only works if dataframely is not installed since we test a fallback mechanism in polars hook",
)
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

    @materialize(input_type=pdt.Polars)
    def consumer_collection(coll: CollectionType):
        # # collections are currently not passed on as annotations to individual tables
        # # thus no cast is happening
        assert [(c.name, c.dtype()) for c in coll.first] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in coll.second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(coll.first >> pdt.export(pdt.Polars())) == 3
        assert len(coll.second >> pdt.export(pdt.Polars())) in [3, 4, 5]

        if with_violation:
            with pytest.raises(cs.exc.MemberValidationError, match="2 members failed validation"):
                coll.validate(cast=True)
        else:
            if with_filter:
                # it is not really without violation
                out, _ = coll.filter(cast=True)
                assert_frame_equal(
                    out.first >> pdt.export(pdt.Polars(lazy=True)),
                    pl.LazyFrame({"a": [3], "b": [3], "c": [None]}).cast(dict(b=pl.Int16, c=pl.String)),
                )
                assert_frame_equal(
                    out.second >> pdt.export(pdt.Polars(lazy=True)),
                    pl.LazyFrame({"a": [3], "b": [2], "c": ["y"]}),
                )
            else:
                assert coll.is_valid(cast=True)

    @materialize(input_type=pdt.Polars)
    def consumer2_collection(coll: CollectionType):
        # # collections are currently not passed on as annotations to individual tables
        # # thus no cast is happening
        assert [(c.name, c.dtype()) for c in coll.first] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in coll.second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(coll.first >> pdt.export(pdt.Polars())) == 3
        assert len(coll.second >> pdt.export(pdt.Polars())) in [3, 4, 5]

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


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.skipif(
    dy is not None,
    reason="This test only works if dataframely is not installed since we test a fallback mechanism in polars hook",
)
def test_type_mapping():
    @materialize(nout=2)
    def get_anno_data() -> tuple[MyFirstColSpec, MySecondColSpec]:
        return data_with_filter_without_rule_violation()

    @materialize(input_type=sa.Table)
    def consumer(first: MyFirstColSpec, second: MySecondColSpec):
        assert isinstance(first.c.b.type, sa.SmallInteger)
        assert isinstance(second.c.b.type, sa.BigInteger)
        assert not isinstance(second.c.b.type, sa.SmallInteger)

    with Flow() as flow:
        with Stage("s01"):
            first, second = get_anno_data()
            consumer(first, second)

    flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)


@pytest.mark.skipif(cs.Collection is object, reason="ColSpec needs to be installed")
@pytest.mark.parametrize(
    "with_filter, with_violation, validate_get_data",
    [(a, b, c) for a in [False, True] for b in [False, True] for c in [False, True]],
)
def test_annotations_sql(with_filter: bool, with_violation: bool, validate_get_data: bool):
    @materialize(nout=2)
    def load_raw_data(name: str):
        return globals()[f"data_{name}"]()

    if validate_get_data:

        @materialize(nout=2, input_type=pdt.SqlAlchemy)
        def get_anno_data_sql(first: MyFirstColSpec, second: MySecondColSpec) -> tuple[MyFirstColSpec, MySecondColSpec]:
            return first >> pdt.alias("first2"), second >> pdt.alias("second2")
    else:

        @materialize(nout=2, input_type=pdt.SqlAlchemy)
        def get_anno_data_sql(first: pdt.Table, second: pdt.Table) -> tuple[pdt.Table, pdt.Table]:
            return first >> pdt.alias("first2"), second >> pdt.alias("second2")

    def get_anno_data(name: str):
        first, second = load_raw_data(name)
        return get_anno_data_sql(first, second)

    @materialize(input_type=pdt.SqlAlchemy)
    def consumer(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            (
                "b",
                pdc.Int16() if validate_get_data and (not with_violation or with_filter) else pdc.Int64(),
            ),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

        # Conversion from sqlalchemy backend to polars backend always uses 64bit integer
        # intentionally
        first = first >> pdt.collect(pdt.Polars()) >> pdt.mutate(b=first.b.cast(pdt.Int16()))
        if not validate_get_data and with_violation:
            if with_filter:
                # this case does not occur for polars versions since they abort in cast
                MyFirstColSpec.validate(first)
            else:
                with pytest.raises(cs.exc.RuleValidationError, match="1 rules failed validation"):
                    MyFirstColSpec.validate(first)
            with pytest.raises(cs.exc.RuleValidationError, match="2 rules failed validation"):
                MySecondColSpec.validate(second)
        else:
            assert MyFirstColSpec.is_valid(first >> pdt.collect(pdt.Polars()))
            assert MySecondColSpec.is_valid(second >> pdt.collect(pdt.Polars()))

    @materialize(input_type=pdt.SqlAlchemy)
    def consumer2(first: MyFirstColSpec, second: MySecondColSpec):
        assert [(c.name, c.dtype()) for c in first] == [
            ("a", pdc.Int64()),
            (
                "b",
                pdc.Int16() if validate_get_data and (not with_violation or with_filter) else pdc.Int64(),
            ),
            ("c", pdc.String()),
        ]
        assert [(c.name, c.dtype()) for c in second] == [
            ("a", pdc.Int64()),
            ("b", pdc.Int64()),
            ("c", pdc.String()),
        ]
        assert len(first >> pdt.export(pdt.Polars())) == 3
        assert len(second >> pdt.export(pdt.Polars())) in [3, 4, 5]

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
            match="failed validation with MyFirstColSpec; Failure counts: {'b|min': 1, 'c|nullability': 1};"
            if with_filter
            else "{'_primary_key_': 2};",
        ):
            flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
    elif with_violation and not validate_get_data and with_filter:
        # # This only raises with polars operations since no cast is implemented without
        # # dataframely
        # with pytest.raises(
        #     RuntimeError,
        #     match="Failed to retrieve table '<Table 'get_anno_data",
        # ):
        ret = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
        assert ret.successful
    else:
        ret = flow.run(cache_validation_mode=CacheValidationMode.FORCE_CACHE_INVALID)
        assert ret.successful
