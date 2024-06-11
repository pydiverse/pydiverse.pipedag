from __future__ import annotations

import datetime as dt
import string
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy as sa
from packaging.version import Version

import tests.util.tasks_library as m
from pydiverse.pipedag import *
from pydiverse.pipedag.backend.table.sql.hooks import PandasTableHook
from pydiverse.pipedag.backend.table.sql.sql import DISABLE_DIALECT_REGISTRATION
from pydiverse.pipedag.backend.table.util import DType

# Parameterize all tests in this file with several instance_id configurations
from tests.fixtures.instances import DATABASE_INSTANCES, with_instances
from tests.util.spy import spy_task
from tests.util.sql import get_config_with_table_store

# disable duckdb for now, since they have a bug in version 0.9.2 that needs fixing
pytestmark = [with_instances(tuple(set(DATABASE_INSTANCES) - {"duckdb"}))]
pd_version = Version(pd.__version__)


class Values:
    INT8 = [-128, 127]
    INT16 = [-32768, 32767]
    INT32 = [-2147483648, 2147483647]
    INT64 = [-9223372036854775808, 9223372036854775807]

    FLOAT32 = [3e38, 3.14159]
    FLOAT64 = [1e300, 3.14159]

    STR = ["", string.printable + string.whitespace]

    BOOLEAN = [True, False]

    DATE = [dt.date(1, 1, 1), dt.date(9999, 12, 31)]
    DATE_NS = [dt.date(1700, 1, 1), dt.date(2200, 1, 1)]
    TIME = [dt.time(0, 0, 0), dt.time(23, 59, 59)]
    DATETIME = [dt.datetime(1, 1, 1, 0, 0, 0), dt.datetime(9999, 12, 31, 23, 59, 59)]
    DATETIME_NS = [dt.datetime(1700, 1, 1), dt.datetime(2200, 1, 1)]


class NoneValues:
    INT = [0, None]
    FLOAT = [0.0, None]
    STR = ["", None]
    BOOLEAN = [False, None]

    DATE = [dt.date(1970, 1, 1), None]
    TIME = [dt.time(0, 0, 0), None]
    DATETIME = [dt.datetime(1970, 1, 1, 0, 0, 0), None]


def get_dialect_name():
    return ConfigContext.get().store.table_store.engine.dialect.name


class TestPandasTableHookNumpy:
    def test_basic(self):
        df = pd.DataFrame(
            {
                "int8": pd.array(Values.INT8, dtype="Int8"),
                "int16": pd.array(Values.INT16, dtype="Int16"),
                "int32": pd.array(Values.INT32, dtype="Int32"),
                "int64": pd.array(Values.INT64, dtype="Int64"),
                "float32": pd.array(Values.FLOAT32, dtype="Float32"),
                "float64": pd.array(Values.FLOAT32, dtype="Float64"),
                "str": pd.array(Values.STR, dtype="string"),
                "boolean": pd.array(Values.BOOLEAN, dtype="boolean"),
            }
        )

        @materialize()
        def numpy_input():
            return df

        @materialize(input_type=(pd.DataFrame, "numpy"))
        def assert_expected(in_df):
            pd.testing.assert_frame_equal(in_df, df, check_dtype=False)

            allowed_dtypes = set(df.dtypes)
            for dtype in in_df.dtypes:
                assert dtype in allowed_dtypes

        with Flow() as f:
            with Stage("stage"):
                t = numpy_input()
                assert_expected(t)

        assert f.run().successful

    def test_none(self):
        df = pd.DataFrame(
            {
                "int": pd.array(NoneValues.INT, dtype="Int64"),
                "float": pd.array(NoneValues.FLOAT, dtype="Float64"),
                "str": pd.array(NoneValues.STR, dtype="string"),
                "boolean": pd.array(NoneValues.BOOLEAN, dtype="boolean"),
            }
        )

        @materialize()
        def numpy_input():
            return df

        @materialize(input_type=(pd.DataFrame, "numpy"))
        def assert_expected(in_df):
            pd.testing.assert_frame_equal(in_df, df, check_dtype=False)

            allowed_dtypes = set(df.dtypes)
            for col, dtype in in_df.dtypes.items():
                if col == "boolean" and get_dialect_name() == "ibm_db_sa":
                    assert dtype == pd.Int16Dtype()
                else:
                    assert dtype in allowed_dtypes

        with Flow() as f:
            with Stage("stage"):
                t = numpy_input()
                assert_expected(t)

        assert f.run().successful

    def test_datetime(self):
        df = pd.DataFrame(
            {
                "date": Values.DATE,
                "date_ns": Values.DATE_NS,
                "date_none": NoneValues.DATE,
                "datetime": Values.DATETIME,
                "datetime_ns": Values.DATETIME_NS,
                "datetime_none": NoneValues.DATETIME,
            }
        ).astype(object)

        df_expected = pd.DataFrame(
            {
                "date": pd.array(Values.DATE_NS, dtype="datetime64[ns]"),
                "date_ns": pd.array(Values.DATE_NS, dtype="datetime64[ns]"),
                "date_none": pd.array(NoneValues.DATE, dtype="datetime64[ns]"),
                "datetime": pd.array(Values.DATETIME_NS, dtype="datetime64[ns]"),
                "datetime_ns": pd.array(Values.DATETIME_NS, dtype="datetime64[ns]"),
                "datetime_none": pd.array(NoneValues.DATETIME, dtype="datetime64[ns]"),
                "date_year": [d.year for d in Values.DATE],
                "date_ns_year": [d.year for d in Values.DATE_NS],
                "date_none_year": [1970, None],
                "datetime_year": [d.year for d in Values.DATETIME],
                "datetime_ns_year": [d.year for d in Values.DATETIME_NS],
                "datetime_none_year": [1970, None],
            }
        )

        @materialize()
        def numpy_input():
            datetime_dtype = (
                sa.DateTime()
                if (get_dialect_name() != "mssql")
                else sa.dialects.mssql.DATETIME2()
            )

            return Table(
                df,
                type_map={
                    "date": sa.Date(),
                    "date_ns": sa.Date(),
                    "date_none": sa.Date(),
                    "datetime": datetime_dtype,
                    "datetime_ns": datetime_dtype,
                    "datetime_none": datetime_dtype,
                },
            )

        @materialize(input_type=(pd.DataFrame, "numpy"))
        def assert_expected(in_df):
            pd.testing.assert_frame_equal(in_df, df_expected, check_dtype=False)

        with Flow() as f:
            with Stage("stage"):
                t = numpy_input()
                assert_expected(t)

        assert f.run().successful


@pytest.mark.skipif(pd_version < Version("2.0"), reason="Requires pandas v2.0")
class TestPandasTableHookArrow:
    def test_basic(self):
        df = pd.DataFrame(
            {
                "int8": pd.array(Values.INT8, dtype="int8[pyarrow]"),
                "int16": pd.array(Values.INT16, dtype="int16[pyarrow]"),
                "int32": pd.array(Values.INT32, dtype="int32[pyarrow]"),
                "int64": pd.array(Values.INT64, dtype="int64[pyarrow]"),
                "float32": pd.array(Values.FLOAT32, dtype="float[pyarrow]"),
                "float64": pd.array(Values.FLOAT32, dtype="double[pyarrow]"),
                "str": pd.array(Values.STR, dtype=pd.ArrowDtype(pa.string())),
                "boolean": pd.array(Values.BOOLEAN, dtype="bool[pyarrow]"),
                "date": pd.array(Values.DATE, dtype=pd.ArrowDtype(pa.date32())),
                "time": pd.array(Values.TIME, dtype=pd.ArrowDtype(pa.time64("us"))),
                "datetime": pd.array(
                    Values.DATETIME, dtype=pd.ArrowDtype(pa.timestamp("us"))
                ),
            }
        )

        @materialize()
        def arrow_input():
            return df

        @materialize(input_type=(pd.DataFrame, "arrow"))
        def assert_expected(in_df):
            pd.testing.assert_frame_equal(in_df, df, check_dtype=False)
            allowed_dtypes = set(df.dtypes)
            # Prefer StringDtype("pyarrow") over ArrowDtype(pa.string()) for now.
            # We need to check this choice with future versions of pandas/pyarrow.
            allowed_dtypes.remove(pd.ArrowDtype(pa.string()))
            allowed_dtypes.add(pd.StringDtype("pyarrow"))
            for col, dtype in in_df.dtypes.items():
                if col == "boolean" and get_dialect_name() == "ibm_db_sa":
                    assert dtype == pd.ArrowDtype(pa.int16())
                else:
                    assert dtype in allowed_dtypes

        with Flow() as f:
            with Stage("stage"):
                t = arrow_input()
                assert_expected(t)

        assert f.run().successful

    def test_none(self):
        df = pd.DataFrame(
            {
                "int": pd.array(NoneValues.INT, dtype="int64[pyarrow]"),
                "float": pd.array(NoneValues.FLOAT, dtype="double[pyarrow]"),
                "str": pd.array(NoneValues.STR, dtype=pd.ArrowDtype(pa.string())),
                "boolean": pd.array(NoneValues.BOOLEAN, dtype="bool[pyarrow]"),
                "date": pd.array(NoneValues.DATE, dtype=pd.ArrowDtype(pa.date32())),
                "time": pd.array(NoneValues.TIME, dtype=pd.ArrowDtype(pa.time64("us"))),
                "datetime": pd.array(
                    NoneValues.DATETIME, dtype=pd.ArrowDtype(pa.timestamp("us"))
                ),
            }
        )

        @materialize()
        def arrow_input():
            return df

        @materialize(input_type=(pd.DataFrame, "arrow"))
        def assert_expected(in_df):
            pd.testing.assert_frame_equal(in_df, df, check_dtype=False)

        with Flow() as f:
            with Stage("stage"):
                t = arrow_input()
                assert_expected(t)

        assert f.run().successful


@pytest.mark.xfail(
    reason=(
        "The string '\\N' can't be materialized using the "
        "COPY FROM STDIN WITH CSV technique."
    ),
    strict=True,
)
@with_instances("postgres")
def test_pandas_table_hook_postgres_null_string():
    data = {"strNA": ["", None, "\\N"]}

    with Flow() as f:
        with Stage("stage_0"):
            t = m.pd_dataframe(data)

    with ConfigContext.get().evolve(swallow_exceptions=True), StageLockContext():
        result = f.run()
        df = result.get(t, as_type=pd.DataFrame)

    assert df["strNA"][0] == ""
    assert pd.isna(df["strNA"][1])
    assert (
        df["strNA"].fillna("X")[2] == "\\N"
    ), "This is a known issue that the string '\\N' is considered NA"


@with_instances("postgres", "local_table_store")
class TestPandasAutoVersion:
    def test_smoke(self, mocker):
        should_swap_inputs = False
        global_df = pd.DataFrame({"global_col": [1, 0, 1, 0]})

        @materialize(input_type=pd.DataFrame, version=AUTO_VERSION, nout=2)
        def in_tables():
            in_table_1 = pd.DataFrame({"col": [1, 2, 3, 4]})
            in_table_2 = pd.DataFrame({"col": [4, 3, 2, 1]})

            if should_swap_inputs:
                return Table(in_table_2), Table(in_table_1)
            else:
                return Table(in_table_1), Table(in_table_2)

        @materialize(input_type=pd.DataFrame, version=AUTO_VERSION)
        def noop(df):
            return df, Table(df)

        @materialize(input_type=pd.DataFrame, version=AUTO_VERSION)
        def global_df_task(df):
            df["col"] *= global_df["global_col"]
            return df

        with Flow() as f:
            with Stage("stage"):
                in_tables_ = in_tables()
                noop_ = noop(in_tables_[0])
                global_df_task(noop_[0])

        f.run()
        in_tables_spy = spy_task(mocker, in_tables_)

        f.run()
        in_tables_spy.assert_called_once()

        should_swap_inputs = True
        f.run()
        in_tables_spy.assert_called(2)


@with_instances("postgres")
class TestPandasCustomHook:
    def test_custom_upload(self):
        class TestTableStore(ConfigContext.get().store.table_store.__class__):
            _dialect_name = DISABLE_DIALECT_REGISTRATION
            # this subclass is just to make sure hooks of other tests are not affected
            pass

        class TestTableStore2(TestTableStore):
            # this tests that the delegation to parent hooks works
            pass

        cfg = get_config_with_table_store(ConfigContext.get(), TestTableStore2)

        @TestTableStore.register_table(pd, replace_hooks=[PandasTableHook])
        class CustomPandasUploadTableHook(PandasTableHook):
            @classmethod
            def upload_table(
                cls,
                df: pd.DataFrame,
                name: str,
                schema: str,
                dtypes: dict[str, DType],
                conn: sa.Connection,
                early: bool,
            ):
                df["custom_upload"] = True
                super().upload_table(df, name, schema, dtypes, conn, early)

        df = pd.DataFrame(
            {
                "int8": pd.array(Values.INT8, dtype="Int8"),
            }
        )

        @materialize()
        def numpy_input():
            return df

        @materialize(input_type=pd.DataFrame)
        def verify_cutom(t):
            assert "custom_upload" in t.columns

        with Flow() as f:
            with Stage("stage"):
                t = numpy_input()
                verify_cutom(t)

        assert f.run(config=cfg).successful

    def test_custom_download(self):
        class TestTableStore(ConfigContext.get().store.table_store.__class__):
            _dialect_name = DISABLE_DIALECT_REGISTRATION
            # this subclass is just to make sure hooks of other tests are not affected
            pass

        cfg = get_config_with_table_store(ConfigContext.get(), TestTableStore)

        @TestTableStore.register_table(pd, replace_hooks=[PandasTableHook])
        class CustomPandasDownloadTableHook(PandasTableHook):
            @classmethod
            def download_table(
                cls,
                query: Any,
                conn: sa.Connection,
                dtypes: dict[str, DType] | None = None,
            ):
                df = super().download_table(query, conn, dtypes)
                df["custom_download"] = True
                return df

        df = pd.DataFrame(
            {
                "int8": pd.array(Values.INT8, dtype="Int8"),
            }
        )

        @materialize()
        def numpy_input():
            return df

        @materialize(input_type=pd.DataFrame)
        def verify_cutom(t):
            assert "custom_download" in t.columns

        with Flow() as f:
            with Stage("stage"):
                t = numpy_input()
                verify_cutom(t)

        assert f.run(config=cfg).successful
