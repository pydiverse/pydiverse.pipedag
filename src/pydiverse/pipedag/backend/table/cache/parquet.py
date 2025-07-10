# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import json
import os
import shutil
import types
from typing import Any

import pandas as pd
import sqlalchemy as sa
from packaging.version import Version
from upath import UPath

import pydiverse.pipedag.backend.table.sql.hooks as sql_hooks
from pydiverse.pipedag import ConfigContext, Stage, Table
from pydiverse.pipedag.materialize.materializing_task import MaterializingTask
from pydiverse.pipedag.materialize.store import BaseTableCache
from pydiverse.pipedag.materialize.table_hook_base import CanMatResult, CanRetResult, TableHook
from pydiverse.pipedag.util import normalize_name
from pydiverse.pipedag.util.path import is_file_uri


class ParquetTableCache(BaseTableCache):
    """
    Local Table Cache that stores tables in `Parquet`_ files.

    .. rubric:: Supported Tables

    The `ParquetTableCache` supports Pandas, Polars and pydiverse.transform.


    :param base_path:
        A path to a folder where the Parquet files should get stored.
        To differentiate between different instances, the ``instance_id`` will
        automatically be appended to the provided path.

    .. _parquet:
        https://parquet.apache.org
    """

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        instance_id = normalize_name(ConfigContext.get().instance_id)

        config = config.copy()
        base_path = UPath(config.pop("base_path")) / instance_id
        return cls(base_path=base_path, **config)

    def __init__(self, *args, base_path: str | UPath, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_path = UPath(base_path).absolute()

    def setup(self):
        if is_file_uri(self.base_path):
            os.makedirs(self.base_path, exist_ok=True)

    def init_stage(self, stage: Stage):
        if is_file_uri(self.base_path):
            os.makedirs(self.base_path / stage.name, exist_ok=True)

    def clear_cache(self, stage: Stage):
        _dir = self.get_stage_path(stage)
        if is_file_uri(_dir):
            shutil.rmtree(_dir)
        else:
            _dir.rmdir(recursive=True)

    def _store_table(self, table: Table, task: MaterializingTask):
        if not super()._store_table(table, task):
            return

        metadata = {
            "cache_key": table.cache_key,
        }
        metadata_path = self.get_table_path(table, ".meta.json")
        metadata_path.write_text(json.dumps(metadata))

    def _has_table(self, table: Table, as_type: type) -> bool:
        metadata_path = self.get_table_path(table, ".meta.json")
        if not metadata_path.exists():
            return False

        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            return metadata["cache_key"] == table.cache_key
        except (OSError, json.decoder.JSONDecodeError):
            return False

    def get_stage_path(self, stage: Stage) -> UPath:
        return self.base_path / stage.name

    def get_table_path(self, table: Table, file_extension: str) -> UPath:
        return self.get_stage_path(table.stage) / (table.name + file_extension)


@ParquetTableCache.register_table(pd)
class PandasTableHook(TableHook[ParquetTableCache]):
    pd_version = Version(pd.__version__)

    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, pd.DataFrame))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(type_ == pd.DataFrame)

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        without_config_context: bool = False,
    ):
        path = store.get_table_path(table, ".parquet")

        df: pd.DataFrame = table.obj
        df.to_parquet(path)

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str | None,
        as_type: type[pd.DataFrame],
        limit: int | None = None,
    ) -> pd.DataFrame:
        # Determine dtype backend for pandas >= 2.0
        # [this is similar to the PandasTableHook found in SQLTableStore]

        backend_str = "numpy"
        if hook_args := ConfigContext.get().table_hook_args.get("pandas", None):
            if dtype_backend := hook_args.get("dtype_backend", None):
                backend_str = dtype_backend

        if isinstance(as_type, tuple):
            backend_str = as_type[1]
        elif isinstance(as_type, dict):
            backend_str = as_type["backend"]

        if PandasTableHook.pd_version < Version("2.0"):
            # for use_nullable_dtypes=False, returned types are mostly numpy backed
            # extension dtypes
            ret = cls._retrieve(store, table, limit, use_nullable_dtypes=backend_str != "arrow")
            # use_nullable_dtypes=False may still return string[python] even though we
            # expect and sometimes get string[pyarrow]
            for col in ret.dtypes[ret.dtypes == "string[python]"].index:
                ret[col] = ret[col].astype(pd.StringDtype("pyarrow"))
        else:
            dtype_backend_map = {"arrow": "pyarrow", "numpy": "numpy_nullable"}
            ret = cls._retrieve(store, table, limit, dtype_backend=dtype_backend_map[backend_str])

        # Prefer StringDtype("pyarrow") over ArrowDtype(pa.string()) for now.
        # We need to check this choice with future versions of pandas/pyarrow.
        for col in ret.dtypes[(ret.dtypes == "large_string[pyarrow]") | (ret.dtypes == "string[pyarrow]")].index:
            ret[col] = ret[col].astype(pd.StringDtype("pyarrow"))
        return ret

    @classmethod
    def _retrieve(cls, store, table, limit: int | None = None, **pandas_kwargs):
        path = store.get_table_path(table, ".parquet")
        import pyarrow.dataset as ds

        if limit is not None:
            # TODO: pandas_kwargs are currently ignored
            return ds.dataset(path).scanner().head(limit).to_pandas()
        else:
            return pd.read_parquet(path, **pandas_kwargs)


try:
    import polars as pl
except ImportError:
    pl = None


@ParquetTableCache.register_table(pl)
class PolarsTableHook(sql_hooks.PolarsTableHook):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        return CanMatResult.new(issubclass(type_, (pl.DataFrame, pl.LazyFrame)))

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        return CanRetResult.new(issubclass(type_, (pl.DataFrame, pl.LazyFrame)))

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table[pl.DataFrame],
        stage_name: str,
        without_config_context: bool = False,
    ):
        path = store.get_table_path(table, ".parquet")
        df = table.obj
        import polars as pl

        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        df.write_parquet(path)
        # intentionally don't apply annotation checks because they might also be done
        # within polars table hook of actual table store

    @classmethod
    def _execute_query(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type,
        dtypes: dict[str, pl.DataType] | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        _ = as_type
        path = store.get_table_path(table, ".parquet")
        df = pl.read_parquet(path, n_rows=limit)
        if issubclass(as_type, pl.LazyFrame):
            return df.lazy()
        return df


try:
    import pydiverse.transform as pdt

    try:
        from pydiverse.transform.eager import PandasTableImpl

        _ = PandasTableImpl

        pdt_old = pdt
        pdt_new = None
    except ImportError:
        try:
            # detect if 0.2 or >0.2 is active
            # this import would only work in <=0.2
            from pydiverse.transform.extended import Polars

            # ensures a "used" state for the import, preventing black from deleting it
            _ = Polars

            pdt_old = None
            pdt_new = pdt
        except ImportError:
            raise NotImplementedError("pydiverse.transform 0.2.0 - 0.2.2 isn't supported") from None
except ImportError:
    pdt = types.ModuleType("pydiverse.transform")
    pdt.Table = None
    pdt_old = None
    pdt_new = None


@ParquetTableCache.register_table(pdt_old)
class PydiverseTransformTableHookOld(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        if not issubclass(type_, pdt.Table):
            return CanMatResult.NO
        from pydiverse.transform.eager import PandasTableImpl

        return CanMatResult.YES_BUT_DONT_CACHE if issubclass(type_, PandasTableImpl) else CanMatResult.NO

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        from pydiverse.transform.eager import PandasTableImpl

        return CanRetResult.new(issubclass(type_, PandasTableImpl))

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table[pdt.Table],
        stage_name: str,
        without_config_context: bool = False,
    ):
        from pydiverse.transform.core.verbs import collect
        from pydiverse.transform.eager import PandasTableImpl

        t = table.obj
        table = table.copy_without_obj()

        if isinstance(t._impl, PandasTableImpl):
            table.obj = t >> collect()
            return store.get_r_table_hook(pd.DataFrame).materialize(store, table, stage_name)

        raise TypeError(f"Unsupported type {type(t._impl).__name__}")

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str | None,
        as_type: type,
        limit: int | None = None,
    ):
        from pydiverse.transform.eager import PandasTableImpl

        if isinstance(as_type, PandasTableImpl):
            hook = store.get_r_table_hook(pd.DataFrame)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame, limit)
            return pdt.Table(PandasTableImpl(table.name, df))

        raise ValueError(f"Invalid type {as_type}")


@ParquetTableCache.register_table(pdt_new)
class PydiverseTransformTableHook(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        type_ = type(tbl.obj)
        if not issubclass(type_, pdt.Table):
            return CanMatResult.NO
        from pydiverse.transform._internal.pipe.verbs import build_query

        query = tbl.obj >> build_query()
        if query is not None:
            # don't cache if the table is SQL backed
            return CanMatResult.NO
        else:
            return CanMatResult.YES_BUT_DONT_CACHE

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        from pydiverse.transform.extended import Polars

        return CanRetResult.new(issubclass(type_, Polars))

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table[pdt.Table],
        stage_name: str,
        without_config_context: bool = False,
    ):
        from pydiverse.transform.extended import (
            Polars,
            export,
        )

        t = table.obj
        table = table.copy_without_obj()

        try:
            table.obj = t >> export(Polars(lazy=True))

            hook = store.get_m_table_hook(table)
            return hook.materialize(store, table, stage_name)
        except Exception as e:
            raise TypeError(f"Unsupported type {type(t._ast).__name__}") from e

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str | None,
        as_type: type,
        limit: int | None = None,
    ):
        from pydiverse.transform.extended import Polars

        if isinstance(as_type, Polars):
            import polars as pl

            hook = store.get_r_table_hook(pl.LazyFrame)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame, limit)
            return pdt.Table(df)

        raise ValueError(f"Invalid type {as_type}")


@ParquetTableCache.register_table()
class SqlAlchemyTableHook(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        return CanMatResult.NO

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        # retrieving SQLAlchemy reference from parquet table cache does not make sense
        if type_ == sa.Table:
            return CanRetResult.NO_HOOK_IS_EXPECTED
        else:
            return CanRetResult.NO
