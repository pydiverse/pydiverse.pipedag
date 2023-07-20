from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
from packaging.version import Version

from pydiverse.pipedag import ConfigContext, Stage, Table
from pydiverse.pipedag.backend.table.base import TableHook
from pydiverse.pipedag.backend.table.cache.base import BaseTableCache
from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo
from pydiverse.pipedag.util import normalize_name


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
        base_path = Path(config.pop("base_path")) / instance_id
        return cls(base_path=base_path, **config)

    def __init__(self, *args, base_path: str | Path, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_path = Path(base_path).absolute()

    def setup(self):
        os.makedirs(self.base_path, exist_ok=True)

    def init_stage(self, stage: Stage):
        os.makedirs(self.base_path / stage.name, exist_ok=True)

    def clear_cache(self, stage: Stage):
        shutil.rmtree(self.get_stage_path(stage))

    def _store_table(self, table: Table, task: MaterializingTask, task_info: TaskInfo):
        super()._store_table(table, task, task_info)

        metadata = {
            "cache_key": table.cache_key,
        }
        metadata_path = self.get_table_path(table, ".meta.json")
        metadata_path.write_text(json.dumps(metadata))

    def _has_table(self, table: Table, as_type: type) -> bool:
        metadata_path = self.get_table_path(table, ".meta.json")
        if not metadata_path.exists():
            return False

        metadata = json.loads(metadata_path.read_text())
        return metadata["cache_key"] == table.cache_key

    def get_stage_path(self, stage: Stage):
        return self.base_path / stage.name

    def get_table_path(self, table: Table, file_extension: str) -> Path:
        return self.get_stage_path(table.stage) / (table.name + file_extension)


@ParquetTableCache.register_table(pd)
class PandasTableHook(TableHook[ParquetTableCache]):
    pd_version = Version(pd.__version__)

    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        task_info: TaskInfo,
    ):
        path = store.get_table_path(table, ".parquet")

        df: pd.DataFrame = table.obj
        df.to_parquet(path)

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type[pd.DataFrame],
    ) -> pd.DataFrame:
        if PandasTableHook.pd_version < Version("2.0"):
            return cls._retrieve(store, table, use_nullable_dtype=True)

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

        dtype_backend = {"arrow": "pyarrow", "numpy": "numpy_nullable"}
        return cls._retrieve(store, table, dtype_backend=dtype_backend[backend_str])

    @classmethod
    def _retrieve(cls, store, table, **pandas_kwargs):
        path = store.get_table_path(table, ".parquet")
        return pd.read_parquet(path, **pandas_kwargs)


try:
    import polars
except ImportError:
    polars = None


@ParquetTableCache.register_table(polars)
class PolarsTableHook(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table[polars.DataFrame],
        stage_name: str,
        task_info: TaskInfo,
    ):
        path = store.get_table_path(table, ".parquet")
        table.obj.write_parquet(path)

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type,
    ):
        path = store.get_table_path(table, ".parquet")
        return polars.read_parquet(path)


try:
    import pydiverse.transform as pdt
except ImportError:
    pdt = None


@ParquetTableCache.register_table(pdt)
class PydiverseTransformTableHook(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pdt.Table)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        from pydiverse.transform.eager import PandasTableImpl

        return issubclass(type_, PandasTableImpl)

    @classmethod
    def materialize(
        cls,
        store: ParquetTableCache,
        table: Table[pdt.Table],
        stage_name: str,
        task_info: TaskInfo,
    ):
        from pydiverse.transform.core.verbs import collect
        from pydiverse.transform.eager import PandasTableImpl

        t = table.obj
        table = table.copy_without_obj()

        if isinstance(t._impl, PandasTableImpl):
            table.obj = t >> collect()
            return store.get_hook_subclass(PandasTableHook).materialize(
                store, table, stage_name, task_info
            )

        raise TypeError(f"Unsupported type {type(t._impl).__name__}")

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type,
    ):
        from pydiverse.transform.eager import PandasTableImpl

        if isinstance(as_type, PandasTableImpl):
            hook = store.get_hook_subclass(PandasTableHook)
            df = hook.retrieve(store, table, stage_name, pd.DataFrame)
            return pdt.Table(PandasTableImpl(table.name, df))

        raise ValueError(f"Invalid type {as_type}")
