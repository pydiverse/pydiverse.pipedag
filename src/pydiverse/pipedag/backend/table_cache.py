from __future__ import annotations

import copy
import json
import os
import shutil
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from pydiverse.pipedag import Table
from pydiverse.pipedag._typing import T
from pydiverse.pipedag.backend.table.base import TableHook, TableHookResolver
from pydiverse.pipedag.context import ConfigContext
from pydiverse.pipedag.util import normalize_name
from pydiverse.pipedag.util.naming import NameDisambiguator

if TYPE_CHECKING:
    from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo

__all__ = [
    "BaseTableCache",
    "ParquetTableCache",
]


class BaseTableCache(TableHookResolver, ABC):
    """Local Table cache base class

    A local table cache stores tables after retrieval from / before
    materialization to table store.

    The local table cache is only used if it stores the correct version as
    would be retrieved from the table store.
    """

    @abstractmethod
    def has_cache_table(self, table: Table, input_hash: str):
        """Checks if the cache has a table for the given input hash"""

    @abstractmethod
    def list_stages(self) -> list[str]:
        """Return list of stages for which cached tables exist"""

    @abstractmethod
    def clear_stages(self, stages: list[str] | None) -> Any:
        """Clear cached tables for the given stages"""


class ParquetTableCache(BaseTableCache):
    """Parquet file based local table cache

    The ParquetTableCache stores tables as parquet files in a folder structure
    on a file system. Files will be stored in the following structure:
    `base_path/instance_id/STAGE_NAME/in or out/TABLE_NAME.pickle`.
    """

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        instance_id = normalize_name(ConfigContext.get().instance_id)
        base_path = Path(config["base_path"]) / instance_id
        return cls(base_path)

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path).absolute()
        os.makedirs(self.base_path, exist_ok=True)

    def store_table(self, table: Table, task: MaterializingTask, task_info: TaskInfo):
        _dir = self.base_path / table.stage.name
        _dir.mkdir(exist_ok=True, parents=True)

        super().store_table(table, task, task_info)

        input_hash = task_info.task_cache_info._input_hash
        path = self.base_path / table.stage.name / f"{table.name}.cache.json"
        path.write_text(json.dumps(dict(input_hash=input_hash)))

    def has_cache_table(self, table: Table, input_hash: str):
        """Checks if the cache has a table for the given cache info"""
        path = self.base_path / table.stage.name / f"{table.name}.cache.json"
        if not path.exists():
            return False
        stored_info = json.loads(path.read_text())
        return stored_info.input_hash == input_hash

    def list_stages(self) -> list[str]:
        return [entry.name for entry in self.base_path.iterdir() if entry.is_dir()]

    def clear_stages(self, stages: list[str] | None = None):
        if stages is None:
            delete_pathes = [self.base_path]
        else:
            delete_pathes = [self.base_path / stage for stage in stages]
        for delete_path in delete_pathes:
            shutil.rmtree(delete_path)


@ParquetTableCache.register_table(pd)
class PandasParquetTableHook(TableHook[ParquetTableCache]):
    """
    Materalize pandas->parquet and retrieve parquet->pandas.
    """

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
        table: Table[pd.DataFrame],
        stage_name,
        task_info: TaskInfo,
    ):
        assert isinstance(store, ParquetTableCache)
        path = store.base_path / stage_name / f"{table.name}.parquet"
        # we might try to avoid this copy for speedup / saving RAM
        df = table.obj.copy()
        df.to_parquet(path)

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type[pd.DataFrame],
        namer: NameDisambiguator | None = None,
    ) -> pd.DataFrame:
        path = store.base_path / stage_name / f"{table.name}.parquet"
        return pd.read_parquet(path)

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


try:
    # optional dependency to polars
    import connectorx
    import polars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    polars = None
    connectorx = None


@ParquetTableCache.register_table(polars, connectorx)
class PolarsParquetTableHook(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        # attention: tidypolars.Tibble is subclass of polars DataFrame
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == polars.dataframe.DataFrame

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[polars.dataframe.DataFrame],
        stage_name,
        task_info: TaskInfo,
    ):
        path = store.base_path / stage_name / f"{table.name}.parquet"
        table.obj.to_parquet(path)

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type[polars.dataframe.DataFrame],
        namer: NameDisambiguator | None = None,
    ) -> polars.dataframe.DataFrame:
        path = store.base_path / stage_name / f"{table.name}.parquet"
        return polars.read_parquet(path)

    @classmethod
    def auto_table(cls, obj: polars.dataframe.DataFrame):
        # currently, we don't know how to store a table name inside polars dataframe
        return super().auto_table(obj)


try:
    # optional dependency to polars
    import tidypolars
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    tidypolars = None


@ParquetTableCache.register_table(tidypolars, polars, connectorx)
class TidyPolarsParquetTableHook(TableHook[ParquetTableCache]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, tidypolars.Tibble)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == tidypolars.Tibble

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[tidypolars.Tibble],
        stage_name,
        task_info: TaskInfo,
    ):
        tibble = table.obj
        table.obj = None
        table = copy.deepcopy(table)
        table.obj = tibble.to_polars()
        PolarsParquetTableHook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type[tidypolars.Tibble],
        namer: NameDisambiguator | None = None,
    ) -> tidypolars.Tibble:
        df = PolarsParquetTableHook.retrieve(store, table, stage_name, as_type, namer)
        return tidypolars.from_polars(df)

    @classmethod
    def auto_table(cls, obj: tidypolars.Tibble):
        return super().auto_table(obj)


try:
    # optional dependency to pydiverse-transform
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = None


# noinspection PyUnresolvedReferences, PyProtectedMember
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
        cls, store, table: Table[pdt.Table], stage_name, task_info: TaskInfo
    ):
        from pydiverse.transform.eager import PandasTableImpl

        t = table.obj
        table.obj = None
        # noinspection PyTypeChecker
        pd_table = copy.deepcopy(table)  # type: Table[pd.DataFrame]
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            pd_table.obj = t >> collect()
            return PandasParquetTableHook.materialize(
                store, pd_table, stage_name, task_info
            )
        raise NotImplementedError

    @classmethod
    def retrieve(
        cls,
        store: ParquetTableCache,
        table: Table,
        stage_name: str,
        as_type: type[T],
        namer: NameDisambiguator | None = None,
    ) -> T:
        from pydiverse.transform.eager import PandasTableImpl

        if issubclass(as_type, PandasTableImpl):
            df = PandasParquetTableHook.retrieve(
                store, table, stage_name, pd.DataFrame, namer
            )
            return pdt.Table(PandasTableImpl(table.name, df))
        raise NotImplementedError

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        return Table(obj, obj._impl.name)

    @classmethod
    def lazy_query_str(cls, store, obj: pdt.Table) -> str:
        from pydiverse.transform.core.verbs import build_query

        query = obj >> build_query()

        if query is not None:
            return str(query)
        return super().lazy_query_str(store, obj)
