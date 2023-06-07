from __future__ import annotations

import json
import shutil
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import structlog

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


class LocalTableCache:
    """Proxy class for local table cache which interprets backend independent options"""

    def __init__(
        self,
        obj: BaseTableCache | None,
        store_input,
        store_output,
        use_stored_input_as_cache,
    ):
        self.logger = structlog.get_logger(logger_name=type(self).__name__)
        self.obj = obj
        self.store_input = store_input
        self.store_output = store_output
        self.use_stored_input_as_cache = use_stored_input_as_cache

    def store_table(
        self,
        table: Table,
        task: MaterializingTask | None,
        task_info: TaskInfo | None,
        is_input: bool,
    ):
        """Stores a table in the associated transaction stage"""
        if self.obj is not None and (
            (is_input and self.store_input) or (not is_input and self.store_output)
        ):
            try:
                self.obj.store_table(table, task, task_info)
            except TypeError:
                self.logger.debug(
                    (
                        "Could not store table in local table cache (expected for"
                        " non-dataframe types)"
                    ),
                    name=table.name,
                    type=type(table.obj),
                )

    def retrieve_table_obj(
        self, table: Table, as_type: type[T], namer: NameDisambiguator | None = None
    ) -> T:
        assert (
            self.obj is not None
        ), "this method should only be called if has_cache_table returns True"
        return self.obj.retrieve_table_obj(
            table, as_type, namer, use_transaction_name=False
        )

    def has_cache_table(self, table: Table, astype: type[T]):
        """Checks if the cache has a table for the given table.cache_key"""
        if self.obj is None or not self.use_stored_input_as_cache:
            return False
        return self.obj.has_cache_table(table, astype)

    def list_stages(self) -> list[str]:
        """Return list of stages for which cached tables exist"""
        if self.obj is None:
            return []
        return self.obj.list_stages()

    def clear_stages(self, stages: list[str] | None) -> Any:
        """Clear cached tables for the given stages"""
        if self.obj is not None:
            self.obj.clear_stages(stages)

    def dispose(self):
        if self.obj is not None:
            self.obj.dispose()


class BaseTableCache(TableHookResolver, ABC):
    """Local Table cache base class

    A local table cache stores tables after retrieval from / before
    materialization to table store.

    The local table cache is only used if it stores the correct version as
    would be retrieved from the table store.
    """

    @abstractmethod
    def has_cache_table(self, table: Table, as_type: type[T]):
        """Checks if the cache has a table for the given table.cache_key"""

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

    def store_table(self, table: Table, task: MaterializingTask, task_info: TaskInfo):
        _dir = self.base_path / table.stage.name
        _dir.mkdir(exist_ok=True, parents=True)

        super().store_table(table, task, task_info, use_transaction_name=False)

        path = self.base_path / table.stage.name / f"{table.name}.cache.json"
        path.write_text(json.dumps(dict(cache_key=table.cache_key)))

    def has_cache_table(self, table: Table, as_type: type[T]):
        """Checks if the cache has a table for the given table.cache_key"""
        try:
            self.get_r_table_hook(as_type)
        except TypeError:
            return False  # cannot dematerialize type wish

        path = self.base_path / table.stage.name / f"{table.name}.cache.json"
        if not path.exists():
            return False
        stored_info = json.loads(path.read_text())
        return stored_info["cache_key"] == table.cache_key

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
        task_info: TaskInfo | None,
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
        task_info: TaskInfo | None,
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
        task_info: TaskInfo | None,
    ):
        t = table.obj
        table = table.copy_without_obj()
        table.obj = t.to_polars()
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
        cls, store, table: Table[pdt.Table], stage_name, task_info: TaskInfo | None
    ):
        from pydiverse.transform.eager import PandasTableImpl

        t = table.obj
        table = table.copy_without_obj()
        if isinstance(t._impl, PandasTableImpl):
            from pydiverse.transform.core.verbs import collect

            table.obj = t >> collect()
            return PandasParquetTableHook.materialize(
                store, table, stage_name, task_info
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
