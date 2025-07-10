# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause
import types
import warnings

import pandas as pd

from pydiverse.pipedag import Schema, Stage, Table
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.errors import CacheError, StageError
from pydiverse.pipedag.materialize.materializing_task import MaterializingTask
from pydiverse.pipedag.materialize.metadata import LazyTableMetadata, TaskMetadata
from pydiverse.pipedag.materialize.store import BaseTableStore
from pydiverse.pipedag.materialize.table_hook_base import CanMatResult, CanRetResult, TableHook


class DictTableStore(BaseTableStore):
    """
    A very basic table store that stores objects in a dictionary.
    Should only ever be used for testing.
    """

    def __init__(self):
        super().__init__()
        self.store = dict()

        self.metadata = dict()
        self.t_metadata = dict()

        self.lazy_table_metadata = dict()
        self.t_lazy_table_metadata = dict()

    def init_stage(self, stage: Stage):
        self.store.setdefault(stage.name, {})
        self.store[stage.transaction_name] = {}

        self.metadata.setdefault(stage, {})
        self.t_metadata[stage] = {}

        self.lazy_table_metadata.setdefault(stage, {})
        self.t_lazy_table_metadata[stage] = {}

    def commit_stage(self, stage: Stage):
        self.store[stage.name] = self.store[stage.transaction_name]
        del self.store[stage.transaction_name]
        self.metadata[stage] = self.t_metadata[stage]
        self.lazy_table_metadata[stage] = self.t_lazy_table_metadata[stage]

    def copy_table_to_transaction(self, table: Table):
        stage = table.stage
        if stage.did_commit:
            raise StageError(
                f"Can't copy table '{table.name}' to transaction. Stage '{stage.name}' has already been committed."
            )

        try:
            t = self.store[stage.name][table.name]
            self.store[stage.transaction_name][table.name] = t
        except KeyError:
            raise CacheError(f"No table with name '{table.name}' found in '{stage.name}' stage") from None

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        stage = table.stage
        if stage.did_commit:
            raise StageError(
                f"Can't copy table '{metadata.name}' to transaction. Stage '{stage.name}' has already been committed."
            )

        try:
            RunContext.get().trace_hook.cache_pre_transfer(table)
            t = self.store[stage.name][metadata.name]
            self.store[stage.transaction_name][metadata.name] = t
            RunContext.get().trace_hook.cache_post_transfer(table)
        except KeyError:
            raise CacheError(f"No table with name '{metadata.name}' found in '{stage.name}' stage") from None

    def delete_table_from_transaction(self, table: Table, *, schema: Schema | None = None):
        if schema is None:
            schema = self.get_schema(table.stage.transaction_name)
        try:
            self.store[schema.name].pop(table.name)
        except KeyError:
            return

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        cache_key = metadata.input_hash + str(metadata.cache_fn_hash)
        self.t_metadata[stage][cache_key] = metadata

    def retrieve_task_metadata(self, task: MaterializingTask, input_hash: str, cache_fn_hash: str) -> TaskMetadata:
        cache_key = input_hash + str(cache_fn_hash)
        try:
            return self.metadata[task.stage][cache_key]
        except KeyError:
            raise CacheError(
                f"There is no metadata for task '{task.name}' with cache key '{task.input_hash}', yet"
            ) from None

    def retrieve_all_task_metadata(
        self, task: MaterializingTask, ignore_position_hashes: bool = False
    ) -> list[TaskMetadata]:
        task_metadata = []
        for m in (
            *self.metadata[task.stage].values(),
            *self.t_metadata[task.stage].values(),
        ):
            if m.name == task.name and ((m.position_hash == task.position_hash) or ignore_position_hashes):
                task_metadata.append(m)
        return task_metadata

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        cache_key = metadata.query_hash + metadata.task_hash
        self.t_lazy_table_metadata[metadata.stage][cache_key] = metadata

    def retrieve_lazy_table_metadata(self, query_hash: str, task_hash: str, stage: Stage) -> LazyTableMetadata:
        try:
            cache_key = query_hash + task_hash
            return self.lazy_table_metadata[stage.name][cache_key]
        except (TypeError, KeyError):
            raise CacheError("Couldn't find metadata for lazy table") from None

    def get_objects_in_stage(self, stage):
        return list(self.store[stage.transaction_name].keys())

    def get_table_objects_in_stage(self, stage: Stage, include_views=True) -> list[str]:
        _ = include_views  # not supported for dict table store
        return list(self.store[stage.transaction_name].keys())


@DictTableStore.register_table(pd)
class PandasTableHook(TableHook[DictTableStore]):
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
        store,
        table: Table[pd.DataFrame],
        stage_name,
        without_config_context: bool = False,
    ):
        if table.name is not None:
            table.obj.attrs["name"] = table.name
        store.store[stage_name][table.name] = table.obj

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type, limit: int | None = None):
        df = store.store[stage_name][table.name]
        if limit:
            df = df[:limit]
        return df.copy()

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


try:
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = types.ModuleType("pydiverse.transform")
    pdt.Table = None


@DictTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[DictTableStore]):
    @classmethod
    def can_materialize(cls, tbl: Table) -> CanMatResult:
        import pydiverse.transform as pdt
        from pydiverse.transform.extended import build_query

        if not isinstance(tbl, pdt.Table):
            # Not a pydiverse transform table
            return CanMatResult.NO

        query = tbl.obj >> build_query()
        if query is None:
            # SQL is not supported
            return CanMatResult.NO
        else:
            # this error is expected for eager backend
            type_ = type(tbl.obj)
            return CanMatResult.YES_BUT_DONT_CACHE if issubclass(type_, pdt.Table) else CanMatResult.NO

    @classmethod
    def can_retrieve(cls, type_) -> CanRetResult:
        from pydiverse.transform import Pandas, Polars

        return CanRetResult.new(type_ is Polars or type_ is Pandas)

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pdt.Table],
        stage_name,
        without_config_context: bool = False,
    ):
        from pydiverse.transform.extended import Pandas, collect

        table.obj = table.obj >> collect(Pandas)
        # noinspection PyTypeChecker
        return PandasTableHook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type, limit: int | None = None):
        df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame, limit)
        from pydiverse.transform import Polars

        if as_type is Polars:
            import polars as pl

            return pdt.Table(pl.from_pandas(df), name=table.name)
        else:
            return pdt.Table(df, name=table.name)

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        # noinspection PyProtectedMember
        return Table(obj, obj._impl.name)
