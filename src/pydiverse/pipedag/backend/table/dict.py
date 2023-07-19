from __future__ import annotations

import warnings

import pandas as pd

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.errors import CacheError, StageError
from pydiverse.pipedag.materialize.core import MaterializingTask, TaskInfo
from pydiverse.pipedag.materialize.metadata import LazyTableMetadata, TaskMetadata


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
                f"Can't copy table '{table.name}' to transaction."
                f" Stage '{stage.name}' has already been committed."
            )

        try:
            t = self.store[stage.name][table.name]
            self.store[stage.transaction_name][table.name] = t
        except KeyError:
            raise CacheError(
                f"No table with name '{table.name}' found in '{stage.name}' stage"
            ) from None

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        stage = table.stage
        if stage.did_commit:
            raise StageError(
                f"Can't copy table '{metadata.name}' to transaction."
                f" Stage '{stage.name}' has already been committed."
            )

        try:
            t = self.store[stage.name][metadata.name]
            self.store[stage.transaction_name][metadata.name] = t
        except KeyError:
            raise CacheError(
                f"No table with name '{metadata.name}' found in '{stage.name}' stage"
            ) from None

    def delete_table_from_transaction(self, table: Table):
        try:
            self.store[table.stage.transaction_name].pop(table.name)
        except KeyError:
            return

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        cache_key = metadata.input_hash + str(metadata.cache_fn_hash)
        self.t_metadata[stage][cache_key] = metadata

    def retrieve_task_metadata(
        self, task: MaterializingTask, input_hash: str, cache_fn_hash: str
    ) -> TaskMetadata:
        cache_key = input_hash + str(cache_fn_hash)
        try:
            return self.metadata[task.stage][cache_key]
        except KeyError:
            raise CacheError(
                "There is no metadata for task "
                f"'{task.name}' with cache key '{task.input_hash}', yet"
            ) from None

    def retrieve_all_task_metadata(self, task: MaterializingTask) -> list[TaskMetadata]:
        task_metadata = []
        for m in (
            *self.metadata[task.stage].values(),
            *self.t_metadata[task.stage].values(),
        ):
            if m.name == task.name and m.position_hash == task.position_hash:
                task_metadata.append(m)
        return task_metadata

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        cache_key = metadata.query_hash + metadata.task_hash
        self.t_lazy_table_metadata[metadata.stage][cache_key] = metadata

    def retrieve_lazy_table_metadata(
        self, query_hash: str, task_hash: str, stage: Stage
    ) -> LazyTableMetadata:
        try:
            cache_key = query_hash + task_hash
            return self.lazy_table_metadata[stage.name][cache_key]
        except (TypeError, KeyError):
            raise CacheError("Couldn't find metadata for lazy table") from None

    def get_objects_in_stage(self, stage):
        return list(self.store[stage.transaction_name].keys())

    def get_table_objects_in_stage(self, stage: Stage) -> list[str]:
        return list(self.store[stage.transaction_name].keys())


@DictTableStore.register_table(pd)
class PandasTableHook(TableHook[DictTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(
        cls,
        store,
        table: Table[pd.DataFrame],
        stage_name,
        task_info: TaskInfo | None,
    ):
        _ = task_info
        if table.name is not None:
            table.obj.attrs["name"] = table.name
        store.store[stage_name][table.name] = table.obj

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        return store.store[stage_name][table.name].copy()

    @classmethod
    def auto_table(cls, obj: pd.DataFrame):
        if name := obj.attrs.get("name"):
            return Table(obj, name)
        return super().auto_table(obj)


try:
    import pydiverse.transform as pdt
except ImportError as e:
    warnings.warn(str(e), ImportWarning)
    pdt = None


@DictTableStore.register_table(pdt)
class PydiverseTransformTableHook(TableHook[DictTableStore]):
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
        store,
        table: Table[pdt.Table],
        stage_name,
        task_info: TaskInfo | None,
    ):
        _ = task_info
        from pydiverse.transform.core.verbs import collect

        table.obj = table.obj >> collect()
        # noinspection PyTypeChecker
        return PandasTableHook.materialize(store, table, stage_name, task_info)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl

        df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame)
        return pdt.Table(PandasTableImpl(table.name, df))

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        # noinspection PyProtectedMember
        return Table(obj, obj._impl.name)
