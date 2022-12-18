from __future__ import annotations

import warnings

import pandas as pd

from pydiverse.pipedag import Stage, Table
from pydiverse.pipedag.backend.table.base import BaseTableStore, TableHook
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.errors import CacheError, StageError
from pydiverse.pipedag.materialize.core import (
    MaterializingTask,
    get_effective_cache_key,
)
from pydiverse.pipedag.materialize.metadata import (
    LazyTableMetadata,
    RawSqlMetadata,
    TaskMetadata,
)


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

        self.raw_sql_metadata = dict()
        self.t_raw_sql_metadata = dict()

    def init_stage(self, stage: Stage):
        self.store.setdefault(stage.name, {})
        self.store[stage.transaction_name] = {}

        self.metadata.setdefault(stage, {})
        self.t_metadata[stage] = {}

        self.lazy_table_metadata.setdefault(stage, {})
        self.t_lazy_table_metadata[stage] = {}

        self.raw_sql_metadata.setdefault(stage, {})
        self.t_raw_sql_metadata[stage] = {}

    def commit_stage(self, stage: Stage):
        self.store[stage.name] = self.store[stage.transaction_name]
        del self.store[stage.transaction_name]
        self.metadata[stage] = self.t_metadata[stage]
        self.lazy_table_metadata[stage] = self.t_lazy_table_metadata[stage]
        self.raw_sql_metadata[stage] = self.t_raw_sql_metadata[stage]

    def copy_table_to_transaction(self, table: Table):
        stage = table.stage
        if stage.did_commit:
            raise StageError(
                f"Can't copy table '{table.name}' to transaction."
                f" Stage '{stage.name}' has already been committed."
            )
        if table.store_id is None:
            raise ValueError(f"Table cache keys can't be None")

        try:
            self.store[stage.transaction_name][table.name] = self.store[stage.name][
                table.name
            ]
        except KeyError:
            raise CacheError

    def copy_lazy_table_to_transaction(self, metadata: LazyTableMetadata, table: Table):
        if (metadata.stage not in self.store) or (
            metadata.name not in self.store[metadata.stage]
        ):
            raise CacheError(
                f"Can't copy lazy table '{metadata.name}' (stage:"
                f" '{metadata.stage}') to transaction because no such table"
                " exists."
            )

        self.store[table.stage.transaction_name][table.name] = self.store[
            metadata.stage
        ][metadata.name]

    def copy_raw_sql_tables_to_transaction(
        self, metadata: RawSqlMetadata, target_stage: Stage
    ):
        raise NotImplementedError(
            "The DictTableStore does not support raw SQL statements"
        )

    def delete_table_from_transaction(self, table: Table):
        try:
            self.store[table.stage.transaction_name].pop(table.name)
        except KeyError:
            return

    def store_task_metadata(self, metadata: TaskMetadata, stage: Stage):
        ctx = RunContext.get()
        # TODO: sql.py has more elaborate filter to ensure ignore_fresh_input can be switched on and off
        cache_key = get_effective_cache_key(
            ctx.ignore_fresh_input,
            metadata.input_hash,
            metadata.version,
            metadata.cache_fn_hash,
        )
        self.t_metadata[stage][cache_key] = metadata

    def copy_task_metadata_to_transaction(self, task: MaterializingTask):
        stage = task.stage
        ctx = RunContext.get()
        # TODO: sql.py has more elaborate filter to ensure ignore_fresh_input can be switched on and off
        cache_key = task.get_effective_cache_key(ctx)
        self.t_metadata[stage][cache_key] = self.metadata[stage][cache_key]

    def retrieve_task_metadata(self, task: MaterializingTask) -> TaskMetadata:
        ctx = RunContext.get()
        # TODO: sql.py has more elaborate filter to ensure ignore_fresh_input can be switched on and off
        cache_key = task.get_effective_cache_key(ctx)
        try:
            return self.metadata[task.stage][cache_key]
        except KeyError:
            raise CacheError(
                f"There is no metadata for task '{task.name}' with cache key"
                f" '{cache_key}'({ctx.ignore_fresh_input}), yet"
            )

    def store_lazy_table_metadata(self, metadata: LazyTableMetadata):
        self.t_lazy_table_metadata[metadata.stage][metadata.query_hash] = metadata

    def retrieve_lazy_table_metadata(
        self, cache_key: str, stage: Stage
    ) -> LazyTableMetadata:
        try:
            return self.lazy_table_metadata[stage.name][cache_key]
        except (TypeError, KeyError):
            raise CacheError

    def store_raw_sql_metadata(self, metadata: RawSqlMetadata):
        self.t_raw_sql_metadata[metadata.stage][metadata.cache_key] = metadata

    def retrieve_raw_sql_metadata(self, cache_key: str, stage: Stage) -> RawSqlMetadata:
        try:
            return self.raw_sql_metadata[stage.name][cache_key]
        except (TypeError, KeyError):
            raise CacheError

    def list_tables(self, stage):
        return self.store[stage.transaction_name].keys()


@DictTableStore.register_table(pd)
class PandasTableHook(TableHook[DictTableStore]):
    @classmethod
    def can_materialize(cls, type_) -> bool:
        return issubclass(type_, pd.DataFrame)

    @classmethod
    def can_retrieve(cls, type_) -> bool:
        return type_ == pd.DataFrame

    @classmethod
    def materialize(cls, store, table: Table[pd.DataFrame], stage_name):
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
    def materialize(cls, store, table: Table[pdt.Table], stage_name):
        from pydiverse.transform.core.verbs import collect

        table.obj = table.obj >> collect()
        # noinspection PyTypeChecker
        return PandasTableHook.materialize(store, table, stage_name)

    @classmethod
    def retrieve(cls, store, table, stage_name, as_type):
        from pydiverse.transform.eager import PandasTableImpl

        df = PandasTableHook.retrieve(store, table, stage_name, pd.DataFrame)
        return pdt.Table(PandasTableImpl(table.name, df))

    @classmethod
    def auto_table(cls, obj: pdt.Table):
        # noinspection PyProtectedMember
        return Table(obj, obj._impl.name)
