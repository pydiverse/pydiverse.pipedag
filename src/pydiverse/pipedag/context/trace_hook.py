from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from pydiverse.pipedag import Result, Table, Task
    from pydiverse.pipedag._typing import Materializable
    from pydiverse.pipedag.context import RunContext, RunContextServer
    from pydiverse.pipedag.materialize.cache import TaskCacheInfo
    from pydiverse.pipedag.materialize.metadata import TaskMetadata


class TraceHook:
    def run_init_context_server(self, run_context_server: RunContextServer):
        pass

    def run_init_context(self, run_context: RunContext):
        pass

    def run_complete(self, result: Result):
        pass

    def stage_init(self, stage_id: int, success: bool):
        pass

    def stage_pre_commit(self, stage_id: int):
        pass

    def stage_post_commit(self, stage_id: int, success: bool):
        pass

    def task_cache_status(
        self,
        task: Task,
        input_hash: str,
        cache_fn_hash: str,
        cache_metadata: TaskMetadata | None = None,
        cached_output: Materializable | None = None,
        cache_valid: bool | None = None,
        lazy: bool | None = None,
    ):
        pass

    def task_complete(self):
        pass

    def query_cache_status(
        self,
        task: Task,
        table: Table,
        task_cache_info: TaskCacheInfo,
        query_hash: str,
        query_str: str,
        cache_metadata: TaskMetadata | None = None,
        cache_valid: bool | None = None,
    ):
        pass

    def query_complete(self, task: Task, table: Table):
        pass

    # def cache_transfer_start(self):
    # def cache_transfer_complete(self):


class PrintTraceHook(TraceHook):
    def __init__(self):
        self.logger = structlog.get_logger(logger_name=type(self).__name__)
        self.run_context = None
        self.run_context_server = None

    def run_init_context_server(self, run_context_server: RunContextServer):
        self.run_context_server = run_context_server
        self.logger.debug(
            "run_init_context_server", run_context_server=run_context_server
        )

    def run_init_context(self, run_context: RunContext):
        self.run_context = run_context
        self.logger.debug("run_init_context", run_context=run_context)

    def run_complete(self, result: Result):
        self.logger.debug("run_complete", result=result)

    def stage_init(self, stage_id: int, success: bool):
        self.logger.debug(
            "stage_init",
            stage_id=stage_id,
            stage=list(self.run_context.flow.stages.values())[stage_id],
            success=success,
        )

    def stage_pre_commit(self, stage_id: int):
        self.logger.debug(
            "stage_pre_commit",
            stage_id=stage_id,
            stage=list(self.run_context.flow.stages.values())[stage_id],
        )

    def stage_post_commit(self, stage_id: int, success: bool):
        self.logger.debug("stage_post_commit", stage_id=stage_id, success=success)

    def task_cache_status(
        self,
        task: Task,
        input_hash: str,
        cache_fn_hash: str,
        cache_metadata: TaskMetadata | None = None,
        cached_output: Materializable | None = None,
        cache_valid: bool | None = None,
        lazy: bool | None = None,
    ):
        self.logger.debug(
            "task_cache_status",
            task=task,
            input_hash=input_hash,
            cache_fn_hash=cache_fn_hash,
            cache_metadata=cache_metadata,
            cached_output=cached_output,
            cache_valid=cache_valid,
            lazy=lazy,
        )

    def task_complete(self):
        self.logger.debug("task_complete")

    def query_cache_status(
        self,
        task: Task,
        table: Table,
        task_cache_info: TaskCacheInfo,
        query_hash: str,
        query_str: str,
        cache_metadata: TaskMetadata | None = None,
        cache_valid: bool | None = None,
    ):
        self.logger.debug(
            "query_cache_status",
            task=task,
            table=table,
            detail=table.assumed_dependencies,
            task_cache_info=task_cache_info,
            query_hash=query_hash,
            query=query_str,
            cache_metadata=cache_metadata,
            cache_valid=cache_valid,
        )

    def query_complete(self, task: Task, table: Table):
        self.logger.debug("query_complete", task=task, table=table)
