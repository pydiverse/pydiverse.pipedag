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
        """
        Called after run context server was initialized.

        The run context server launches a central service in case of multi-node
        execution. Hooks might integrate with it to collect information at a central
        point.
        """
        pass

    def run_init_context(self, run_context: RunContext):
        """
        Called after initialization of run context.

        This happens when entering RunContextServer context.
        """
        pass

    def run_complete(self, result: Result):
        """
        Called after completion of pipedag run when result is available.
        """
        pass

    def stage_post_init(self, stage_id: int, success: bool):
        """
        Called after initialization of stage.

        The stage schema can be used for writing tables from this moment on.
        """
        pass

    def stage_pre_commit(self, stage_id: int):
        """
        Called before committing stage.
        """
        pass

    def stage_post_commit(self, stage_id: int, success: bool):
        """
        Called after committing stage.

        Stage commit may include copying remaining tables from cache to transaction
        schema.
        """
        pass

    def task_begin(self, task: Task):
        """
        Called at beginning of task execution.

        This typically is just very shortly before task_cache_status. It also can be
        used as the starting point for dematerialization.
        """
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
        """
        Called after it is known whether task is cache valid for non-lazy tasks.

        It is not called in case cache validity check is disabled. For lazy tasks,
        it is also called for technical reasons because the cache_fn_hash may be
        retrieved from the cache in order to avoid recomputation in some special
        cases.
        """
        pass

    def task_pre_call(self, task: Task):
        """
        Called before call of task function.

        This is the same moment when dematerialization completed.
        """
        pass

    def task_post_call(self, task: Task):
        """
        Called after call of task function.

        This is the same moment when materialization begins.
        """
        pass

    def task_complete(self, task: Task, result: Result):
        """
        Called after task completion.

        This is the same moment when materialization completes.
        All query related callbacks of this task are called before this.
        """
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
        """
        Called after it is known whether query is cache valid.

        Task caching information is available in task_cache_info.
        In addition, query_str and table.assumed_dependencies are used for computing
        query_hash.
        """
        pass

    def query_complete(self, task: Task, table: Table):
        """
        Called after query completion in case query is not cache valid.
        """
        pass

    def cache_init_transfer(self, task: Task, table: Table):
        """
        Called when preparing transfer from cached table to transactions schema.
        """
        pass

    def cache_pre_transfer(self, table: Table):
        """
        Called before transferring cached table to transaction schema.

        This function may be called in context of another thread or process.
        """
        pass

    def cache_post_transfer(self, table: Table):
        """
        Called after transferring cached table to transaction schema.

        This function may be called in context of another thread or process.
        """
        pass


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

    def stage_post_init(self, stage_id: int, success: bool):
        self.logger.debug(
            "stage_post_init",
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

    def task_begin(self, task: Task):
        self.logger.debug("task_begin", task=task)

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

    def task_pre_call(self, task: Task):
        self.logger.debug("task_pre_call", task=task)

    def task_post_call(self, task: Task):
        self.logger.debug("task_post_call", task=task)

    def task_complete(self, task: Task, result: Result):
        self.logger.debug("task_complete", task=task, result=result)

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

    def cache_init_transfer(self, task: Task, table: Table):
        self.logger.debug("cache_init_transfer", task=task, table=table)

    def cache_pre_transfer(self, table: Table):
        self.logger.debug("cache_pre_transfer", table=table)

    def cache_post_transfer(self, table: Table):
        self.logger.debug("cache_post_transfer", table=table)
