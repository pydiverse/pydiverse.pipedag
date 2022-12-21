from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog
from attrs import frozen

from pydiverse.pipedag.context import ConfigContext, StageLockContext
from pydiverse.pipedag.context.run_context import DematerializeRunContext
from pydiverse.pipedag.core.task import Task, TaskGetItem
from pydiverse.pipedag.errors import LockError
from pydiverse.pipedag.materialize.core import MaterializingTask
from pydiverse.pipedag.util import deep_map

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import Materializable


@frozen
class Result:
    underlying: Any
    successful: bool
    config_context: ConfigContext | None

    task_values: dict[Task, Any]
    exception: Exception = None

    def get(self, task: Task | TaskGetItem, as_type: type = None) -> Materializable:
        """Load the results of a task from the database.

        :param task: The task
        :param as_type: The type as which tables produced by this task should
            be dematerialized. If no type is specified, the input type of
            the task is used.
        :return: The results of the task.
        """
        if not self.successful:
            logger = structlog.getLogger()
            logger.warning(
                "Attention: getting tables from unsuccessful run is unreliable!"
            )

        if isinstance(task, Task):
            root_task = task
        else:
            root_task = task.task

        if as_type is None:
            assert isinstance(root_task, MaterializingTask)
            as_type = root_task.input_type

        if self.config_context.strict_result_get_locking:
            try:
                StageLockContext.get()
            except LookupError:
                raise LockError(
                    "Called Result.get() without opening StageLockContext. Consider"
                    " using 'strict_result_get_locking: false' for interactive"
                    " debugging"
                ) from None

        # TODO: Check that the results loaded from the database correspond
        #       to the run_id of this result object.
        with self.config_context as config_ctx, DematerializeRunContext() as run_ctx:

            def dematerialize_mapper(item):
                return config_ctx.store.dematerialize_item(
                    item, as_type=as_type, ctx=run_ctx
                )

            task_value = self.task_values[root_task]
            return deep_map(task.resolve_value(task_value), dematerialize_mapper)
