from __future__ import annotations

from typing import TYPE_CHECKING, Any

from attrs import frozen

from pydiverse.pipedag.context import ConfigContext
from pydiverse.pipedag.context.run_context import DematerializeRunContext
from pydiverse.pipedag.core.task import Task, TaskGetItem
from pydiverse.pipedag.util import deep_map

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import Materializable


@frozen
class Result:
    underlying: Any
    successful: bool

    def get(self, task: Task | TaskGetItem, as_type: type = None) -> Materializable:
        """Load the results of a task from the database.

        :param task: The task
        :param as_type: The type as which tables produced by this task should be dematerialized.
            If no type is specified, the input type of the task is used.
        :return: The results of the task.
        """
        if as_type is None:
            if isinstance(task, Task):
                as_type = task.input_type
            else:
                as_type = task.task.input_type

        # TODO: Check that the results loaded from the database correspond
        #       to the run_id of this result object.
        with ConfigContext.from_file() as config_ctx, DematerializeRunContext() as run_ctx:

            def dematerialize_mapper(item):
                return config_ctx.store.dematerialize_item(
                    item, as_type=as_type, ctx=run_ctx
                )

            return deep_map(task.value, dematerialize_mapper)
