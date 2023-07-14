from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog
from attrs import frozen

from pydiverse.pipedag.context import ConfigContext, StageLockContext
from pydiverse.pipedag.context.run_context import DematerializeRunContext, RunContext
from pydiverse.pipedag.core.task import Task, TaskGetItem
from pydiverse.pipedag.errors import LockError

if TYPE_CHECKING:
    import pydot

    from pydiverse.pipedag.context.run_context import FinalTaskState
    from pydiverse.pipedag.core import Flow, Subflow


@frozen
class Result:
    """
    Flow execution result.

    Attributes
    ----------
    flow :
        The flow that produced this result
    underlying :
        The underlying result object returned by the orchestration engine.
        Depending on the engine, this object might have a different type.
    successful :
        Whether the flow execution was successful or not.
    task_values :
        A dictionary mapping from tasks to the values returned by them.
    task_states :
        A dictionary mapping from tasks to their final states.
    exception :
        If an exception was raised during execution, it will get stored in
        this attribute.
    """

    flow: Flow
    subflow: Subflow

    underlying: Any
    successful: bool
    config_context: ConfigContext | None

    task_values: dict[Task, Any]
    task_states: dict[Task, FinalTaskState]
    exception: Exception | None

    @staticmethod
    def init_from(
        *,
        subflow: Subflow,
        underlying: Any,
        successful: bool,
        task_values: dict[Task, Any],
        exception: Exception | None,
    ) -> Result:
        return Result(
            flow=subflow.flow,
            subflow=subflow,
            underlying=underlying,
            successful=successful,
            config_context=ConfigContext.get(),
            task_values=task_values,
            task_states=RunContext.get().get_task_states(),
            exception=exception,
        )

    def get(self, task: Task | TaskGetItem, as_type: type = None) -> Any:
        """Retrieve the output produced by a task.

        Any tables and blobs returned by a task get loaded from their
        corresponding store.

        If :ref:`strict_result_get_locking` is set to True, this call, as well as
        as :py:meth:`Flow.run()` must be wrapped inside a :py:class:`StageLockContext`.

        :param task: The task for which you want to retrieve the output.
        :param as_type: The type as which tables produced by this task should
            be dematerialized. If no type is specified, the input type of
            the task is used.
        :return: The results of the task.
        """
        from pydiverse.pipedag.materialize.store import dematerialize_output_from_store

        if not self.successful:
            logger = structlog.get_logger(logger_name=type(self).__name__)
            logger.warning(
                "Attention: getting tables from unsuccessful run is unreliable!"
            )

        if self.config_context.strict_result_get_locking:
            try:
                StageLockContext.get()
            except LookupError:
                raise LockError(
                    "Called Result.get() without opening StageLockContext. Consider"
                    " using 'strict_result_get_locking: false' for interactive"
                    " debugging"
                ) from None

        if isinstance(task, Task):
            task_output = self.task_values[task]
        else:
            task_output = self.task_values[task.task]

        with self.config_context, DematerializeRunContext(self.flow):
            store = self.config_context.store
            return dematerialize_output_from_store(store, task, task_output, as_type)

    def visualize(self):
        """
        Wrapper for :py:meth:`Flow.visualize()`.
        """
        return self.subflow.visualize(result=self)

    def visualize_url(self) -> str:
        """
        Wrapper for :py:meth:`Flow.visualize_url()`.
        """
        return self.subflow.visualize_url(result=self)

    def visualize_pydot(self) -> pydot.Dot:
        """
        Wrapper for :py:meth:`Flow.visualize_pydot()`.
        """
        return self.subflow.visualize_pydot(result=self)
