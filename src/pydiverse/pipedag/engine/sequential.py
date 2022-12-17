from __future__ import annotations

from typing import TYPE_CHECKING

from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.core.result import Result
from pydiverse.pipedag.engine.base import OrchestrationEngine

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow


class SequentialEngine(OrchestrationEngine):
    """Execute flow sequentially

    This engine executes all tasks in a flow sequentially in the order
    that they were defined in.
    """

    def run(self, flow: Flow, **run_kwargs):
        run_context = RunContext.get()
        config_context = ConfigContext.get()

        results = {}
        exception = None

        try:
            for task in flow.tasks:
                results[task] = task.run(
                    inputs={
                        in_id: results[in_t] for in_id, in_t in task.input_tasks.items()
                    },
                    run_context=run_context,
                    config_context=config_context,
                )

        except Exception as e:
            if run_kwargs.get("fail_fast", False):
                raise e
            exception = e

        return Result(
            underlying=results,
            successful=(exception is None),
            config_context=ConfigContext.get(),
            task_values=results,
            exception=exception,
        )
