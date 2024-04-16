from __future__ import annotations

from typing import TYPE_CHECKING

from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.core.result import Result
from pydiverse.pipedag.engine.base import OrchestrationEngine

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Subflow


class SequentialEngine(OrchestrationEngine):
    """Most basic orchestration engine that just executes all tasks sequentially."""

    def run(self, flow: Subflow, **run_kwargs):
        run_context = RunContext.get()
        config_context = ConfigContext.get()

        failed_tasks = set()  # type: set[Task]
        results = {}
        exception = None

        try:
            for task in flow.get_tasks():
                try:
                    if not (set(task.input_tasks) & failed_tasks):
                        results[task] = task.run(
                            inputs={
                                in_id: results[in_t]
                                for in_id, in_t in task.input_tasks.items()
                                if in_t in results
                            },
                            run_context=run_context,
                            config_context=config_context,
                        )
                    else:
                        failed_tasks.add(task)
                except Exception as e:
                    if config_context.fail_fast:
                        raise e
                    if config_context._swallow_exceptions:
                        exception = e
                        failed_tasks.add(task)
                    else:
                        raise e

        except Exception as e:
            if config_context.fail_fast:
                raise e
            exception = e

        return Result.init_from(
            subflow=flow,
            underlying=results,
            successful=(exception is None),
            task_values=results,
            exception=exception,
        )
