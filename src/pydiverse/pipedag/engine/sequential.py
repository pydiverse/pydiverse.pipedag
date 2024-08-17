from __future__ import annotations

from typing import TYPE_CHECKING

from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.core.result import Result
from pydiverse.pipedag.engine.base import OrchestrationEngine

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Subflow


class SequentialEngine(OrchestrationEngine):
    """Most basic orchestration engine that just executes all tasks sequentially."""

    def run(
        self,
        flow: Subflow,
        ignore_position_hashes: bool = False,
        task_links: dict[(str, str, str), (str, str)] | None = None,
        **run_kwargs,
    ):
        run_context = RunContext.get()
        config_context = ConfigContext.get()

        failed_tasks = set()  # type: set[Task]
        results = {}
        exception = None

        try:
            for task in flow.get_tasks():
                try:
                    if not (set(task.input_tasks) & failed_tasks):
                        position_hash = (
                            task.position_hash
                            if hasattr(task, "position_hash")
                            else None
                        )
                        task_link = (
                            None
                            if not task_links
                            else task_links.get(
                                (task.stage.name, task.name, position_hash), None
                            )
                            or task_links.get((task.stage.name, task.name, None), None)
                        )
                        results[task] = task.run(
                            inputs={
                                in_id: results[in_t]
                                for in_id, in_t in task.input_tasks.items()
                                if in_t in results
                            },
                            run_context=run_context,
                            config_context=config_context,
                            ignore_position_hashes=ignore_position_hashes,
                            task_link=task_link,
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
