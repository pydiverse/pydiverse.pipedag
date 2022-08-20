from __future__ import annotations

from typing import TYPE_CHECKING

from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.core.result import Result
from pydiverse.pipedag.engine.base import Engine

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow


class SequentialEngine(Engine):
    """Execute flow sequentially

    This engine executes all tasks in a flow sequentially in the order
    that they were defined in.
    """

    def run(self, flow: Flow, **run_kwargs):
        run_context = RunContext.get()
        config_context = ConfigContext.get()

        try:
            results = {}
            for task in flow.tasks:
                results[task] = task.run(
                    inputs={
                        in_id: results[in_t] for in_id, in_t in task.input_tasks.items()
                    },
                    run_context=run_context,
                    config_context=config_context,
                )

            return Result(
                underlying=results,
                successful=True,
            )

        except Exception as e:
            return Result(
                underlying=e,
                successful=False,
            )
