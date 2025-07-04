# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from pydiverse.pipedag import ExternalTableReference, Table, Task
from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.core import Subflow
from pydiverse.pipedag.core.result import Result
from pydiverse.pipedag.engine.base import (
    OrchestrationEngine,
)


class SequentialEngine(OrchestrationEngine):
    """Most basic orchestration engine that just executes all tasks sequentially."""

    def run(
        self,
        flow: Subflow,
        ignore_position_hashes: bool = False,
        inputs: dict[Task, ExternalTableReference] | None = None,
        **run_kwargs,
    ):
        run_context = RunContext.get()
        config_context = ConfigContext.get()

        failed_tasks = set()  # type: set[Task]
        results = {}
        exception = None
        inputs = inputs if inputs is not None else {}

        try:
            for task in flow.get_tasks():
                try:
                    if not (set(task.input_tasks) & failed_tasks):
                        task_inputs = {
                            **{
                                in_id: results[in_t]
                                for in_id, in_t in task.input_tasks.items()
                                if in_t in results and in_t not in inputs
                            },
                            **{
                                in_id: Table(inputs[in_t]) for in_id, in_t in task.input_tasks.items() if in_t in inputs
                            },
                        }

                        results[task] = task.run(
                            inputs=task_inputs,
                            run_context=run_context,
                            config_context=config_context,
                            ignore_position_hashes=ignore_position_hashes,
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
