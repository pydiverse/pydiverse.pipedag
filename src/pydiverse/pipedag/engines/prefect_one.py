from typing import Any

import prefect
from packaging.version import parse as parse_version

if parse_version(prefect.__version__) >= parse_version("2.0"):
    raise ImportError(f"Requires prefect 1.x (found {prefect.__version__}).")

from pydiverse.pipedag.core.flow import Flow as PPDFlow
from pydiverse.pipedag.core.task import Task as PPDTask
from pydiverse.pipedag.engines.base import Engine


class PrefectOneEngine(Engine):
    def __init__(
        self,
        flow_kwargs: dict[str, Any] = None,
    ):
        self.flow_kwargs = flow_kwargs or {}

    def construct_workflow(self, f: PPDFlow):
        g = f.build_graph()

        flow = prefect.Flow(f.name, **self.flow_kwargs)
        tasks: dict[PPDTask, prefect.Task] = {}

        for t in g.nodes:  # type: PPDTask
            task = prefect.task(
                name=t.name,
                state_handlers=[stage_ref_counter_handler],
            )(t.run)
            task._pipedag = t
            tasks[t] = task
            flow.add_task(task)

        for u, v in g.edges:
            flow.add_edge(tasks[u], tasks[v])

        return flow


def stage_ref_counter_handler(prefect_task, old_state, new_state):
    """Prefect Task state handler to update the stage's reference counter

    For internal use only;
    Decreases the reference counters of all stages associated with a task
    by one once the task has finished running.
    """

    task: PPDTask = prefect_task._pipedag

    if new_state.is_mapped():
        # Is mapping task -> Increment reference counter by the number
        # of child tasks.
        raise NotImplementedError

    if new_state.is_failed():
        run_count = prefect.context.get("task_run_count", 0)
        if run_count <= prefect_task.max_retries:
            # Will retry -> Don't decrement ref counter
            return

    if new_state.is_finished():
        # Did finish task and won't retry -> Decrement ref counter
        if new_state.is_successful():
            task.on_success()
        else:
            task.on_failure()
