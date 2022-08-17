from __future__ import annotations

import prefect
from packaging.version import parse as parse_version

if parse_version(prefect.__version__) >= parse_version("2.0"):
    raise ImportError(f"Requires prefect 1.x (found {prefect.__version__}).")

from typing import TYPE_CHECKING, Any

from pydiverse.pipedag.context import ConfigContext, RunContextProxy
from pydiverse.pipedag.engines.base import Engine

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Task


class PrefectEngine(Engine):
    def __init__(
        self,
        flow_kwargs: dict[str, Any] = None,
    ):
        self.flow_kwargs = flow_kwargs or {}

    def construct_workflow(self, f: Flow):
        g = f.explicit_graph
        assert g is not None

        flow = prefect.Flow(f.name, **self.flow_kwargs)
        tasks: dict[Task, prefect.Task] = {}

        run_context = RunContextProxy.get()
        config_context = ConfigContext.get()

        for t in g.nodes:  # type: Task
            task = prefect.task(
                name=t.name,
            )(t.run)
            task._pipedag = t
            tasks[t] = task

            flow.add_task(task)
            flow.set_dependencies(
                task,
                keyword_tasks=dict(
                    inputs={
                        in_id: tasks[in_t] for in_id, in_t in t.input_tasks.items()
                    },
                    run_context=run_context,
                    config_context=config_context,
                ),
            )

        for u, v in g.edges:
            flow.add_edge(tasks[u], tasks[v])

        return flow
