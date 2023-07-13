from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

import structlog
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from pydiverse.pipedag.context import ConfigContext, RunContext
from pydiverse.pipedag.core.result import Result
from pydiverse.pipedag.engine.base import OrchestrationEngine
from pydiverse.pipedag.util import requires

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Subflow, Task

try:
    import prefect

    prefect_version = Version(prefect.__version__)
except ImportError as e:
    warnings.warn(str(e), ImportWarning)

    prefect = None
    prefect_version = Version("0")


@requires(prefect, ImportError("Module 'prefect' not installed"))
@requires(
    prefect_version in SpecifierSet("~=1.0"),
    ImportWarning(f"Requires prefect version 1.x (found {prefect_version})"),
)
class PrefectOneEngine(OrchestrationEngine):
    """
    Hands over execution of a flow to `Prefect 1 <https://docs-v1.prefect.io>`_.

    :param flow_kwargs:
        Optional dictionary of keyword arguments that get passed to the
        initializer of |prefect1.Flow|_.

    .. |prefect1.Flow| replace:: ``prefect.Flow``
    .. _prefect1.Flow: https://docs-v1.prefect.io/api/latest/core/flow.html
    """

    def __init__(self, flow_kwargs: dict[str, Any] = None):
        self.flow_kwargs = flow_kwargs or {}
        self.logger = structlog.get_logger(stage=self)

    def construct_prefect_flow(self, f: Subflow):
        run_context = RunContext.get()
        config_context = ConfigContext.get()

        flow_kwargs = {
            "name": f.name,
            **self.flow_kwargs,
        }

        flow = prefect.Flow(**flow_kwargs)
        tasks: dict[Task, prefect.Task] = {}

        for t in f.get_tasks():
            task = prefect.task(name=t.name)(t.run)
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

        for task in f.get_tasks():
            for parent in f.get_parent_tasks(task):
                flow.add_edge(tasks[parent], tasks[task])

        project_name = config_context.pipedag_name + "-" + config_context.instance_id
        try:
            flow.register(project_name=project_name)
        except ValueError as _e:
            self.logger.warning(f"Please make sure project {project_name} exists: {_e}")

        return flow, tasks

    def run(self, flow: Subflow, **run_kwargs):
        prefect_flow, tasks_map = self.construct_prefect_flow(flow)
        result = prefect_flow.run(**run_kwargs)

        # Compute task_values
        task_values = {}
        for task, prefect_task in tasks_map.items():
            task_values[task] = result.result[prefect_task].result

        # If the task failed, extract the exception
        exception = None
        if result.is_failed():
            for task_res in result.result.values():
                if task_res.is_failed() and isinstance(task_res.result, Exception):
                    exception = task_res.result
                    break
            else:
                # Generic Fallback
                exception = Exception(
                    f"Prefect run failed with message: {result.message}"
                )

        return Result.init_from(
            subflow=flow,
            underlying=result,
            successful=result.is_successful(),
            task_values=task_values,
            exception=exception,
        )


@requires(prefect, ImportError("Module 'prefect' not installed"))
@requires(
    prefect_version in SpecifierSet("~=2.0"),
    ImportWarning(f"Requires prefect version 2.x (found {prefect_version})"),
)
class PrefectTwoEngine(OrchestrationEngine):
    """
    Hands over execution of a flow to `Prefect 2 <https://docs.prefect.io>`_.

    :param flow_kwargs:
        Optional dictionary of keyword arguments that get passed to the
        initializer of |@prefect2.flow|_ deecorator.

    .. |@prefect2.flow| replace:: ``@prefect.flow``
    .. _@prefect2.flow:
            https://docs.prefect.io/latest/api-ref/prefect/flows/#prefect.flows.flow
    """

    def __init__(self, flow_kwargs: dict[str, Any] = None):
        self.flow_kwargs = flow_kwargs or {}

    def construct_prefect_flow(self, f: Subflow) -> prefect.Flow:
        from pydiverse.pipedag.materialize.core import MaterializingTask

        run_context = RunContext.get()
        config_context = ConfigContext.get()

        flow_kwargs = {
            "name": f.name,
            "validate_parameters": False,
            **self.flow_kwargs,
        }

        @prefect.flow(**flow_kwargs)
        def pipedag_flow():
            futures: dict[Task, prefect.futures.PrefectFuture] = {}

            for t in f.get_tasks():
                task_kwargs = {"name": t.name}
                if isinstance(t, MaterializingTask):
                    task_kwargs["version"] = t.version

                task = prefect.task(**task_kwargs)(t.run)

                parents = [futures[p] for p in f.get_parent_tasks(t)]
                inputs = {in_id: futures[in_t] for in_id, in_t in t.input_tasks.items()}
                futures[t] = task.submit(
                    inputs=inputs,
                    run_context=run_context,
                    config_context=config_context,
                    wait_for=parents,
                )

            return futures

        return pipedag_flow

    def run(self, flow: Subflow, **kwargs):
        if kwargs:
            raise TypeError(f"{type(self).__name__}.run doesn't take kwargs.")
        prefect_flow = self.construct_prefect_flow(flow)
        result = prefect_flow(return_state=True)

        # Compute task_values
        task_values = {}
        successful = result.is_completed()

        for task, state in result.result().items():
            if state.is_completed():
                task_values[task] = state.result()
            else:
                successful = False

        # If the task failed, extract the exception
        exception = None
        if not successful:
            for state in result.result().values():
                if state.is_failed() or state.is_crashed():
                    exception = prefect.states.get_state_exception(state)
                    break

        return Result.init_from(
            subflow=flow,
            underlying=result,
            successful=successful,
            task_values=task_values,
            exception=exception,
        )


# Automatic Prefect Version Selection


if prefect_version in SpecifierSet("~=1.0"):
    PrefectEngine = PrefectOneEngine
elif prefect_version in SpecifierSet("~=2.0"):
    PrefectEngine = PrefectTwoEngine
else:
    from abc import ABC

    @requires(prefect, ImportWarning("Module 'prefect' not installed"))
    @requires(False, ImportWarning(f"Incompatible prefect version {prefect_version}"))
    class PrefectEngine(OrchestrationEngine, ABC):
        pass
