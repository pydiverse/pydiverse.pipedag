from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pydiverse.pipedag import ExternalTableReference, RawSql, Table, Task
from pydiverse.pipedag.core.task import TaskGetItem
from pydiverse.pipedag.util import Disposable

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Result, Subflow


class OrchestrationEngine(Disposable, ABC):
    """Flow orchestration engine base class"""

    @abstractmethod
    def run(
        self,
        flow: Subflow,
        ignore_position_hashes: bool = False,
        inputs: dict[Task | TaskGetItem, ExternalTableReference] | None = None,
        **kwargs,
    ) -> Result:
        """Execute a flow

        :param flow: the pipedag flow to execute
        :param ignore_position_hashes:
            If ``True``, the position hashes of tasks are not checked
            when retrieving the inputs of a task from the cache.
            This simplifies execution of subgraphs if you don't care whether inputs to
            that subgraph are cache invalid. This allows multiple modifications in the
            Graph before the next run updating the cache.
            Attention: This may break automatic cache invalidation.
            And for this to work, any task producing an input
            for the chosen subgraph may never be used more
            than once per stage.
        :param kwargs: Optional keyword arguments. How they get used is
            engine specific.
        :return: A result instance wrapping the flow execution result.
        """


def _replace_task_inputs_with_const_inputs(
    task_inputs: dict[int, Any], inputs: dict[tuple[int, int | str | None], Table]
) -> dict[int, Any]:
    # check if one of the inputs should be replaced with a constant input
    for task_identifier, reference in inputs.items():
        task_id = task_identifier[0]
        if task_id not in task_inputs.keys():
            continue
        task_item = task_identifier[1]
        if isinstance(task_inputs[task_id], Table):
            task_inputs[task_id] = reference
        elif isinstance(task_inputs[task_id], tuple):
            # handling TaskGetItem args
            items = list(task_inputs[task_id])
            items[task_item] = reference
            task_inputs[task_id] = tuple(items)
        elif isinstance(task_inputs[task_id], dict):
            # handling TaskGetItem kwargs and RawSql tasks
            items = task_inputs[task_id]
            items[task_item] = reference
            task_inputs[task_id] = items
        elif isinstance(task_inputs[task_id], RawSql):
            items = task_inputs[task_id]
            if task_inputs[task_id].loaded_tables is None:
                task_inputs[task_id].loaded_tables = {task_item: reference}
            else:
                task_inputs[task_id].loaded_tables[task_item] = reference
            task_inputs[task_id] = items
        else:
            raise TypeError(f"Got unexpected task type: {type(task_inputs[task_id])}")
    return task_inputs
