from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pydiverse.pipedag import ExternalTableReference, Task
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
