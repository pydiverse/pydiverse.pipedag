from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pydiverse.pipedag.util import Disposable

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Result, Subflow


class OrchestrationEngine(Disposable, ABC):
    """Flow orchestration engine base class"""

    @abstractmethod
    def run(self, flow: Subflow, **kwargs) -> Result:
        """Execute a flow

        :param flow: the pipedag flow to execute
        :param kwargs: Optional keyword arguments. How they get used is
            engine specific.
        :return: A result instance wrapping the flow execution result.
        """
