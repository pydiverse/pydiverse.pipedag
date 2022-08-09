from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Result


class Engine(ABC):
    """Flow execution engine base class"""

    @abstractmethod
    def run(self, flow: Flow, **kwargs) -> Result:
        """Execute a flow

        :param flow: the pipedag flow to execute
        :param kwargs: Optional keyword arguments. How they get used is
            engine specific.
        :return: A result instance wrapping the flow execution result.
        """
