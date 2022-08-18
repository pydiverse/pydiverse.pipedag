from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow


class Engine(ABC):
    @abstractmethod
    def run(self, flow: Flow, **kwargs):
        ...
