from __future__ import annotations

from typing import TYPE_CHECKING, Any

from attrs import frozen

if TYPE_CHECKING:
    from pydiverse.pipedag._typing import Materializable
    from pydiverse.pipedag.core.task import Task, TaskGetItem


@frozen
class Result:
    underlying: Any
    successful: bool

    def get_result(self, task: Task | TaskGetItem, as_type: type) -> Materializable:
        raise NotImplementedError
