from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum
from typing import TYPE_CHECKING

from pydiverse.pipedag.context.context import BaseContext

if TYPE_CHECKING:
    from pydiverse.pipedag.backend.lock import LockState
    from pydiverse.pipedag.core import Flow, Stage
    from pydiverse.pipedag.materialise import Blob, Table


class RunContext(ABC, BaseContext):
    _context_var = ContextVar("run_context")

    flow: Flow

    def __init__(self):
        self.run_id = uuid.uuid4().hex[:20]

    # STAGE: REFERENCE COUNTING

    # @abstractmethod
    # def get_stage_ref_count(self, stage: Stage):
    #     ...
    #
    # @abstractmethod
    # def incr_stage_ref_count(self, stage: Stage, by: int = 1) -> int:
    #     ...
    #
    # @abstractmethod
    # def decr_stage_ref_count(self, stage: Stage, by: int = 1) -> int:
    #     ...

    # STAGE: STATE

    # @abstractmethod
    # def get_stage_state(self, stage: Stage) -> StageState:
    #     ...
    #
    # @abstractmethod
    # @contextmanager
    # def init_stage(self, stage: Stage):
    #     ...
    #
    # @abstractmethod
    # @contextmanager
    # def commit_stage(self, stage: Stage):
    #     ...

    # STAGE: LOCK

    @abstractmethod
    def get_stage_lock_state(self, stage: Stage) -> LockState:
        ...

    @abstractmethod
    def set_stage_lock_state(self, stage: Stage, state: LockState):
        ...

    # TABLE / BLOB

    # @abstractmethod
    # def add_names(
    #     self, tables: list[Table], blobs: list[Blob]
    # ) -> tuple[bool, list[Table], list[Blob]]:
    #     ...
    #
    # @abstractmethod
    # def remove_table_names(self, tables: list[Table]):
    #     ...
    #
    # @abstractmethod
    # def remove_blob_names(self, blobs: list[Blob]):
    #     ...


# class StageState(Enum):
#     UNINITIALIZED = 0
#     INITIALIZING = 1
#     READY = 2
#     COMMITTING = 3
#     COMMITTED = 4
#
#     FAILED = 255
