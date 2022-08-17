from __future__ import annotations

import functools
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum
from threading import Lock
from typing import TYPE_CHECKING

from pydiverse.pipedag.context.context import BaseContext, ConfigContext
from pydiverse.pipedag.errors import StageError
from pydiverse.pipedag.util.ipc import IPCServer

if TYPE_CHECKING:
    from pydiverse.pipedag import Blob, Flow, Stage, Table
    from pydiverse.pipedag._typing import T
    from pydiverse.pipedag.backend import LockState


def synchronized(lock_attr: str):
    def decorator(func: T) -> T:
        @functools.wraps(func)
        def synced_func(self, *args, **kwargs):
            with getattr(self, lock_attr):
                return func(self, *args, **kwargs)

        return synced_func

    return decorator


class SetEvent(threading.Event):
    """Just like threading.Event except its initial state is set"""

    def __init__(self):
        super().__init__()
        self.set()


class RunContextServer(IPCServer):
    def __init__(self, flow: Flow):
        super().__init__()

        from pydiverse.pipedag.backend import LockState

        self.flow = flow
        self.run_id = uuid.uuid4().hex[:20]

        self.stages = list(self.flow.stages.values())
        self.stages.sort(key=lambda s: s.id)

        num_stages = len(self.stages)

        # STATE
        self.ref_count = [0] * num_stages
        self.stage_state = [StageState.UNINITIALIZED] * num_stages

        self.table_names = [set() for _ in range(num_stages)]
        self.blob_names = [set() for _ in range(num_stages)]

        # STATE LOCKS
        self.ref_count_lock = Lock()
        self.stage_state_lock = Lock()
        self.names_lock = Lock()

        # LOCKING
        # self.stage_lock_certain_event = [SetEvent() for _ in range(num_stages)]
        # self.stage_lock_state_lock = Lock()

        self.lock_manager = ConfigContext.get().get_lock_manager()
        self.lock_manager.add_lock_state_listener(self._lock_state_listener)

        # TODO: Initialize backend metadata stuff

    def __enter__(self):
        super().__enter__()
        self.__context_proxy = RunContextProxy(self)
        self.__context_proxy.__enter__()
        return self.__context_proxy

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._release_all_locks()
        self.__context_proxy.__exit__(exc_type, exc_val, exc_tb)
        super().__exit__(exc_type, exc_val, exc_tb)

    def handle_request(self, request):
        op_name = request["op"]
        op = getattr(self, op_name)

        if not callable(op):
            raise TypeError(f"OP '{op_name}' is not callable")

        try:
            args = request.get("args", ())
            kwargs = request.get("kwargs", {})
            result = op(*args, **kwargs)
            return {"status": 0, "op": op_name, "result": result}
        except Exception as e:
            return {"status": -1, "op": op_name, "message": repr(e)}

    # STAGE: Reference Counting

    @synchronized("ref_count_lock")
    def get_stage_ref_count(self, stage_id: int):
        return self.ref_count[stage_id]

    @synchronized("ref_count_lock")
    def incr_stage_ref_count(self, stage_id: int, by: int = 1):
        self.ref_count[stage_id] += by
        return self.ref_count[stage_id]

    @synchronized("ref_count_lock")
    def decr_stage_ref_count(self, stage_id: int, by: int = 1):
        self.ref_count[stage_id] -= by
        rc = self.ref_count[stage_id]
        if rc == 0:
            self.lock_manager.release(self.stages[stage_id])
        if rc < 0:
            self.logger.error(
                "Reference counter is negative.",
                reference_count=rc,
                stage=self.stages[stage_id],
            )
        return rc

    # STAGE: State

    @synchronized("stage_state_lock")
    def get_stage_state(self, stage_id: int):
        return self.stage_state[stage_id].value

    def enter_init_stage(self, stage_id: int):
        return self._enter_stage_state_transition(
            stage_id,
            StageState.UNINITIALIZED,
            StageState.INITIALIZING,
            StageState.READY,
        )

    def exit_init_stage(self, stage_id: int, success: bool):
        self._exit_stage_state_transition(
            stage_id,
            success,
            StageState.INITIALIZING,
            StageState.READY,
        )

    def enter_commit_stage(self, stage_id: int):
        return self._enter_stage_state_transition(
            stage_id,
            StageState.READY,
            StageState.COMMITTING,
            StageState.COMMITTED,
        )

    def exit_commit_stage(self, stage_id: int, success: bool):
        self._exit_stage_state_transition(
            stage_id,
            success,
            StageState.COMMITTING,
            StageState.COMMITTED,
        )

    def _enter_stage_state_transition(
        self,
        stage_id: int,
        from_: StageState,
        transition: StageState,
        to: StageState,
    ):
        with self.stage_state_lock:
            # Already in correct state
            if self.stage_state[stage_id] == to:
                return False

            # Try to begin transition
            if self.stage_state[stage_id] == from_:
                self.stage_state[stage_id] = transition
                return True

        # Wait until transition is over
        while self.stage_state[stage_id] == transition:
            time.sleep(0.001)

        # Check if transition was successful
        with self.stage_state_lock:
            state = self.stage_state[stage_id]

            if state == to:
                return False
            if state == StageState.FAILED:
                raise StageError(
                    f"Stage '{self.stages[stage_id].name}' is in unexpected state"
                    f" {state}"
                )

    def _exit_stage_state_transition(
        self,
        stage_id: int,
        success: bool,
        transition: StageState,
        to: StageState,
    ):
        if not success:
            with self.stage_state_lock:
                self.stage_state[stage_id] = StageState.FAILED
                return

        with self.stage_state_lock:
            if self.stage_state[stage_id] == transition:
                self.stage_state[stage_id] = to
            else:
                raise RuntimeError

    # LOCKING

    def _lock_state_listener(
        self, stage: Stage, old_state: LockState, new_state: LockState
    ):
        """Internal listener that gets notified when the state of a lock changes"""
        from pydiverse.pipedag.backend import LockState
        from pydiverse.pipedag.core import Stage

        if not isinstance(stage, Stage):
            return

        # Logging
        if new_state == LockState.UNCERTAIN:
            self.logger.warning(
                f"Lock for stage '{stage.name}' transitioned to UNCERTAIN state."
            )
        if old_state == LockState.UNCERTAIN and new_state == LockState.LOCKED:
            self.logger.info(
                f"Lock for stage '{stage.name}' is still LOCKED (after being"
                " UNCERTAIN)."
            )
        if old_state == LockState.UNCERTAIN and new_state == LockState.INVALID:
            self.logger.error(f"Lock for stage '{stage.name}' has become INVALID.")

    def _release_all_locks(self):
        locks = list(self.lock_manager.lock_states.items())
        for lock, state in locks:
            self.lock_manager.release(lock)

    def acquire_stage_lock(self, stage_id: int):
        stage = self.stages[stage_id]
        self.lock_manager.acquire(stage)

    def release_stage_lock(self, stage_id: int):
        stage = self.stages[stage_id]
        self.lock_manager.release(stage)

    # @synchronized("stage_lock_state_lock")
    # def set_stage_lock_state(self, stage_id: int, state: int) -> int:
    #     from pydiverse.pipedag.backend.lock import LockState
    #
    #     old_state = self.stage_lock_state[stage_id]
    #     self.stage_lock_state[stage_id] = LockState(state)
    #     return old_state.value

    # @synchronized("stage_lock_state_lock")
    # def wait_stage_lock_state_certain(self, stage_id: int) -> int:
    #     self.stage_lock_certain_event[stage_id].wait()

    # TABLE / BLOB: Names

    @synchronized("names_lock")
    def add_names(self, stage_id: int, tables: list[str], blobs: list[str]):
        table_duplicates = []
        blob_duplicates = []

        for table in tables:
            if table in self.table_names[stage_id]:
                table_duplicates.append(table)
        for blob in blobs:
            if blob in self.blob_names[stage_id]:
                blob_duplicates.append(blob)

        if table_duplicates or blob_duplicates:
            return {
                "success": False,
                "table_duplicates": table_duplicates,
                "blob_duplicates": blob_duplicates,
            }

        for table in tables:
            self.table_names[stage_id].add(table)
        for blob in blobs:
            self.blob_names[stage_id].add(blob)

        return {
            "success": True,
        }

    @synchronized("names_lock")
    def remove_names(self, stage_id: int, tables: list[str], blobs: list[str]):
        for table in tables:
            self.table_names[stage_id].remove(table)
        for blob in blobs:
            self.blob_names[stage_id].remove(blob)

    # TASK: Memo

    ...


class RunContextProxy(BaseContext):
    _context_var = ContextVar("run_context_proxy")

    def __init__(self, server: RunContextServer):
        self.client = server.get_client()
        self.flow = server.flow
        self.run_id = server.run_id

    def _request(self, op: str, *args, **kwargs):
        response = self.client.request(
            {
                "op": op,
                "args": args,
                "kwargs": kwargs,
            }
        )

        if response["status"] != 0:
            raise Exception(response["message"])
        return response["result"]

    # STAGE: Reference Counting

    def get_stage_ref_count(self, stage: Stage) -> int:
        return self._request("get_stage_ref_count", stage.id)

    def incr_stage_ref_count(self, stage: Stage, by: int = 1) -> int:
        return self._request("incr_stage_ref_count", stage.id, by)

    def decr_stage_ref_count(self, stage: Stage, by: int = 1) -> int:
        return self._request("decr_stage_ref_count", stage.id, by)

    # STAGE: State

    def get_stage_state(self, stage: Stage) -> StageState:
        value = self._request("get_stage_state", stage.id)
        return StageState(value)

    @contextmanager
    def init_stage(self, stage: Stage):
        can_continue = self._request("enter_init_stage", stage.id)
        if not can_continue:
            yield False
            return

        try:
            yield True
        except Exception as e:
            # Failed initializing stage
            self._request("exit_init_stage", stage.id, False)
            raise StageError from e
        else:
            self._request("exit_init_stage", stage.id, True)

    @contextmanager
    def commit_stage(self, stage: Stage):
        can_continue = self._request("enter_commit_stage", stage.id)
        if not can_continue:
            yield False
            return

        try:
            yield True
        except Exception as e:
            # Failed committing stage
            self._request("exit_commit_stage", stage.id, False)
            raise StageError from e
        else:
            self._request("exit_commit_stage", stage.id, True)

    # STAGE: Lock

    def acquire_stage_lock(self, stage: Stage):
        self._request("acquire_stage_lock", stage.id)

    def release_stage_lock(self, stage: Stage):
        self._request("release_stage_lock", stage.id)

    # TABLE / BLOB: Names

    def add_names(
        self, stage: Stage, tables: list[Table], blobs: list[Blob]
    ) -> tuple[bool, tuple[str, ...], tuple[str, ...]]:
        response = self._request(
            "add_names",
            stage.id,
            [t.name for t in tables],
            [b.name for b in blobs],
        )

        if response["success"]:
            return True, (), ()
        else:
            return False, response["table_duplicates"], response["blob_duplicates"]

    def remove_names(self, stage: Stage, tables: list[Table], blobs: list[Blob]):
        self._request(
            "remove_names",
            stage.id,
            [t.name for t in tables],
            [b.name for b in blobs],
        )

    # TASK: Memo

    ...


class StageState(Enum):
    UNINITIALIZED = 0
    INITIALIZING = 1
    READY = 2
    COMMITTING = 3
    COMMITTED = 4

    FAILED = 255
