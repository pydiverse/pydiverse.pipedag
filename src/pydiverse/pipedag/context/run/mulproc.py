# https://github.com/ronny-rentner/UltraDict
# https://pypi.org/project/atomics/
# https://docs.python.org/3/library/multiprocessing.shared_memory.html#multiprocessing.shared_memory.ShareableList
# https://docs.python.org/3/library/multiprocessing.shared_memory.html#multiprocessing.managers.SharedMemoryManager


# Things I Need:
#
# - FOR REFERENCE COUNTING:
#   - Atomic increment and decrement
#   - A way to watch a value and call a function if it hits 0
#
# - FOR SCHEMA LOCKING:
#   - Atomic bit flag if my schema has been created or not
#   - ...
#
# - GENERAL GLOBAL STATE:
#   - Set of table names ?
#   - Set of blob names ?
#
#     Maybe just a large shared memory buffer (16MB should be enough)
#     Implement the set behaviour using the md5 hash of the table / blob name
#

from __future__ import annotations

import atexit
import struct
import time
from contextlib import contextmanager
from hashlib import sha256
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.shared_memory import SharedMemory
from typing import TYPE_CHECKING, Iterable

import atomics

from pydiverse.pipedag.context.run.base import RunContext, StageState
from pydiverse.pipedag.errors import StageError

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Flow, Stage
    from pydiverse.pipedag.materialise import Blob, Table


class MultiProcManager:
    def __init__(self, hash_set_size=2**20):
        self.hash_set_size = hash_set_size

    @contextmanager
    def __call__(self, flow: Flow):
        with SharedMemoryManager() as smm:
            requested_size = 2 * 8 * len(flow.stages)
            stage_shm = smm.SharedMemory(requested_size)

            table_shs = SharedHashSet.using_smm(smm, self.hash_set_size)
            blob_shs = SharedHashSet.using_smm(smm, self.hash_set_size)

            handle = MultiProcRunContext(
                flow=flow,
                num_stages=len(flow.stages),
                stage_shm=stage_shm,
                table_shs=table_shs,
                blob_shs=blob_shs,
            )

            with handle:
                yield handle


class MultiProcRunContext(RunContext):
    def __init__(
        self,
        flow: Flow,
        num_stages: int,
        stage_shm: SharedMemory,
        table_shs: SharedHashSet,
        blob_shs: SharedHashSet,
    ):
        super().__init__()

        self.flow = flow
        self._num_stages = num_stages

        self._stage_shm = stage_shm
        self._table_shs = table_shs
        self._blob_shs = blob_shs

    # STAGE REFERENCE COUNTING

    def get_stage_ref_count(self, stage: Stage):
        offset = stage.stage_id * 8
        with atomics.atomicview(
            buffer=self._stage_shm.buf[offset : offset + 8],
            atype=atomics.INT,
        ) as a:
            return a.load()

    def incr_stage_ref_count(self, stage: Stage, by: int = 1) -> int:
        offset = stage.stage_id * 8
        with atomics.atomicview(
            buffer=self._stage_shm.buf[offset : offset + 8],
            atype=atomics.INT,
        ) as a:
            if by == 1:
                return a.fetch_inc() + 1
            else:
                return a.fetch_add(by) + by

    def decr_stage_ref_count(self, stage: Stage, by: int = 1) -> int:
        offset = stage.stage_id * 8
        with atomics.atomicview(
            buffer=self._stage_shm.buf[offset : offset + 8],
            atype=atomics.INT,
        ) as a:
            if by == 1:
                return a.fetch_dec() - 1
            else:
                return a.fetch_sub(by) - by

    # STAGE STATES

    def get_stage_state(self, stage: Stage) -> StageState:
        offset = (self._num_stages + stage.stage_id) * 8
        with atomics.atomicview(
            buffer=self._stage_shm.buf[offset : offset + 8],
            atype=atomics.INT,
        ) as a:
            return StageState(a.load())

    def init_stage(self, stage: Stage):
        return self._wait_for_state_change(
            stage,
            StageState.UNINITIALIZED,
            StageState.INITIALIZING,
            StageState.READY,
        )

    def commit_stage(self, stage: Stage):
        return self._wait_for_state_change(
            stage,
            StageState.READY,
            StageState.COMMITTING,
            StageState.COMMITTED,
        )

    @contextmanager
    def _wait_for_state_change(
        self, stage: Stage, from_: StageState, transition: StageState, to: StageState
    ):
        offset = (self._num_stages + stage.stage_id) * 8
        with atomics.atomicview(
            buffer=self._stage_shm.buf[offset : offset + 8],
            atype=atomics.INT,
        ) as a:
            success = a.cmpxchg_strong(
                expected=from_.value,
                desired=transition.value,
            )

            if not success:
                # Wait until transition done
                while a.load() == transition.value:
                    time.sleep(0.0001)
                if a.load() == StageState.FAILED.value:
                    raise StageError

                yield False
                return

            try:
                yield True
                if not a.cmpxchg_strong(
                    expected=transition.value,
                    desired=to.value,
                ):
                    raise RuntimeError

            except Exception as e:
                # Failed creating stage
                a.store(StageState.FAILED.value)
                raise StageError from e

    # TABLE / BLOB

    @staticmethod
    def _name_to_id(obj: Table | Blob) -> str:
        return f"STAGE: {obj.stage.name} -*- NAME: {obj.name}"

    def add_names(self, tables: list[Table], blobs: list[Blob]):
        table_duplicates = []
        blob_duplicates = []

        with self._table_shs._lock, self._blob_shs._lock:
            for table in tables:
                if self._table_shs._contains(self._name_to_id(table)):
                    table_duplicates.append(table)

            for blob in blobs:
                if self._blob_shs._contains(self._name_to_id(blob)):
                    blob_duplicates.append(blob)

            success = not table_duplicates and not blob_duplicates
            if success:
                for table in tables:
                    if not self._table_shs._add(self._name_to_id(table)):
                        raise RuntimeError

                for blob in blobs:
                    if not self._blob_shs._add(self._name_to_id(blob)):
                        raise RuntimeError

            return success, table_duplicates, blob_duplicates

    def remove_table_names(self, tables):
        self._table_shs.remove_bulk([self._name_to_id(table) for table in tables])

    def remove_blob_names(self, blobs):
        self._blob_shs.remove_bulk([self._name_to_id(blob) for blob in blobs])

    # TASK MEMO

    ...


class SharedHashSet:
    """
    Linear Probing based hash set:
    https://en.wikipedia.org/wiki/Linear_probing
    """

    _alignment = 8

    _mask = 0x7FFFFFFFFFFFFFFF
    _tombstone = 0x8000000000000000

    _header_offset = 8  # 8 padding bytes for lock
    _header_format = "QQ"  # uint64 hash set size | uint64 num elements

    def __init__(self, size: int = None, *, name: str = None):
        self._hash_offset = self._header_offset + struct.calcsize(self._header_format)

        if name is None or size is not None:
            requested_size = self._hash_offset + (8 * size)
            self.shm = SharedMemory(name, create=True, size=requested_size)
        else:
            self.shm = SharedMemory(name)

        if size is not None:
            struct.pack_into(
                self._header_format,
                self.shm.buf,
                self._header_offset,
                size,
                0,
            )

            self._len = size
        else:
            self._len = struct.unpack_from(
                self._header_format,
                self.shm.buf,
                self._header_offset,
            )[0]

        self._lock = SharedLock(self.shm, 0)

    @classmethod
    def using_smm(cls, manager: SharedMemoryManager, size: int):
        # noinspection PyUnresolvedReferences, PyProtectedMember
        with manager._Client(manager._address, authkey=manager._authkey) as conn:
            shs = cls(size=size)
            try:
                from multiprocessing.managers import dispatch

                dispatch(conn, None, "track_segment", (shs.shm.name,))
            except BaseException as e:
                shs.shm.unlink()
                raise e
        return shs

    def __len__(self):
        with self._lock:
            return struct.unpack_from(
                self._header_format, self.shm.buf, self._header_offset
            )[1]

    def __contains__(self, v: str):
        with self._lock:
            return self._contains(v)

    def add(self, v: str):
        with self._lock:
            self._add(v)

    def add_bulk(self, it: Iterable[str]):
        with self._lock:
            for v in it:
                self._add(v)

    def remove(self, v: str):
        with self._lock:
            self._remove(v)

    def remove_bulk(self, it: Iterable[str]):
        with self._lock:
            for v in it:
                self._remove(v)

    def _contains(self, v: str) -> bool:
        key = self._hash(v)
        found, _ = self._find(key)
        return found

    def _add(self, v: str) -> bool:
        key = self._hash(v)
        found, found_idx = self._find(key, skip_tombstones=False)
        if not found:
            self._set(found_idx, key)

            size, count = struct.unpack_from(
                self._header_format, self.shm.buf, self._header_offset
            )
            struct.pack_into(
                self._header_format, self.shm.buf, self._header_offset, size, count + 1
            )
        return not found

    def _remove(self, v: str):
        # https://en.wikipedia.org/wiki/Linear_probing#Deletion
        key = self._hash(v)
        found, found_idx = self._find(key)
        if not found:
            raise KeyError(f"Value '{v}' not found in hash set.")

        # Insert tombstone
        self._set(found_idx, self._tombstone)

        size, count = struct.unpack_from(
            self._header_format, self.shm.buf, self._header_offset
        )
        struct.pack_into(
            self._header_format, self.shm.buf, self._header_offset, size, count - 1
        )

    def _find(self, k: int, skip_tombstones=True) -> tuple[bool, int]:
        start_idx = k % self._len

        for i in range(0, self._len):
            val = self._get(start_idx + i)
            if val == k:
                return True, start_idx + i

            if val == 0:
                return False, start_idx + i

            if not skip_tombstones and val == self._tombstone:
                return False, start_idx + i

        raise MemoryError("Out of memory")

    def _set(self, idx: int, val: int):
        length = self._len
        if idx >= length:
            idx -= length
            if idx >= length:
                idx %= length
        elif idx < 0:
            idx += length
            if idx < 0:
                idx %= length

        struct.pack_into(
            "Q",
            self.shm.buf,
            self._hash_offset + (idx * self._alignment),
            val,
        )

    def _get(self, idx: int) -> int:
        length = self._len
        if idx >= length:
            idx -= length
            if idx >= length:
                idx %= length
        elif idx < 0:
            idx += length
            if idx < 0:
                idx %= length

        return struct.unpack_from(
            "Q",
            self.shm.buf,
            self._hash_offset + (idx * self._alignment),
        )[0]

    def _hash(self, v: str) -> int:
        assert isinstance(v, str)

        digest = sha256(v.encode()).digest()
        key = int.from_bytes(digest[:8], byteorder="little", signed=False)
        key = key & self._mask

        assert key != 0
        return key


class SharedLock:
    # Maybe check out https://en.wikipedia.org/wiki/Mutual_exclusion#Software_solutions
    # Might have better performance
    # -> Szymanski's algorithm
    # -> https://www.technovelty.org/python/half-baked-python-mutex.html

    def __init__(self, shm: SharedMemory, offset: int):
        self.shm = shm
        self.offset = offset
        self._has_lock = False
        self._setup_atomic()

    def _setup_atomic(self):
        assert not hasattr(self, "_ctx")

        self._ctx = atomics.atomicview(
            buffer=self.shm.buf[self.offset : self.offset + 1],
            atype=atomics.UINT,
        )
        self._view = self._ctx.__enter__()

        atexit.register(lambda: self._ctx.__exit__(None, None, None))
        atexit.register(lambda: self.release(try_=True))

    def acquire(self):
        while True:
            if self._view.cmpxchg_weak(expected=0, desired=1):
                self._has_lock = True
                return

    def release(self, try_=False):
        if not self._has_lock:
            if try_:
                return
            raise RuntimeError("Can't release unlocked lock")

        if not self._view.cmpxchg_strong(expected=1, desired=0):
            raise RuntimeError("Lock has unexpectedly been released already")
        self._has_lock = False

    def locked(self):
        return self._has_lock

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __getstate__(self):
        state = self.__dict__.copy()
        state["has_lock"] = False
        del state["_ctx"]
        del state["_view"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._setup_atomic()
