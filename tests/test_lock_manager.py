from __future__ import annotations

import threading
import time
from typing import Callable

import pytest

from pydiverse.pipedag.backend.lock import BaseLockManager, LockState
from tests.fixtures.instances import with_instances


class RaisingThread(threading.Thread):
    def run(self) -> None:
        try:
            super().run()
        except BaseException as e:
            self.exception = e

    def join(self, timeout=None) -> None:
        super().join(timeout)
        if e := getattr(self, "exception", None):
            raise e


def _test_lock_manager(create_lock_manager: Callable[[], BaseLockManager]):
    lock_name = "test_lock"
    sleep_time = 1

    ready_barrier = threading.Barrier(2)
    locked_event = threading.Event()
    sleep_event = threading.Event()
    unlocked_event = threading.Event()
    done_barrier = threading.Barrier(2)

    def lm_1_task():
        lm = create_lock_manager()
        ready_barrier.wait()

        try:
            with lm(lock_name):
                assert lm.get_lock_state(lock_name) == LockState.LOCKED
                locked_event.set()
                time.sleep(sleep_time)
                sleep_event.set()
                time.sleep(0.025)
            unlocked_event.set()
        finally:
            done_barrier.wait(timeout=3)
            assert lm.get_lock_state(lock_name) == LockState.UNLOCKED
            lm.dispose()

    def lm_2_task():
        lm = create_lock_manager()
        ready_barrier.wait()

        locked_event.wait()
        start_time = time.perf_counter()

        try:
            with lm(lock_name):
                assert lm.get_lock_state(lock_name) == LockState.LOCKED
                end_time = time.perf_counter()

                if not sleep_event.is_set() or not unlocked_event.wait(timeout=0.025):
                    raise RuntimeError(
                        "Second lock manager was able to acquire lock before the "
                        "first lock manager released it."
                    )

            delta = end_time - start_time
            assert delta >= sleep_time
        finally:
            done_barrier.wait(timeout=3)
            assert lm.get_lock_state(lock_name) == LockState.UNLOCKED
            lm.dispose()

    t1 = RaisingThread(target=lm_1_task, daemon=True)
    t2 = RaisingThread(target=lm_2_task, daemon=True)

    t1.start()
    t2.start()

    t1.join(timeout=(5 + sleep_time))
    assert not t1.is_alive(), "Thread timed out"

    t2.join(timeout=5)
    assert not t2.is_alive(), "Thread timed out"


@pytest.mark.parallelize
def test_zookeeper():
    from kazoo.client import KazooClient

    from pydiverse.pipedag.backend.lock import ZooKeeperLockManager

    def create_lock_manager():
        # TODO: Don't hardcode ip - Get somehow from the environment
        client = KazooClient(hosts="localhost:2181")
        return ZooKeeperLockManager(client, "pipedag/tests/zookeeper/")

    _test_lock_manager(create_lock_manager)


@pytest.mark.parallelize
def test_filelock():
    import tempfile
    from pathlib import Path

    from pydiverse.pipedag.backend.lock import FileLockManager

    base_path = Path(tempfile.gettempdir()) / "pipedag" / "tests"

    def create_lock_manager():
        return FileLockManager(base_path=base_path)

    _test_lock_manager(create_lock_manager)


@pytest.mark.parallelize
def test_no_lock():
    from pydiverse.pipedag.backend.lock import NoLockManager

    def create_lock_manager():
        return NoLockManager()

    with pytest.raises(BaseException):
        _test_lock_manager(create_lock_manager)
        pytest.fail("No lock manager MUST fail the lock manager tests")


@with_instances("postgres", "mssql", "ibm_db2")
def test_database():
    from pydiverse.pipedag import ConfigContext
    from pydiverse.pipedag.backend.lock import DatabaseLockManager

    config_context = ConfigContext.get()

    def create_lock_manager():
        with config_context.evolve():
            return DatabaseLockManager.init_from_config_context(config_context)

    _test_lock_manager(create_lock_manager)
