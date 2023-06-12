import concurrent.futures
import threading
import time
from typing import Callable

from pydiverse.pipedag.backend.lock import BaseLockManager, LockState


def _test_lock_manager(create_lock_manager: Callable[[], BaseLockManager]):
    lock_name = "test_lock"
    sleep_time = 1

    ready_barrier = threading.Barrier(2)
    locked_event = threading.Event()
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
            unlocked_event.set()
        finally:
            done_barrier.wait(timeout=1)

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
                if not unlocked_event.is_set():
                    raise RuntimeError(
                        "Second lock manager was able to acquire lock before the "
                        "first lock manager released it."
                    )

            delta = end_time - start_time
            assert delta >= sleep_time
        finally:
            done_barrier.wait(timeout=1)

            assert lm.get_lock_state(lock_name) == LockState.UNLOCKED
            lm.dispose()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        lm_1_future = executor.submit(lm_1_task)
        lm_2_future = executor.submit(lm_2_task)

        lm_1_future.result()
        lm_2_future.result()


def test_zookeeper():
    from kazoo.client import KazooClient

    from pydiverse.pipedag.backend.lock import ZooKeeperLockManager

    def create_lock_manager():
        # TODO: Don't hardcode ip - Get somehow from the environment
        client = KazooClient(hosts="localhost:2181")
        lock_manager = ZooKeeperLockManager(client, "pipedag/tests/zookeeper/")
        return lock_manager

    _test_lock_manager(create_lock_manager)


def test_filelock():
    import tempfile
    from pathlib import Path

    from pydiverse.pipedag.backend.lock import FileLockManager

    base_path = Path(tempfile.gettempdir()) / "pipedag" / "tests"

    def create_lock_manager():
        lock_manager = FileLockManager(base_path=base_path)
        return lock_manager

    _test_lock_manager(create_lock_manager)
