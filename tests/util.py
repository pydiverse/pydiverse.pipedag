from __future__ import annotations

import pytest
import sqlalchemy as sa
from kazoo.client import KazooClient

import pydiverse.pipedag
from pydiverse.pipedag.backend import (
    FileBlobStore,
    PipeDAGStore,
    SQLTableStore,
    ZooKeeperLockManager,
)


@pytest.fixture(autouse=True, scope="function")
def setup_pipedag():
    engine = sa.create_engine("postgresql://postgres:pipedag@127.0.0.1/pipedag")
    kazoo_client = KazooClient()

    pydiverse.pipedag.config = pydiverse.pipedag.configuration.Config(
        store=PipeDAGStore(
            table=SQLTableStore(engine),
            blob=FileBlobStore("/tmp/pipedag/blobs"),
            lock=ZooKeeperLockManager(kazoo_client),
        )
    )

    yield

    # Teardown
    # -> Reset everything and release any locks still held
    pydiverse.pipedag.config.store._reset()
