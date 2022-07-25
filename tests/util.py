import pytest
import sqlalchemy as sa
from kazoo.client import KazooClient

import pdpipedag
from pdpipedag.backend import (
    PipeDAGStore,
    SQLTableStore,
    FileBlobStore,
    ZooKeeperLockManager,
)


@pytest.fixture(autouse=True, scope="function")
def setup_pipedag():
    engine = sa.create_engine("postgresql://postgres:pipedag@127.0.0.1/pipedag")
    kazoo_client = KazooClient()

    pdpipedag.config = pdpipedag.configuration.Config(
        store=PipeDAGStore(
            table=SQLTableStore(engine),
            blob=FileBlobStore("/tmp/pipedag/blobs"),
            lock=ZooKeeperLockManager(kazoo_client),
        )
    )

    yield

    # Teardown
    # -> Reset everything and release any locks still held
    pdpipedag.config.store._reset()
