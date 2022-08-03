from __future__ import annotations

from contextlib import contextmanager

import pytest
import sqlalchemy as sa
from kazoo.client import KazooClient

import pydiverse.pipedag as pdd
from pydiverse.pipedag.backend import (
    FileBlobStore,
    PipeDAGStore,
    SQLTableStore,
    ZooKeeperLockManager,
)

__all__ = [
    "setup_pipedag",
    "settings",
]


@pytest.fixture(autouse=True, scope="function")
def setup_pipedag():
    engine = sa.create_engine("postgresql://postgres:pipedag@127.0.0.1/pipedag")
    kazoo_client = KazooClient()

    pdd.config.name = "pipedag_tests"
    pdd.config.store = PipeDAGStore(
        table=SQLTableStore(engine),
        blob=FileBlobStore("/tmp/pipedag/blobs"),
        lock=ZooKeeperLockManager(kazoo_client),
    )

    yield

    # Teardown
    # -> Reset everything and release any locks still held
    pdd.config.store._reset()


@contextmanager
def settings(**kwargs):
    """Temporarily change a configuration option"""
    original_settings = {k: getattr(pdd.config, k) for k in kwargs.keys()}
    for k, v in kwargs.items():
        setattr(pdd.config, k, v)

    yield

    for k, v in original_settings.items():
        setattr(pdd.config, k, v)
