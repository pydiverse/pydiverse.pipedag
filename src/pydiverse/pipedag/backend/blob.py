from __future__ import annotations

import os
import pickle
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydiverse.pipedag.context import ConfigContext
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.util import Disposable, normalize_name

if TYPE_CHECKING:
    from pydiverse.pipedag.core import Stage
    from pydiverse.pipedag.materialize import Blob

__all__ = [
    "BaseBlobStore",
    "FileBlobStore",
]


class BaseBlobStore(Disposable, ABC):
    """Blob store base class

    A blob (binary large object) store is responsible for storing arbitrary
    python objects. This can, for example, be done by serializing them using
    the python ``pickle`` module.

    A store must use a blob's name (``Blob.name``) and stage (``Blob.stage``)
    as the primary keys for storing and retrieving blobs. This means that
    two different ``Blob`` objects can be used to store and retrieve the same
    data as long as they have the same name and stage.
    """

    @abstractmethod
    def init_stage(self, stage: Stage):
        """Initialize a stage and start a transaction"""

    @abstractmethod
    def commit_stage(self, stage: Stage):
        """Commit the stage transaction

        Replace the blobs of the base stage with the blobs in the transaction.
        """

    @abstractmethod
    def store_blob(self, blob: Blob):
        """Stores a blob in the associated stage transaction"""

    @abstractmethod
    def copy_blob_to_transaction(self, blob: Blob):
        """Copy a blob from the base stage to the transaction

        This operation MUST not remove the blob from the base stage or modify
        it in any way.
        """

    @abstractmethod
    def delete_blob_from_transaction(self, blob: Blob):
        """Delete a blob from the transaction

        If the blob doesn't exist in the transaction, fail silently.
        """

    @abstractmethod
    def retrieve_blob(self, blob: Blob) -> Any:
        """Loads a blob from the store

        Retrieves the stored python object from the store and returns it.
        If the stage hasn't yet been committed, the blob must be retrieved
        from the transaction, else it must be retrieved from the committed
        stage.
        """


class FileBlobStore(BaseBlobStore):
    """File based blob store

    The ``FileBlobStore`` stores blobs in a folder structure on a file system.
    In the base directory there will be two folders for every stage, one
    for the base and one for the transaction stage. Inside those folders the
    blobs will be stored as pickled files:
    ``base_path/instance_id/STAGE_NAME/BLOB_NAME.pkl``.

    To commit a stage, the only thing that has to be done is to rename
    the appropriate folders.
    """

    @classmethod
    def _init_conf_(cls, config: dict[str, Any]):
        instance_id = normalize_name(ConfigContext.get().instance_id)
        base_path = Path(config["base_path"]) / instance_id
        return cls(base_path)

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path).absolute()
        os.makedirs(self.base_path, exist_ok=True)

    def init_stage(self, stage: Stage):
        stage_path = self.get_stage_path(stage.name)
        transaction_path = self.get_stage_path(stage.transaction_name)

        try:
            os.mkdir(stage_path)
        except FileExistsError:
            pass

        try:
            os.mkdir(transaction_path)
        except FileExistsError:
            shutil.rmtree(transaction_path)
            os.mkdir(transaction_path)

    def commit_stage(self, stage: Stage):
        stage_path = self.get_stage_path(stage.name)
        transaction_path = self.get_stage_path(stage.transaction_name)
        tmp_path = self.get_stage_path(stage.name + "__swap")

        os.rename(transaction_path, tmp_path)
        os.rename(stage_path, transaction_path)
        os.rename(tmp_path, stage_path)
        shutil.rmtree(transaction_path)

    def store_blob(self, blob: Blob):
        with open(
            self.get_blob_path(blob.stage.transaction_name, blob.name), "wb"
        ) as f:
            pickle.dump(blob.obj, f, pickle.HIGHEST_PROTOCOL)

    def copy_blob_to_transaction(self, blob: Blob):
        try:
            shutil.copy2(
                self.get_blob_path(blob.stage.name, blob.name),
                self.get_blob_path(blob.stage.transaction_name, blob.name),
            )
        except FileNotFoundError:
            raise CacheError(
                f"Can't copy blob '{blob.name}' (stage: '{blob.stage.name}')"
                " to working transaction because no such blob exists."
            ) from None

    def delete_blob_from_transaction(self, blob: Blob):
        try:
            os.remove(self.get_blob_path(blob.stage.transaction_name, blob.name))
        except FileNotFoundError:
            return

    def retrieve_blob(self, blob: Blob):
        stage = blob.stage

        with open(self.get_blob_path(stage.current_name, blob.name), "rb") as f:
            return pickle.load(f)

    def get_stage_path(self, stage_name: str) -> Path:
        return self.base_path / stage_name

    def get_blob_path(self, stage_name: str, blob_name: str) -> Path:
        return self.base_path / stage_name / (blob_name + ".pkl")
