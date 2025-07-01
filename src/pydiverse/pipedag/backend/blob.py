# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import os
import pickle
import shutil
from typing import Any

from upath import UPath

from pydiverse.pipedag import Blob, Stage
from pydiverse.pipedag.context import ConfigContext
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.store import BaseBlobStore
from pydiverse.pipedag.util import normalize_name

__all__ = [
    "FileBlobStore",
]

from pydiverse.pipedag.util.path import is_file_uri


class NoBlobStore(BaseBlobStore):
    """No blob store

    The ``NoBlobStore`` can be used for pipelines where no task outputs any blob.
    """

    def init_stage(self, stage: Stage):
        pass

    def commit_stage(self, stage: Stage):
        pass

    def store_blob(self, blob: Blob):
        raise AttributeError(
            f"Using NoBlobStore is only allowed for pipelines where no task outputs "
            f"any blob. See {blob.stage.name}.{blob.name} ({type(blob.obj)})"
        )

    def copy_blob_to_transaction(self, blob: Blob):
        raise AttributeError(
            f"Using NoBlobStore is only allowed for pipelines where no task outputs "
            f"any blob. See {blob.stage.name}.{blob.name} ({type(blob.obj)})"
        )

    def delete_blob_from_transaction(self, blob: Blob):
        raise AttributeError(
            f"Using NoBlobStore is only allowed for pipelines where no task outputs "
            f"any blob. See {blob.stage.name}.{blob.name} ({type(blob.obj)})"
        )

    def retrieve_blob(self, blob: Blob):
        raise AttributeError(
            f"Using NoBlobStore is only allowed for pipelines where no task outputs "
            f"any blob. See {blob.stage.name}.{blob.name} ({type(blob.obj)})"
        )


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
        base_path = UPath(config["base_path"]) / instance_id
        return cls(base_path)

    def __init__(self, base_path: str | UPath):
        self.base_path = UPath(base_path).absolute()
        if is_file_uri(self.base_path):
            os.makedirs(self.base_path, exist_ok=True)

    def init_stage(self, stage: Stage):
        stage_path = self.get_stage_path(stage.name)
        transaction_path = self.get_stage_path(stage.transaction_name)

        if is_file_uri(stage_path):
            try:
                os.mkdir(stage_path)
            except FileExistsError:
                pass

        if is_file_uri(transaction_path):
            try:
                os.mkdir(transaction_path)
            except FileExistsError:
                shutil.rmtree(transaction_path)
                os.mkdir(transaction_path)
        else:
            # clear bucket key prefix similar to removing a directory
            if transaction_path.exists():
                transaction_path.fs.rmdir(transaction_path)

    def commit_stage(self, stage: Stage):
        stage_path = self.get_stage_path(stage.name)
        transaction_path = self.get_stage_path(stage.transaction_name)
        tmp_path = self.get_stage_path(stage.name + "__swap")

        if not is_file_uri(transaction_path):
            # TODO: think about switching to odd/even transaction schemas
            # to avoid renaming all files in a schema.
            # Another alternative: store all files written to transaction
            raise NotImplementedError("Using Bucket as base path is currently not implemented.")
        os.rename(transaction_path, tmp_path)
        os.rename(stage_path, transaction_path)
        os.rename(tmp_path, stage_path)
        shutil.rmtree(transaction_path)

    def store_blob(self, blob: Blob):
        with open(self.get_blob_path(blob.stage.transaction_name, blob.name), "wb") as f:
            pickle.dump(blob.obj, f, pickle.HIGHEST_PROTOCOL)

    def copy_blob_to_transaction(self, blob: Blob):
        from_path = self.get_blob_path(blob.stage.name, blob.name)
        to_path = self.get_blob_path(blob.stage.transaction_name, blob.name)
        if is_file_uri(from_path):
            try:
                shutil.copy2(
                    from_path,
                    to_path,
                )
            except FileNotFoundError:
                raise CacheError(
                    f"Can't copy blob '{blob.name}' (stage: '{blob.stage.name}')"
                    " to working transaction because no such blob exists."
                ) from None
        else:
            from_path.fs.copy(from_path, to_path, recursive=True)

    def delete_blob_from_transaction(self, blob: Blob):
        self.get_blob_path(blob.stage.transaction_name, blob.name).unlink(missing_ok=True)

    def retrieve_blob(self, blob: Blob):
        stage = blob.stage

        with open(self.get_blob_path(stage.current_name, blob.name), "rb") as f:
            return pickle.load(f)

    def get_stage_path(self, stage_name: str) -> UPath:
        return self.base_path / stage_name

    def get_blob_path(self, stage_name: str, blob_name: str) -> UPath:
        return self.base_path / stage_name / (blob_name + ".pkl")
