from __future__ import annotations

import os
import pickle
import shutil
from abc import ABC, abstractmethod
from typing import Any

from pydiverse.pipedag import config
from pydiverse.pipedag.core import Blob, Schema
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.util import normalise_name

__all__ = [
    "BaseBlobStore",
    "FileBlobStore",
]


class BaseBlobStore(ABC):
    """Blob store base class

    A blob (binary large object) store is responsible for storing arbitrary
    python objects. This can, for example, be done by serializing them using
    the python `pickle` module.

    A store must use a blob's name (`blob.name`) and schema (`blob.schema`)
    as the primary keys for storing and retrieving blobs. This means that
    two different `Blob` objects can be used to store and retrieve the same
    data as long as they have the same name and schema.
    """

    @abstractmethod
    def create_schema(self, schema: Schema):
        """Creates a schema

        Ensures that the base schema exists (but doesn't clear it) and that
        the working schema exists and is empty.
        """

    @abstractmethod
    def swap_schema(self, schema: Schema):
        """Swap the base schema with the working schema

        After the schema swap the contents of the base schema should be in the
        working schema, and the contents of the working schema in the base
        schema.
        """

    @abstractmethod
    def store_blob(self, blob: Blob):
        """Stores a blob in the associated working schema"""

    @abstractmethod
    def copy_blob_to_working_schema(self, blob: Blob):
        """Copy a blob from the base schema to the working schema

        This operation MUST not remove the blob from the base schema or modify
        it in any way.
        """

    @abstractmethod
    def delete_blob_from_working_schema(self, blob: Blob):
        """Delete a blob from the working schema

        If the blob doesn't exist in the working schema, fail silently.
        """

    @abstractmethod
    def retrieve_blob(self, blob: Blob, from_cache: bool = False) -> Any:
        """Loads a blob from the store

        Retrieves the stored python object from the store and returns it.
        If `from_cache` is `False` (default), the blob must be retrieved
        from the current schema (`blob.schema.current_name`). Before a
        schema swap this corresponds to the working schema and afterwards
        to the base schema. If `from_cache` is `True`, it must always be
        retrieved from the base schema.
        """


class FileBlobStore(BaseBlobStore):
    """File based blob store

    The FileBlobStore stores blobs in a folder structure on a file system.
    In the base directory there will be two folders for every schema, one
    for the base and one for the working schema. Inside those folders the
    blobs will be stored as pickled files:
    `/base_path/PROJECT_NAME/SCHEMA_NAME/BLOB_NAME.pkl`.

    To swap a schema, the only thing that has to be done is to rename the
    appropriate folders.
    """

    def __init__(self, base_path: str):
        self.base_path = os.path.abspath(base_path)
        if config.name is not None:
            project_name = normalise_name(config.name)
            self.base_path = os.path.join(self.base_path, project_name)

        os.makedirs(self.base_path, exist_ok=True)

    def create_schema(self, schema: Schema):
        schema_path = self.get_schema_path(schema.name)
        working_schema_path = self.get_schema_path(schema.working_name)

        try:
            os.mkdir(schema_path)
        except FileExistsError:
            pass

        try:
            os.mkdir(working_schema_path)
        except FileExistsError:
            shutil.rmtree(working_schema_path)
            os.mkdir(working_schema_path)

    def swap_schema(self, schema: Schema):
        schema_path = self.get_schema_path(schema.name)
        working_schema_path = self.get_schema_path(schema.working_name)
        tmp_schema_path = self.get_schema_path(schema.name + "__tmp_swap")

        os.rename(working_schema_path, tmp_schema_path)
        os.rename(schema_path, working_schema_path)
        os.rename(tmp_schema_path, schema_path)
        shutil.rmtree(working_schema_path)

    def store_blob(self, blob: Blob):
        with open(self.get_blob_path(blob.schema.working_name, blob.name), "wb") as f:
            pickle.dump(blob.obj, f, pickle.HIGHEST_PROTOCOL)

    def copy_blob_to_working_schema(self, blob: Blob):
        try:
            shutil.copy2(
                self.get_blob_path(blob.schema.name, blob.name),
                self.get_blob_path(blob.schema.working_name, blob.name),
            )
        except FileNotFoundError:
            raise CacheError(
                f"Can't copy blob '{blob.name}' (schema: '{blob.schema.name}')"
                " to working schema because no such blob exists."
            )

    def delete_blob_from_working_schema(self, blob: Blob):
        try:
            os.remove(self.get_blob_path(blob.schema.working_name, blob.name))
        except FileNotFoundError:
            return

    def retrieve_blob(self, blob: Blob, from_cache: bool = False):
        schema = blob.schema
        schema_name = schema.name if from_cache else schema.current_name

        with open(self.get_blob_path(schema_name, blob.name), "rb") as f:
            return pickle.load(f)

    def get_schema_path(self, schema_name: str):
        return os.path.join(self.base_path, schema_name)

    def get_blob_path(self, schema_name: str, blob_name: str):
        return os.path.join(self.base_path, schema_name, blob_name + ".pkl")
