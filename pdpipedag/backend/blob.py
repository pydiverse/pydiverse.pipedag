import os
import shutil
import pickle
from abc import ABC, abstractmethod

from pdpipedag.core import Blob, Schema
from pdpipedag.errors import CacheError


class BaseBlobStore(ABC):

    @abstractmethod
    def create_schema(self, schema: Schema):
        ...

    @abstractmethod
    def swap_schema(self, schema: Schema):
        ...

    @abstractmethod
    def store_blob(self, blob: Blob):
        ...

    @abstractmethod
    def copy_blob_to_working_schema(self, blob: Blob):
        ...

    @abstractmethod
    def retrieve_blob(self, blob: Blob, from_cache: bool = False):
        ...


class FileBlobStore(BaseBlobStore):

    def __init__(
            self,
            base_path: str
    ):
        self.base_path = os.path.abspath(base_path)
        os.makedirs(self.base_path, exist_ok = True)

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
        tmp_schema_path = self.get_schema_path(schema.name + '__tmp_swap')

        os.rename(working_schema_path, tmp_schema_path)
        os.rename(schema_path, working_schema_path)
        os.rename(tmp_schema_path, schema_path)
        shutil.rmtree(working_schema_path)

    def store_blob(self, blob: Blob):
        with open(self.get_blob_path(blob.schema.working_name, blob.name), 'wb') as f:
            pickle.dump(blob.obj, f, pickle.HIGHEST_PROTOCOL)

    def copy_blob_to_working_schema(self, blob: Blob):
        try:
            shutil.copy2(
                self.get_blob_path(blob.schema.name, blob.name),
                self.get_blob_path(blob.schema.working_name, blob.name)
            )
        except FileNotFoundError:
            raise CacheError(
                f"Can't copy blob '{blob.name}' (schema: '{blob.schema.name}')"
                f" to working schema because no such blob exists.")

    def retrieve_blob(self, blob: Blob, from_cache: bool = False):
        schema = blob.schema
        schema_name = schema.name if from_cache else schema.current_name

        with open(self.get_blob_path(schema_name, blob.name), 'rb') as f:
            return pickle.load(f)

    def get_schema_path(self, schema_name: str):
        return os.path.join(self.base_path, schema_name)

    def get_blob_path(self, schema_name: str, blob_name: str):
        return os.path.join(self.base_path, schema_name, blob_name + '.pkl')
