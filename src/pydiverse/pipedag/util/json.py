"""Utilities for JSON to support PipeDAG objects

PipeDAG objects get serialized as JSON objects with a special `__pipedag_type__`
key to identify them when decoding. The associated value encodes the
type of the object.
"""

from __future__ import annotations

import datetime as dt
import json
from enum import Enum
from functools import cache
from pathlib import Path

from pydiverse.pipedag import Stage
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.core.config import PipedagConfig
from pydiverse.pipedag.materialize.container import Blob, RawSql, Table

TYPE_KEY = "__pipedag_type__"


class Type(str, Enum):
    # Pipedag types
    TABLE = "table"
    RAW_SQL = "raw_sql"
    BLOB = "blob"
    STAGE = "stage"
    PIPEDAG_CONFIG = "pipedag_config"

    # Other types we want to support
    PATHLIB_PATH = "pathlib:path"
    DT_DATE = "dt:date"
    DT_DATETIME = "dt:datetime"

    def __str__(self):
        return self.value


def json_default(o):
    """Encode `Table`, `RawSql`, 'Stage', and `Blob` objects"""
    if isinstance(o, Table):
        return {
            TYPE_KEY: Type.TABLE,
            "stage": o.stage.name,
            "name": o.name,
            "primary_key": o.primary_key,
            "indexes": o.indexes,
            "cache_key": o.cache_key,
        }
    if isinstance(o, RawSql):
        return {
            TYPE_KEY: Type.RAW_SQL,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
            "table_names": o.table_names,
        }
    if isinstance(o, Blob):
        return {
            TYPE_KEY: Type.BLOB,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
        }
    if isinstance(o, Stage):
        return {
            TYPE_KEY: Type.STAGE,
            "name": o.name,
        }
    if isinstance(o, PipedagConfig):
        return {
            TYPE_KEY: Type.STAGE,
            "config_dict": o.config_dict,
        }
    if isinstance(o, Path):
        return {
            TYPE_KEY: Type.PATHLIB_PATH,
            "path": str(o),
        }
    if isinstance(o, dt.date):
        return {
            TYPE_KEY: Type.DT_DATE,
            "date": o.isoformat(),
        }
    if isinstance(o, dt.datetime):
        return {
            TYPE_KEY: Type.DT_DATETIME,
            "datetime": o.isoformat(),
        }

    raise TypeError(f"Object of type {type(o)} is not JSON serializable")


def json_object_hook(d: dict):
    """Decode json with `Table` and `Blob` objects"""
    if TYPE_KEY not in d:
        return d

    type_ = Type(d[TYPE_KEY])

    run_context = RunContext.get()
    stages = run_context.flow.stages

    if type_ == Type.TABLE:
        tbl = Table(
            name=d["name"],
            primary_key=d["primary_key"],
            indexes=d["indexes"],
        )
        tbl.stage = stages[d["stage"]]
        tbl.cache_key = d["cache_key"]
        return tbl
    if type_ == Type.RAW_SQL:
        raw_sql = RawSql(name=d["name"])
        raw_sql.stage = stages[d["stage"]]
        raw_sql.cache_key = d["cache_key"]
        raw_sql.table_names = d["table_names"]
        return raw_sql
    if type_ == Type.BLOB:
        blob = Blob(name=d["name"])
        blob.stage = stages[d["stage"]]
        blob.cache_key = d["cache_key"]
        return blob
    if type_ == Type.STAGE:
        return stages[d["name"]]
    if type_ == Type.PIPEDAG_CONFIG:
        # PipedagConfig objects are allowed as input to @materialize tasks,
        # but it is not allowed as output since this might cause trouble
        # for cache-invalidation
        raise TypeError("PipedagConfig can't be deserialized.")
    if type_ == Type.PATHLIB_PATH:
        return Path(d["path"])
    if type_ == Type.DT_DATE:
        return dt.date.fromisoformat(d["date"])
    if type_ == Type.DT_DATETIME:
        return dt.datetime.fromisoformat(d["datetime"])

    raise ValueError(f"Invalid value for '{TYPE_KEY}' key: {type_}")


@cache
class PipedagJSONEncoder(json.JSONEncoder):
    def __init__(self):
        super().__init__(
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
            default=json_default,
        )


@cache
class PipedagJSONDecoder(json.JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=json_object_hook)
