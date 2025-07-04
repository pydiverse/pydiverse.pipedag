# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

"""Utilities for JSON to support PipeDAG objects

PipeDAG objects get serialized as JSON objects with a special `__pipedag_type__`
key to identify them when decoding. The associated value encodes the
type of the object.
"""

import datetime as dt
import importlib
import json
from enum import Enum
from functools import cache
from pathlib import Path
from typing import get_args, get_origin

from pydiverse.common.util.computation_tracing import fully_qualified_name
from pydiverse.common.util.import_ import load_object

TYPE_KEY = "__pipedag_type__"


class Type(str, Enum):
    # Pipedag types
    TABLE = "table"
    RAW_SQL = "raw_sql"
    BLOB = "blob"
    STAGE = "stage"
    PIPEDAG_CONFIG = "pipedag_config"
    CONFIG_CONTEXT = "config_context"

    # Data classes can be reconstructed
    DATA_CLASS = "data_class"

    # Types / Type Annotations can be reconstructed
    TYPE = "type"
    GENERIC_ALIAS = "generic_alias"

    # Other types we want to support
    PATHLIB_PATH = "pathlib:path"
    DT_DATE = "dt:date"
    DT_DATETIME = "dt:datetime"

    def __str__(self):
        return self.value


def json_default(o):
    """Encode `Table`, `RawSql`, 'Stage', and `Blob` objects"""
    from pydiverse.pipedag import ConfigContext, Stage
    from pydiverse.pipedag.container import Blob, RawSql, Table
    from pydiverse.pipedag.core.config import PipedagConfig

    if isinstance(o, Table):
        kwargs = {}
        if o.assumed_dependencies is not None:
            kwargs["assumed_dependencies"] = o.assumed_dependencies  # [json_default(t) for t in o.assumed_dependencies]
        return {
            TYPE_KEY: Type.TABLE,
            "stage": o.stage.name if o.stage is not None else None,
            "name": o.name,
            "primary_key": o.primary_key,
            "indexes": o.indexes,
            "cache_key": o.cache_key,
            "materialization_details": o.materialization_details,
            "external_schema": o.external_schema,
            "shared_lock_allowed": o.shared_lock_allowed,
            "annotation": o.annotation,
            **kwargs,
        }
    if isinstance(o, RawSql):
        kwargs = {}
        if o.assumed_dependencies is not None:
            kwargs["assumed_dependencies"] = o.assumed_dependencies
        return {
            TYPE_KEY: Type.RAW_SQL,
            "stage": o.stage.name if o.stage is not None else None,
            "name": o.name,
            "cache_key": o.cache_key,
            "table_names": o.table_names,
            **kwargs,
        }
    if isinstance(o, Blob):
        return {
            TYPE_KEY: Type.BLOB,
            "stage": o.stage.name if o.stage is not None else None,
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
            TYPE_KEY: Type.PIPEDAG_CONFIG,
            "config_dict": o.config_dict,
        }
    if isinstance(o, ConfigContext):
        return {
            TYPE_KEY: Type.CONFIG_CONTEXT,
            "config_dict": o._config_dict,
            "pipedag_name": o.pipedag_name,
            "flow_name": o.flow_name,
            "instance_name": o.instance_name,
        }
    if isinstance(o, Path):
        return {
            TYPE_KEY: Type.PATHLIB_PATH,
            "path": str(o),
        }
    if isinstance(o, dt.datetime):
        return {
            TYPE_KEY: Type.DT_DATETIME,
            "datetime": o.isoformat(),
        }
    if isinstance(o, dt.date):
        return {
            TYPE_KEY: Type.DT_DATE,
            "date": o.isoformat(),
        }
    if get_origin(o) is not None:
        # must be GenericAlias
        # somehow isinstance(o, GenericAlias) did not work reliably
        return {
            TYPE_KEY: Type.GENERIC_ALIAS,
            "origin": get_origin(o),
            "args": get_args(o),
        }
    if isinstance(o, type):
        return {
            TYPE_KEY: Type.TYPE,
            "module": o.__module__,
            "qualname": o.__qualname__,
        }
    try:
        # provide better error message in case of pdt.Table
        import pydiverse.transform as pdt

        if isinstance(o, pdt.Table):
            raise TypeError(
                "pydiverse.transform.Table is not supposed to be JSON serialized. "
                "It should be a pipedag Table instead."
                "Consider adding pydiverse.transform.Table to your auto_table setting "
                "in the PipedagConfig."
            )
    except ImportError:
        pass
    if hasattr(o, "__dataclass_fields__") and not isinstance(o, type):
        return {
            TYPE_KEY: Type.DATA_CLASS,
            "config_dict": {
                "class": fully_qualified_name(o),
                "args": o.__dict__,
            },
        }
    try:
        # provide better error message in case of pdt.Table
        import polars as pl

        if isinstance(o, pl.DataFrame | pl.LazyFrame):
            raise TypeError(
                "Polars Tables are not supposed to be JSON serialized. "
                "It should be a pipedag Table instead."
                "Consider adding polars.DataFrame and polars.LazyFrame to your "
                "auto_table setting in the PipedagConfig."
            )
    except ImportError:
        pass
    import pandas as pd

    if isinstance(o, pd.DataFrame):
        raise TypeError(
            "Pandas Tables are not supposed to be JSON serialized. "
            "It should be a pipedag Table instead."
            "Consider adding pandas.DataFrame to your "
            "auto_table setting in the PipedagConfig."
        )

    raise TypeError(f"Object of type {type(o)} is not JSON serializable")


def json_object_hook(d: dict):
    """Decode json with `Table` and `Blob` objects"""
    from pydiverse.pipedag import ConfigContext
    from pydiverse.pipedag.container import Blob, RawSql, Table
    from pydiverse.pipedag.context import RunContext
    from pydiverse.pipedag.core.config import PipedagConfig

    if TYPE_KEY not in d:
        return d

    type_ = Type(d[TYPE_KEY])

    def get_stage(name: str | None):
        if name is None:
            return None
        stages = RunContext.get().flow.stages
        return stages[name]

    if type_ == Type.TABLE:
        tbl = Table(
            name=d["name"],
            primary_key=d["primary_key"],
            indexes=d["indexes"],
            materialization_details=d.get("materialization_details"),
            annotation=d.get("annotation"),
        )
        tbl.stage = get_stage(d["stage"])
        tbl.cache_key = d["cache_key"]
        tbl.external_schema = d.get("external_schema")
        tbl.shared_lock_allowed = d.get("shared_lock_allowed", True)
        tbl.assumed_dependencies = d.get("assumed_dependencies")
        return tbl
    if type_ == Type.RAW_SQL:
        raw_sql = RawSql(name=d["name"])
        raw_sql.stage = get_stage(d["stage"])
        raw_sql.cache_key = d["cache_key"]
        raw_sql.table_names = d["table_names"]
        raw_sql.assumed_dependencies = d.get("assumed_dependencies")
        return raw_sql
    if type_ == Type.BLOB:
        blob = Blob(name=d["name"])
        blob.stage = get_stage(d["stage"])
        blob.cache_key = d["cache_key"]
        return blob
    if type_ == Type.STAGE:
        return get_stage(d["name"])
    if type_ == Type.PIPEDAG_CONFIG:
        return PipedagConfig(d["config_dict"])
    if type_ == Type.CONFIG_CONTEXT:
        return ConfigContext.new(d["config_dict"], d["pipedag_name"], d["flow_name"], d["instance_name"])
    if type_ == Type.PATHLIB_PATH:
        return Path(d["path"])
    if type_ == Type.DT_DATE:
        return dt.date.fromisoformat(d["date"])
    if type_ == Type.DT_DATETIME:
        return dt.datetime.fromisoformat(d["datetime"])
    if type_ == Type.DATA_CLASS:
        return load_object(d["config_dict"])
    if type_ == Type.TYPE:
        module = d["module"]
        qualname = d["qualname"]
        return getattr(importlib.import_module(module), qualname)
    if type_ == Type.GENERIC_ALIAS:
        origin = d["origin"]
        args = tuple(d["args"])
        return origin[args]

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
