"""Utilities for JSON to support PipeDAG objects

PipeDAG objects get serialized as JSON objects with a special `_pipedag_type_`
key to identify them when decoding. The associated value encodes the
type of the object.
"""

from __future__ import annotations

from pydiverse.pipedag import Stage
from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.materialize.container import Blob, RawSql, Table
from pydiverse.pipedag.util.config import PipedagConfig

PIPEDAG_TYPE = "_pipedag_type_"
PIPEDAG_TYPE_TABLE = "table"
PIPEDAG_TYPE_RAWSQL = "raw_sql"
PIPEDAG_TYPE_BLOB = "blob"
PIPEDAG_TYPE_STAGE = "stage"
PIPEDAG_TYPE_PIPEDAG_CONFIG = "pipedag_config"


def json_default(o):
    """Encode `Table`, `RawSql`, 'Stage', and `Blob` objects"""
    if isinstance(o, Table):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_TABLE,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
        }
    if isinstance(o, RawSql):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_RAWSQL,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
        }
    if isinstance(o, Stage):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_STAGE,
            "name": o.name,
        }
    if isinstance(o, Blob):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_BLOB,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
        }
    if isinstance(o, PipedagConfig):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_PIPEDAG_CONFIG,
            "pipedag_config_dict": o.pipedag_config_dict,
            "config_file": str(o.config_file),
        }

    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def json_object_hook(d: dict):
    """Decode json with `Table` and `Blob` objects"""
    pipedag_type = d.get(PIPEDAG_TYPE)
    if pipedag_type:
        run_context = RunContext.get()
        stages = run_context.flow.stages

        if pipedag_type == PIPEDAG_TYPE_TABLE:
            return Table(
                name=d["name"],
                stage=stages[d["stage"]],
                cache_key=d["cache_key"],
            )
        elif pipedag_type == PIPEDAG_TYPE_RAWSQL:
            return RawSql(
                name=d["name"],
                stage=stages[d["stage"]],
                cache_key=d["cache_key"],
            )
        elif pipedag_type == PIPEDAG_TYPE_BLOB:
            return Blob(
                name=d["name"],
                stage=stages[d["stage"]],
                cache_key=d["cache_key"],
            )
        elif pipedag_type == PIPEDAG_TYPE_STAGE:
            return stages[d["name"]]
        elif pipedag_type == PIPEDAG_TYPE_PIPEDAG_CONFIG:
            return PipedagConfig(d["pipedag_config_dict"], d["config_file"])
        else:
            raise ValueError(
                f"Invalid value for '{PIPEDAG_TYPE}' key: {repr(pipedag_type)}"
            )

    return d
