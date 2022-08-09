"""Utilities for JSON to support PipeDAG objects

PipeDAG objects get serialized as JSON objects with a special `_pipedag_type_`
key to identify them when decoding. The associated value encodes the
type of the object.
"""

from __future__ import annotations

from pydiverse.pipedag.context import RunContext
from pydiverse.pipedag.materialize.container import Blob, Table

PIPEDAG_TYPE = "_pipedag_type_"
PIPEDAG_TYPE_TABLE = "table"
PIPEDAG_TYPE_BLOB = "blob"


def json_default(o):
    """Encode `Table` and `Blob` objects"""
    if isinstance(o, Table):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_TABLE,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
        }
    if isinstance(o, Blob):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_BLOB,
            "stage": o.stage.name,
            "name": o.name,
            "cache_key": o.cache_key,
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
        elif pipedag_type == PIPEDAG_TYPE_BLOB:
            return Blob(
                name=d["name"],
                stage=stages[d["stage"]],
                cache_key=d["cache_key"],
            )
        else:
            raise ValueError(
                f"Invalid value for '{PIPEDAG_TYPE}' key: {repr(pipedag_type)}"
            )

    return d
