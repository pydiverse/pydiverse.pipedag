from __future__ import annotations

import pdpipedag
from pdpipedag.core import Table, Blob

PIPEDAG_TYPE = '_pipedag_type_'
PIPEDAG_TYPE_TABLE = 'table'
PIPEDAG_TYPE_BLOB = 'blob'


def json_default(o):
    if isinstance(o, Table):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_TABLE,
            'schema': o.schema.name,
            'name': o.name,
            'cache_key': o.cache_key,
        }
    if isinstance(o, Blob):
        return {
            PIPEDAG_TYPE: PIPEDAG_TYPE_BLOB,
            'schema': o.schema.name,
            'name': o.name,
            'cache_key': o.cache_key,
        }

    raise TypeError(f'Object of type {type(o).__name__} is not JSON serializable')


def json_object_hook(d: dict):
    pipedag_type = d.get(PIPEDAG_TYPE)
    if pipedag_type:
        if pipedag_type == PIPEDAG_TYPE_TABLE:
            return Table(
                name = d['name'],
                schema = pdpipedag.config.store.schemas[d['schema']],
                cache_key = d['cache_key']
            )
        elif pipedag_type == PIPEDAG_TYPE_BLOB:
            return Blob(
                name = d['name'],
                schema = pdpipedag.config.store.schemas[d['schema']],
                cache_key = d['cache_key']
            )
        else:
            raise ValueError(f"Invalid value for '{PIPEDAG_TYPE}' key: {repr(pipedag_type)}")

    return d
