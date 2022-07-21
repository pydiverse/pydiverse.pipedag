from __future__ import annotations

import datetime
from dataclasses import dataclass


@dataclass
class TaskMetadata:
    name: str
    schema: str
    version: str | None
    timestamp: datetime.datetime
    run_id: str
    cache_key: str
    output_json: str


@dataclass
class LazyTableMetadata:
    name: str
    schema: str
    cache_key: str
