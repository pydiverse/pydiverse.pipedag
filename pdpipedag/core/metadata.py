import datetime
from dataclasses import dataclass


@dataclass
class TaskMetadata:
    name: str
    schema: str
    version: str
    timestamp: datetime.datetime
    run_id: str
    cache_key: str
    output_json: str


@dataclass
class LazyTableMetadata:
    name: str
    schema: str
    cache_key: str
