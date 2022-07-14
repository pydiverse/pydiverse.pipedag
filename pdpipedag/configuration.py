from dataclasses import dataclass

from pdpipedag import backend


# TODO: Provide a mechanism to configure pipedag using a TOML config file

@dataclass
class Config:
    table_backend: backend.table.BaseTableStorage


# Global config object
config: Config = None
