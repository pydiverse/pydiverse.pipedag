# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

# In order to avoid circular dependencies, we use the following import order:
# - Containers
# - Contexts
# - Task
# - Materialize
# - Core:
#   * PipedagConfig (rather independent from other core)
#   * GroupNode (connected to task)
#   * VisualizationStyle (connected to GroupNode)
#   * Stage (needed by Flow)
#   * Result (needed by Flow)
#   * Flow (needs a lot of the above)
# - Engine
# - Backends

# isort: skip_file
from .container import (
    Blob,
    ExternalTableReference,
    RawSql,
    Schema,
    Table,
    materialize_table,
)
from .context import ConfigContext, StageLockContext
from .core.task import (
    Task,
    TaskGetItem,
)
from .materialize import (
    input_stage_versions,
    materialize,
)
from .materialize.materializing_task import AUTO_VERSION
from .core import (
    PipedagConfig,
    GroupNode,
    VisualizationStyle,
    Stage,
    Result,
    Flow,
)
from . import backend

__all__ = [
    "Flow",
    "Stage",
    "materialize",
    "input_stage_versions",
    "Table",
    "RawSql",
    "Blob",
    "ExternalTableReference",
    "Task",
    "TaskGetItem",
    "GroupNode",
    "VisualizationStyle",
    "Schema",
    "Result",
    "PipedagConfig",
    "ConfigContext",
    "StageLockContext",
    "AUTO_VERSION",
    "backend",
    "materialize_table",
]
