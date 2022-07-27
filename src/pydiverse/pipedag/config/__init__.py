from __future__ import annotations

import pydiverse.pipedag as pdd

store: pdd.backend.core.PipeDAGStore = None  # type: ignore
auto_table: tuple[type, ...] = tuple()
auto_blob: tuple[type, ...] = tuple()
