# Copyright (c) QuantCo and pydiverse contributors 2025-2026
# SPDX-License-Identifier: BSD-3-Clause

from collections.abc import Callable

import numpy as np
import pyarrow as pa
import structlog
from pandas.core.dtypes.base import ExtensionDtype


def build_types_mapper_from_table_and_dtypes(
    table: pa.Table,
    dtypes: dict[str, ExtensionDtype | np.dtype] | None,
    *,
    skip_datetime: bool = False,
) -> tuple[dict[pa.DataType, object], Callable[[pa.DataType], object | None]]:
    """
    Build:
      - type_map_arrow_pandas: dict[pa.DataType -> pandas dtype object]
      - types_mapper: callable suitable for table.to_pandas(types_mapper=...)

    We fill the map while iterating the Arrow schema:
      - For each column `name` present in `dtypes`, we look up its Arrow `dtype`.
      - We map that Arrow dtype -> desired pandas dtype.
      - If the same Arrow dtype is seen again with a *different* pandas dtype, warn.
        (We keep the first mapping and ignore later conflicting ones.)
    """
    logger = structlog.get_logger(__name__ + ".build_types_mapper_from_table_and_dtypes")
    dtypes = dtypes or {}

    type_map_arrow_pandas: dict[pa.DataType, object] = {}

    # Option 1: iterate fields (cleanest)
    for field in table.schema:
        name = field.name
        arrow_dtype = field.type

        if name not in dtypes:
            continue

        desired_pd_dtype = dtypes[name]

        if skip_datetime and str(desired_pd_dtype).startswith("datetime64["):
            continue  # somehow they seem to cause trouble in the arrow->pandas conversion

        if arrow_dtype in type_map_arrow_pandas:
            existing = type_map_arrow_pandas[arrow_dtype]
            if str(existing) != str(desired_pd_dtype):
                logger.warning(
                    "Ambiguous Arrow->pandas mapping for same Arrow dtype; keeping first mapping",
                    given_dtypes=dtypes,
                    arrow_dtype=str(arrow_dtype),
                    existing_pandas_dtype=str(existing),
                    conflicting_pandas_dtype=str(desired_pd_dtype),
                    conflicting_column=name,
                )
            # Keep the first mapping
            continue

        type_map_arrow_pandas[arrow_dtype] = desired_pd_dtype

    def types_mapper(pa_type: pa.DataType) -> object | None:
        return type_map_arrow_pandas.get(pa_type)

    return type_map_arrow_pandas, types_mapper
