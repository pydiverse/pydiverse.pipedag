# Copyright (c) QuantCo and pydiverse contributors 2025-2026
# SPDX-License-Identifier: BSD-3-Clause
"""Mistakes at flow declaration sites that both type checkers must report.

Not a pytest module - see README.md. Every line below is expected to produce an error,
and every expected error carries both a mypy and a pyright suppression marker. Both
checkers are configured to flag suppressions that turn out to be unnecessary, so this
file fails not only when a new error appears but also when one of these errors *stops*
being reported.
"""

import pandas as pd

from pydiverse.pipedag import Flow, Stage, Table, materialize


@materialize(version="1.0", input_type=pd.DataFrame)
def one_frame() -> Table:
    return Table(pd.DataFrame({"x": [1]}))


@materialize(version="1.0", input_type=pd.DataFrame)
def combine(left: pd.DataFrame, right: pd.DataFrame, suffix: str = "_r") -> Table:
    return Table(left.merge(right, suffixes=("", suffix)))


@materialize(nout=2)
def two() -> tuple[Table, Table]:
    return Table(pd.DataFrame()), Table(pd.DataFrame())


@materialize(nout=3)
def three() -> tuple[Table, Table, Table]:
    return Table(pd.DataFrame()), Table(pd.DataFrame()), Table(pd.DataFrame())


def wiring_errors() -> None:
    with Flow("flow"), Stage("stage"):
        a = one_frame()

        # Missing argument.
        combine(a)  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]

        # Unexpected keyword argument. The misspelling has to be one the `typos`
        # pre-commit hook does not recognise, otherwise that hook rejects this file.
        combine(a, a, suffx="_x")  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]

        # Too many positional arguments.
        combine(a, a, "_x", a)  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]

        # Wrong concrete type for a scalar parameter. Task objects are assignable to
        # anything, but plain values are still checked.
        combine(a, a, suffix=3)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


def nout_errors() -> None:
    with Flow("flow"), Stage("stage"):
        # nout=2 does not unpack into three names.
        p, q, r = two()  # type: ignore[misc]  # pyright: ignore[reportAssignmentType]

        # ... nor nout=3 into two.
        s, t = three()  # type: ignore[misc]  # pyright: ignore[reportAssignmentType]

        combine(p, q, str(r))
        combine(s, t)


def result_get_errors() -> None:
    flow = Flow("flow")
    with flow, Stage("stage"):
        a = one_frame()

    result = flow.run()

    # `as_type` decides the returned type, so the annotation here is wrong.
    _: int = result.get(a, as_type=pd.DataFrame)  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]

    # Same for the task's own accessor.
    __: int = a.get_output_from_store(as_type=pd.DataFrame)  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]
