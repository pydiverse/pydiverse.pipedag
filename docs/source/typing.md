# Static typing

Pipedag ships a `py.typed` marker, so type checkers such as
[pyright](https://github.com/microsoft/pyright) and [mypy](https://mypy-lang.org/) see the
annotations of its public API. This page explains what they can and cannot check for you, and how
to annotate your tasks so that the checks are useful.

## What gets checked

The {py:func}`@materialize <pydiverse.pipedag.materialize>` decorator preserves the signature of
the function it decorates. At flow declaration sites, that means the following mistakes are
reported statically:

```python
@materialize(version="1.0", input_type=pd.DataFrame)
def combine(left: pd.DataFrame, right: pd.DataFrame, how: str = "inner"):
    return Table(left.merge(right, how=how))

with Flow() as flow:
    with Stage("stage"):
        a, b = input_tables()

        combine(a)                  # error: missing argument "right"
        combine(a, b, hwo="outer")  # error: no parameter named "hwo"
        combine(a, b, "outer", 1)   # error: too many arguments
        combine(a, b, how=3)        # error: int is not assignable to str
```

Tasks declared with `nout=2` or `nout=3` whose function is annotated to return a tuple of that
length are checked for the arity of the unpacking assignment:

```python
@materialize(nout=2)
def two_tables() -> tuple[Table, Table]:
    return Table(...), Table(...)

with Flow() as flow:
    with Stage("stage"):
        a, b = two_tables()     # fine
        a, b, c = two_tables()  # error: not enough values to unpack
```

Such a result that is *not* unpacked stays a task, so it can also be passed around as a whole
(`both = two_tables()`, `both[0]`, `result.get(both)`).

Every other case — other `nout` values, a `nout` computed at run time, or a function that returns a
list — still unpacks, iterates and subscripts, but each element is typed
`MaterializingTaskGetItem[Any]` and the arity is not verified:

```python
@materialize(nout=4)
def four_tables():
    return Table(...), Table(...), Table(...), Table(...)

with Stage("stage"):
    a, b, c, d = four_tables()   # fine, but a..d are MaterializingTaskGetItem[Any]
    w, x, y = four_tables()      # NOT reported; fails at run time instead
```

Each checked arity needs its own `@materialize` overload plus a matching tuple type. A single
variadic definition can't replace them, because a `TypeVarTuple` can't wrap each element in
`MaterializingTaskGetItem` (the same missing mapped-type feature described below). `nout=2` and
`nout=3` cover nearly all real usage, so only those two are spelled out.

{py:meth}`Result.get() <pydiverse.pipedag.Result.get>` and
{py:meth}`MaterializingTask.get_output_from_store()
<pydiverse.pipedag.MaterializingTask.get_output_from_store>` return `Any`. What they return depends
on the task's output structure (a single table, a tuple, a dict, ...) and on the table hook
(`as_type=sa.Table` returns an `sa.Alias`, pydiverse.transform types return a `pdt.Table`), none of
which is known statically. Annotate the variable instead:

```python
with StageLockContext():
    result = flow.run()
    df: pd.DataFrame = result.get(task, as_type=pd.DataFrame)
```

## Declaration time vs. run time, and why task objects are opaque

As described in {ref}`declaration time vs. run time <quickstart:Declaration time vs. run time>`, a
task function is annotated in terms of the *dematerialized* values it receives at run time:

```python
@materialize(version="1.0", input_type=pd.DataFrame)
def clean(df: pd.DataFrame):
    return Table(df.dropna())
```

but at declaration time it is called with a
{py:class}`~pydiverse.pipedag.MaterializingTask`, not with a `pd.DataFrame`. Rewriting each
parameter type into "task producing that type" can't be expressed in Python's type system — there
is no way to map a transformation over a
[`ParamSpec`](https://docs.python.org/3/library/typing.html#typing.ParamSpec).

Pipedag therefore solves this from the other side: task objects
({py:class}`~pydiverse.pipedag.Task`, {py:class}`~pydiverse.pipedag.TaskGetItem` and their
subclasses {py:class}`~pydiverse.pipedag.MaterializingTask` and
{py:class}`~pydiverse.pipedag.MaterializingTaskGetItem`) as well as
{py:class}`~pydiverse.pipedag.Table` and {py:class}`~pydiverse.pipedag.Blob` have `Any` among
their base classes, the same technique the
standard library's type stubs use for `unittest.mock.Mock`. Such an object is assignable to any
parameter type, so wiring never produces a false error. For `Table` and `Blob` this also covers
`Table(ExternalTableReference(...))` passed as a task input, and `return Table(...)` from a task
annotated with a schema class (see below).

The trade-off is deliberate: **task objects are opaque to the type checker**. Unknown attributes,
subscripting, and passing a task where a concrete type is expected are all accepted without
complaint. That is what makes lazy field access work:

```python
with Flow() as flow:
    with Stage("stage"):
        economic = economic_data()
        model(economic.aa, economic.bb)   # recorded, resolved at run time
```

One limitation remains: generic containers are invariant. A list literal passed directly as an
argument is fine, but a variable previously inferred as `list[Table[ExternalTableReference]]` is not
assignable to a `list[sa.Alias]` parameter. Annotate such variables as `list[Any]`, or use a
covariant parameter type such as `Sequence[sa.Alias]` in the task signature.

## Annotating task functions

Annotations on task functions are not only documentation — pipedag reads them at run time. The
return annotation drives [dataframely](https://github.com/Quantco/dataframely) and colspec
validation as well as column type selection in the table store. Two consequences:

- **All names in a task signature must be resolvable at run time.** Pipedag calls
  {py:func}`typing.get_type_hints` on the decorated function when the `@materialize` decorator is
  applied. String annotations (including those produced by `from __future__ import annotations`)
  work as long as they refer to names in the module's globals; names imported only under
  `if TYPE_CHECKING:` or defined locally make the decorator fail.

- **Annotate the schema class directly**, not wrapped in `Table`:

  ```python
  @materialize(input_type=pl.LazyFrame)
  def features() -> MyColSpec:      # validated
      ...

  @materialize(input_type=pl.LazyFrame)
  def features() -> Table[MyColSpec]:   # NOT validated
      ...
  ```

  The second form silently disables validation, because pipedag looks for an annotation that is
  itself a class and `Table[MyColSpec]` is a subscripted generic. If you want to annotate that a
  task returns a table without a schema, use a bare `-> Table`.

## Running a type checker

mypy only checks the bodies of functions that have annotations, unless `check_untyped_defs` is
enabled. Flow declarations typically live in an unannotated `def get_flow():`, so either annotate it
(`def get_flow() -> Flow:`) or enable that option.

pyright infers the return type of an unannotated `nout=2` / `nout=3` task function. mypy treats it as
`Any`, which makes the whole task `Any` and disables all checks at its call sites. With mypy,
annotate the return type of such tasks (`-> tuple[Table, Table]`).

Pipedag's own public API assertions live in `tests/type_checking/` and are checked with both
pyright and mypy; pyright is authoritative. The library's internals are *not* type-clean: pointing a
checker at `src/pydiverse/pipedag/` reports many pre-existing errors that do not affect the
annotations you consume.
