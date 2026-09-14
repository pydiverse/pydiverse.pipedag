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

Tasks declared with `nout=2` or `nout=3` are checked for the arity of the unpacking assignment:

```python
@materialize(nout=2)
def two_tables():
    return Table(...), Table(...)

with Flow() as flow:
    with Stage("stage"):
        a, b = two_tables()     # fine
        a, b, c = two_tables()  # error: not enough values to unpack
```

A `nout=2` / `nout=3` result that is *not* unpacked stays a task, so it can also be passed around
as a whole:

```python
with Stage("stage"):
    both = two_tables()   # one task representing both outputs
    a = both[0]           # subscripting still works
...
result.get(both)          # fetches the task's outputs
```

Only `nout=2` and `nout=3` get this arity check. Every other `nout` value — including one computed
at run time — still unpacks, iterates and subscripts, but each element is typed
`MaterializingTaskGetItem[Any]` and the arity is not verified:

```python
@materialize(nout=4)
def four_tables():
    return Table(...), Table(...), Table(...), Table(...)

with Stage("stage"):
    a, b, c, d = four_tables()   # fine, but a..d are MaterializingTaskGetItem[Any]
    w, x, y = four_tables()      # NOT reported; fails at run time instead
```

The cutoff is not a limitation of the checkers but of the type system. Each checked arity needs its
own `@materialize` overload plus a matching tuple type, and a single variadic definition cannot
replace them: a `TypeVarTuple` can either be generic over the arity *or* wrap each element in
`MaterializingTaskGetItem`, never both — the same missing mapped-type facility described below.
Since `nout=2` and `nout=3` cover effectively all real usage, only those two are spelled out.

Finally, {py:meth}`Result.get() <pydiverse.pipedag.Result.get>` and
{py:meth}`MaterializingTask.get_output_from_store()
<pydiverse.pipedag.MaterializingTask.get_output_from_store>` return the type you asked for:

```python
with StageLockContext():
    result = flow.run()
    df = result.get(task, as_type=pd.DataFrame)  # df: pd.DataFrame
    obj = result.get(task)                       # obj: Any
```

Without `as_type`, the value is dematerialized as the task's own `input_type`, which is a run-time
property of the task and not something the annotation can express — hence `Any`.

## Declaration time vs. run time, and why task objects are opaque

As described in [](/quickstart#declaration-time-vs-run-time), a task function is annotated in terms
of the *dematerialized* values it receives at run time:

```python
@materialize(version="1.0", input_type=pd.DataFrame)
def clean(df: pd.DataFrame):
    return Table(df.dropna())
```

but at declaration time it is called with a
{py:class}`~pydiverse.pipedag.MaterializingTask`, not with a `pd.DataFrame`. Rewriting each
parameter type into "task producing that type" is not expressible in Python's type system — there
is no way to map a transformation over a
[`ParamSpec`](https://docs.python.org/3/library/typing.html#typing.ParamSpec).

Pipedag therefore solves this from the other side: {py:class}`~pydiverse.pipedag.MaterializingTask`
and {py:class}`~pydiverse.pipedag.MaterializingTaskGetItem` have `Any` among their base classes, the
same technique the standard library's type stubs use for `unittest.mock.Mock`. A task object is
assignable to any parameter type, so wiring never produces a false error.

The trade-off is deliberate: **task objects are opaque to the type checker**. Attribute access,
subscripting, and passing a task where a concrete type is expected are all accepted without
complaint. That is what makes lazy field access work:

```python
with Flow() as flow:
    with Stage("stage"):
        economic = economic_data()
        model(economic.aa, economic.bb)   # recorded, resolved at run time
```

## Annotating task functions

Annotations on task functions are not only documentation — pipedag reads them at run time. The
return annotation drives [dataframely](https://github.com/Quantco/dataframely) and colspec
validation as well as column type selection in the table store. Two consequences:

- **Do not use `from __future__ import annotations`** in modules that define tasks, and do not
  reference names that only exist under `if TYPE_CHECKING:` in a task signature. Pipedag calls
  {py:func}`typing.get_type_hints` on the decorated function at flow declaration time, which would
  fail to resolve them.

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

Pipedag's own public API assertions live in `tests/typing/` and are checked with both pyright and
mypy; pyright is authoritative. Note that the library's internals are *not* type-clean — pointing
a checker at `src/pydiverse/pipedag/` will report a large number of pre-existing errors that do not
affect the annotations you consume.
