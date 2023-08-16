from __future__ import annotations

import copy
import functools
import inspect
import uuid
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, overload

from pydiverse.pipedag._typing import CallableT
from pydiverse.pipedag.context import ConfigContext, RunContext, TaskContext
from pydiverse.pipedag.core.task import Task, TaskGetItem, UnboundTask
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.cache import TaskCacheInfo, task_cache_key
from pydiverse.pipedag.materialize.container import Blob, RawSql, Table
from pydiverse.pipedag.util import deep_map
from pydiverse.pipedag.util.computation_tracing import (
    ComputationTraceRef,
    ComputationTracerProxy,
)
from pydiverse.pipedag.util.hashing import stable_hash

if TYPE_CHECKING:
    from pydiverse.pipedag import Flow, Stage
    from pydiverse.pipedag.backend.table.base import TableHook
    from pydiverse.pipedag.materialize.store import PipeDAGStore


@overload
def materialize(
    *,
    name: str = None,
    input_type: type | tuple | dict[str, Any] = None,
    version: str = None,
    cache: Callable = None,
    lazy: bool = False,
    nout: int = 1,
) -> Callable[[CallableT], CallableT | UnboundMaterializingTask]:
    ...


@overload
def materialize(fn: CallableT, /) -> CallableT | UnboundMaterializingTask:
    ...


def materialize(
    fn: CallableT = None,
    *,
    name: str = None,
    input_type: type | tuple | dict[str, Any] = None,
    version: str = None,
    cache: Callable = None,
    lazy: bool = False,
    nout: int = 1,
):
    """Decorator to create a task whose outputs get materialized.

    This decorator takes a class and turns it into a :py:class:`MaterializingTask`.
    This means, that this function can only be used as part of a flow.
    Any outputs it produces get written to their appropriate storage backends
    (either the table or blob store). Additionally, any :py:class:`Table`
    or :py:class:`Blob` objects this task receives as an input get replaced
    with the appropriate object retrieved from the storage backend.
    In other words: All outputs from a materializing task get written to the store,
    and all inputs get retrieved from the store.

    Because of how caching is implemented, task inputs and outputs must all
    be "materializable". This means that they can only contain objects of
    the following types:
    ``dict``, ``list``, ``tuple``,
    ``int``, ``float``, ``str``, ``bool``, ``None``,
    ``datetime.date``, ``datetime.datetime``, ``pathlib.Path``,
    :py:class:`Table`, :py:class:`RawSql`, :py:class:`Blob`, or :py:class:`Stage`.

    :param fn:
        The function that gets executed by this task.
    :param name:
        The name of this task.
        If no `name` is provided, the name of `fn` is used instead.
    :param input_type:
        The data type as which to retrieve table objects from the store.
        All tables passed to this task get loaded from the table store and converted
        to this type.
    :param version:
        The version of this task.
        Unless the task is lazy, you always need to manually change this
        version number when you change the implementation to ensure that the
        task gets executed and the cache flushed.

        If the `version` is ``None`` and the task isn't lazy, then the task always
        gets executed, and all downstream tasks get invalidated.
    :param cache:
        An explicit cache function used to determine cache validity of the task inputs.

        This function gets called every time before the task gets executed.
        It gets called with the same arguments as the task.

        An explicit function for validating cache validity. If the output
        of this function changes while the source parameters are the same (e.g.
        the source is a filepath and `cache` loads data from this file), then the
        cache will be deemed invalid and is not used.
    :param lazy:
        Whether this task is lazy or not.

        Unlike a normal task, lazy tasks always get executed. However, if a lazy
        task produces a lazy table (e.g. a SQL query), the table store checks if
        the same query has been executed before. If this is the case, then the
        query doesn't get executed, and instead, the table gets copied from the cache.

        This behaviour is very useful, because you don't need to manually bump
        the `version` of a lazy task. This only works because for lazy tables
        generating the query is very cheap compared to executing it.
    :param nout:
        The number of objects returned by the task.
        If set, this allows unpacking and iterating over the results from the task.

    Example
    -------

    ::

        @materialize(version="1.0", input_type=pd.DataFrame)
        def task(df: pd.DataFrame) -> pd.DataFrame:
            df = ...  # Do something with the dataframe
            return Table(df)

        @materialize(lazy=True, input_type=sa.Table)
        def lazy_task(tbl: sa.Table) -> sa.Table:
            query = sa.select(tbl).where(...)
            return Table(query)

    You can use the `cache` argument to specify an explicit cache function.
    In this example we define a task that reads a dataframe from a csv file.
    Without specifying a cache function, this task would only get rerun when the
    `path` argument changes. Instead, we define a function that calculates a
    hash based on the contents of the csv file, and pass it to the `cache` argument
    of ``@materialize``. This means that the task now gets invalidated and rerun if
    either the `path` argument changes or if the content of the file pointed
    to by `path` changes::

        import hashlib

        def file_digest(path):
            with open(path, "rb") as f:
                return hashlib.file_digest(f, "sha256").hexdigest()

        @materialize(version="1.0", cache=file_digest)
        def task(path):
            df = pd.read_scv(path)
            return Table(df)

    Setting nout allows you to use unpacking assignment with the task result::

        @materialize(version="1.0", nout=2)
        def task():
            return 1, 2

        # Later, while defining a flow, you can use unpacking assignment
        # because you specified that the task returns 2 objects (nout=2).
        one, two = task()

    If a task returns a dictionary or a list, you can use square brackets to
    explicitly wire individual values / elements to task inputs::

        @materialize(version="1.0")
        def task():
            return {
                "x": [0, 1],
                "y": [2, 3],
            }

        # Later, while defining a flow
        # This would result in another_task being called with ([0, 1], 3).
        task_out = task()
        another_task(task_out["x"], task_out["y"][1])
    """
    if fn is None:
        return partial(
            materialize,
            name=name,
            input_type=input_type,
            version=version,
            cache=cache,
            lazy=lazy,
            nout=nout,
        )

    return UnboundMaterializingTask(
        fn,
        name=name,
        input_type=input_type,
        version=version,
        cache=cache,
        lazy=lazy,
        nout=nout,
    )


class UnboundMaterializingTask(UnboundTask):
    """A materializing task without any bound arguments.

    Instances of this class get initialized using the
    :py:func:`@materialize <pydiverse.pipedag.materialize>` decorator.
    By calling this object inside a ``with Flow(...)`` context,
    a :py:class:`~.MaterializingTask` gets created that binds the provided
    arguments to the task.

    >>> @materialize
    ... def some_task(arg):
    ...     ...
    ...
    >>> type(some_task)
    <class 'pydiverse.pipedag.materialize.core.UnboundMaterializingTask'>

    >>> with Flow() as f:
    ...     with Stage("stage"):
    ...         x = some_task(...)
    ...
    >>> type(x)
    <class 'pydiverse.pipedag.materialize.core.MaterializingTask'>

    When you call a `UnboundMaterializingTask` outside a flow definition context,
    then the original function (that was decorated with :py:func:`@materialize
    <pydiverse.pipedag.materialize>`) gets called. The only difference being, that
    any :py:class:`~.Table`, :py:class:`~.Blob` or :py:class:`~.RawSql` objects
    returned from the task get replaced with the object that they wrap:

    >>> import pandas as pd
    >>>
    >>> @materialize(input_type=pd.DataFrame)
    ... def double_it(df):
    ...     return Table(df * 2)
    ...
    >>> x = pd.DataFrame({"x": [0, 1, 2, 3]})
    >>> double_it(x)
       x
    0  0
    1  2
    2  4
    3  6


    """

    def __init__(
        self,
        fn: Callable,
        *,
        name: str = None,
        input_type: type = None,
        version: str = None,
        cache: Callable = None,
        lazy: bool = False,
        nout: int = 1,
    ):
        super().__init__(
            MaterializationWrapper(fn),
            name=name,
            nout=nout,
        )

        self._bound_task_type = MaterializingTask
        self._original_fn = fn

        self.input_type = input_type
        self.version = version
        self.cache = cache
        self.lazy = lazy

        if version is AUTO_VERSION:
            if input_type is None:
                # The input_type is used to determine which type of auto versioning
                # to use (either not supported, lazy or tracing)
                raise ValueError("Auto-versioning task must specify an input type")
            if lazy:
                raise ValueError(
                    "Task can't be lazy and auto-versioning at the same time"
                )

    def __call__(self, *args, **kwargs) -> MaterializingTask:
        return super().__call__(*args, **kwargs)  # type: ignore

    def _call_original_function(self, *args, **kwargs):
        result = self._original_fn(*args, **kwargs)

        def unwrap_mutator(x):
            if isinstance(x, (Task, Blob)):
                return x.obj
            if isinstance(x, RawSql):
                return x.sql
            return x

        return deep_map(result, unwrap_mutator)


class MaterializingTask(Task):
    """
    A pipedag task that materializes all its outputs.

    Instances of this class get initialized by calling a
    :py:class:`~.UnboundMaterializingTask`.

    As a user, you will encounter `MaterializingTask` objects, together with
    :py:class:`~.MaterializingTaskGetItem` objects, mostly during flow declaration,
    because they are used to bind the output (or parts of the output) from one
    task, to the input of another task.
    """

    def __init__(
        self,
        unbound_task: UnboundMaterializingTask,
        bound_args: inspect.BoundArguments,
        flow: Flow,
        stage: Stage,
    ):
        super().__init__(unbound_task, bound_args, flow, stage)

        self.input_type = unbound_task.input_type
        self._version = unbound_task.version
        self.cache = unbound_task.cache
        self.lazy = unbound_task.lazy

    @property
    def version(self):
        try:
            if v := TaskContext.get().override_version:
                return v
        except LookupError:
            pass

        return self._version

    @version.setter
    def version(self, version):
        try:
            TaskContext.get().override_version = version
        except LookupError:
            raise AttributeError(
                "Can't set task version outside of a TaskContext"
            ) from None

    def __getitem__(self, item) -> MaterializingTaskGetItem:
        """Construct a :py:class:`~.MaterializingTaskGetItem`.

        If the corresponding task returns an object that supports
        :external+python:py:meth:`__getitem__ <object.__getitem__>`,
        then this allows you to forward only parts of the task's output to
        the next task.

        Example
        -------
        ::

            @materialize
            def dict_task():
                return {"x": 1, "y": 2}

            @materialize
            def assert_equal(actual, expected):
                assert actual == expected

            with Flow():
                with Stage("stage"):
                    d = dict_task()

                    # Only pass parts of the dictionary returned by dict_task
                    # to the assert_equal tasks.
                    assert_equal(d["x"], 1)
                    assert_equal(d["y"], 2)

        """
        return MaterializingTaskGetItem(self, self, item)

    def run(self, inputs: dict[int, Any], **kwargs):
        # When running only a subset of an entire flow, not all inputs
        # get calculated during flow execution. As a consequence, we must load
        # those inputs from the cache.

        def replace_stage_with_pseudo_stage(x):
            if isinstance(x, (Table, Blob, RawSql)):
                x.stage = PseudoStage(x.stage, did_commit=True)
            return x

        for in_id, in_task in self.input_tasks.items():
            if in_id in inputs:
                continue

            store = ConfigContext.get().store
            cached_output, metadata = store.retrieve_most_recent_task_output_from_cache(
                in_task
            )

            cached_output = deep_map(cached_output, replace_stage_with_pseudo_stage)
            inputs[in_id] = cached_output

        return super().run(inputs, **kwargs)

    def get_output_from_store(self, as_type: type = None) -> Any:
        """Retrieves the output of the task from the cache.

        No guarantees are made regarding whether the returned values are still
        up-to-date and cache valid.

        :param as_type: The type as which tables produced by this task should
            be dematerialized. If no type is specified, the input type of
            the task is used.
        :return: The output of the task.
        :raise CacheError: if no outputs for this task could be found in the store.

        Example
        -------

        ::

            # Define some flow
            with Flow() as f:
                with Stage("stage_1"):
                    x = some_task()
                    ...

            # Get output BEFORE calling flow.run()
            df_x = x.get_output_from_store(as_type=pd.DataFrame)

        """
        return _get_output_from_store(self, as_type)


class MaterializingTaskGetItem(TaskGetItem):
    """Object that represents a subset of a :py:class:`~.MaterializingTask` output.

    Instances of this class get initialized by calling
    :py:class:`MaterializingTask.__getitem__`.
    """

    def __init__(
        self,
        task: MaterializingTask,
        parent: MaterializingTask | MaterializingTaskGetItem,
        item: Any,
    ):
        super().__init__(task, parent, item)

    def __getitem__(self, item) -> MaterializingTaskGetItem:
        """
        Same as :py:meth:`MaterializingTask.__getitem__`,
        except that it allows you to further refine the selection.
        """
        return super().__getitem__(item)

    def get_output_from_store(self, as_type: type = None) -> Any:
        """
        Same as :py:meth:`MaterializingTask.get_output_from_store()`,
        except that it only loads the required subset of the output.
        """
        return _get_output_from_store(self, as_type)


class MaterializationWrapper:
    """Function wrapper that contains all high level materialization logic

    :param fn: The function to wrap
    """

    def __init__(self, fn: Callable):
        functools.update_wrapper(self, fn)

        self.fn = fn
        self.fn_signature = inspect.signature(fn)

    def __call__(self, *args, **kwargs):
        """Function wrapper / materialization logic

        :param args: The arguments passed to the function
        :param _pipedag_task_: The `MaterializingTask` instance which called
            this wrapper.
        :param kwargs: The keyword arguments passed to the function
        :return: A copy of what the original function returns annotated
            with some additional metadata.
        """

        task_context = TaskContext.get()
        config_context = ConfigContext.get()
        run_context = RunContext.get()

        task: MaterializingTask = task_context.task  # type: ignore
        bound = self.fn_signature.bind(*args, **kwargs)

        if task is None:
            raise TypeError("Task can't be None.")

        # If this is the first task in this stage to be executed, ensure that
        # the stage has been initialized and locked.
        store = config_context.store
        store.ensure_stage_is_ready(task.stage)

        # Compute the cache key for the task inputs
        input_json = store.json_encode(bound.arguments)
        input_hash = stable_hash("INPUT", input_json)

        cache_fn_hash = ""
        if task.cache is not None:
            cache_fn_output = store.json_encode(task.cache(*args, **kwargs))
            cache_fn_hash = stable_hash("CACHE_FN", cache_fn_output)

        memo_cache_key = task_cache_key(task, input_hash, cache_fn_hash)

        # Check if this task has already been run with the same inputs
        # If yes, return memoized result. This prevents DuplicateNameExceptions
        with run_context.task_memo(task, memo_cache_key) as (success, memo):
            if success:
                task.logger.info(
                    "Task has already been run with the same inputs."
                    " Using memoized results."
                )

                return memo

            if task.version is AUTO_VERSION:
                assert not task.lazy

                auto_version = self.compute_auto_version(task, store, bound)
                task.version = auto_version
                task.logger.info("Assigned auto version to task", version=auto_version)

            # Cache Lookup (only required if task isn't lazy)
            force_task_execution = config_context._force_task_execution
            skip_cache_lookup = (
                config_context.ignore_task_version or task.version is None
            )

            if not task.lazy and not skip_cache_lookup and not force_task_execution:
                try:
                    cached_output, cache_metadata = store.retrieve_cached_output(
                        task, input_hash, cache_fn_hash
                    )
                    store.copy_cached_output_to_transaction_stage(
                        cached_output, cache_metadata, task
                    )
                    run_context.store_task_memo(task, memo_cache_key, cached_output)
                    task.logger.info("Found task in cache. Using cached result.")
                    TaskContext.get().is_cache_valid = True
                    return cached_output
                except CacheError as e:
                    task.logger.info("Failed to retrieve task from cache", cause=str(e))
                    TaskContext.get().is_cache_valid = False

            if task.lazy:
                # For lazy tasks, is_cache_valid gets set to false during the
                # store.materialize_task procedure
                TaskContext.get().is_cache_valid = True

                if config_context.ignore_cache_function:
                    # `cache_fn_hash` is not used for cache retrieval if
                    # ignore_cache_function is set to True.
                    # In that case, cache_metadata.cache_fn_hash may be different
                    # from the cache_fn_hash of the current task run.

                    try:
                        _, cache_metadata = store.retrieve_cached_output(
                            task, input_hash, cache_fn_hash
                        )
                        cache_fn_hash = cache_metadata.cache_fn_hash
                    except CacheError as e:
                        task.logger.info(
                            "Failed to retrieve task from cache", cause=str(e)
                        )

            # Prepare TaskCacheInfo
            if not task.lazy and skip_cache_lookup:
                # Assign random version to disable caching for this task
                task = copy.copy(task)
                task.version = uuid.uuid4().hex

            task_cache_info = TaskCacheInfo(
                task=task,
                input_hash=input_hash,
                cache_fn_hash=cache_fn_hash,
                cache_key=task_cache_key(task, input_hash, cache_fn_hash),
            )

            # Compute the input_tables value of the TaskContext
            input_tables = []

            def _input_tables_visitor(x):
                if isinstance(x, Table):
                    input_tables.append(x)
                return x

            deep_map(bound.arguments.values(), _input_tables_visitor)
            task_context.input_tables = input_tables

            # Not found in cache / lazy -> Evaluate Function
            args, kwargs = store.dematerialize_task_inputs(
                task, bound.args, bound.kwargs
            )

            result = self.fn(*args, **kwargs)
            result = store.materialize_task(task, task_cache_info, result)

            # Delete underlying objects from result (after materializing them)
            def obj_del_mutator(x):
                if isinstance(x, (Table, Blob)):
                    x.obj = None
                if isinstance(x, RawSql):
                    x.loaded_tables = None
                return x

            result = deep_map(result, obj_del_mutator)
            run_context.store_task_memo(task, memo_cache_key, result)
            self.value = result

            return result

    def compute_auto_version(
        self,
        task: MaterializingTask,
        store: PipeDAGStore,
        bound: inspect.BoundArguments,
    ) -> str:
        # Decide on which type of auto versioning to use.
        # For this, we go off the `auto_version_support` flag of the retrieve
        # table hook of the input type
        from pydiverse.pipedag.backend.table.base import AutoVersionSupport

        hook = store.table_store.get_r_table_hook(task.input_type)
        auto_version_support = hook.auto_version_support

        if auto_version_support == AutoVersionSupport.NONE:
            raise TypeError(
                f"Auto versioning not supported for input type {task.input_type}."
            )

        # Perform auto versioning
        info = None
        if auto_version_support == AutoVersionSupport.LAZY:
            info = self.compute_auto_version_lazy(task, hook, store, bound)
        elif auto_version_support == AutoVersionSupport.TRACE:
            info = self.compute_auto_version_trace(task, hook, store, bound)

        assert isinstance(info, str)

        auto_version = "auto_" + stable_hash("AUTO-VERSION", info)
        return auto_version

    def compute_auto_version_lazy(
        self,
        task: MaterializingTask,
        hook: type[TableHook],
        store: PipeDAGStore,
        bound: inspect.BoundArguments,
    ) -> str:
        args, kwargs = store.dematerialize_task_inputs(
            task, bound.args, bound.kwargs, for_auto_versioning=True
        )

        try:
            result = self.fn(*args, **kwargs)
        except Exception as e:
            raise RuntimeError("Auto versioning failed") from e

        result, tables = self._auto_version_prepare_and_check_task_output(
            task, store, result
        )

        if len(tables) == 0:
            raise ValueError(
                f"Task with version={AUTO_VERSION} must return at least one Table."
            )

        # Compute auto version
        auto_version_info = []
        for table in tables:
            obj = table.obj
            auto_version_info.append(hook.get_auto_version_lazy(obj))

        return store.json_encode(
            {
                "task_output": result,
                "auto_version_info": auto_version_info,
            }
        )

    def compute_auto_version_trace(
        self,
        task: MaterializingTask,
        hook: type[TableHook],
        store: PipeDAGStore,
        bound: inspect.BoundArguments,
    ) -> str | None:
        computation_tracer = hook.get_computation_tracer()

        def tracer_proxy_mapper(x):
            if isinstance(x, Table):
                return computation_tracer.create_proxy(("TABLE", x.stage.name, x.name))
            if isinstance(x, RawSql):
                return computation_tracer.create_proxy(
                    ("RAW_SQL", x.stage.name, x.name, x.cache_key)
                )
            if isinstance(x, Blob):
                return computation_tracer.create_proxy(("BLOB", x.stage.name, x.name))

            return x

        args = deep_map(bound.args, tracer_proxy_mapper)
        kwargs = deep_map(bound.kwargs, tracer_proxy_mapper)

        with computation_tracer:
            try:
                result = self.fn(*args, **kwargs)
            except Exception as e:
                raise RuntimeError("Auto versioning failed") from e

        result, tables = self._auto_version_prepare_and_check_task_output(
            task, store, result
        )

        # Compute auto version
        trace_hash = computation_tracer.trace_hash()
        table_info = []

        # Include the computation tracers in the returned tables in the version
        # to invalidate the task when outputs get switched
        for table in tables:
            obj = table.obj
            assert isinstance(obj, ComputationTracerProxy)
            table_info.append(ComputationTraceRef(obj).id)

        return store.json_encode(
            {
                "task_output": result,
                "table_info": table_info,
                "trace_hash": trace_hash,
            }
        )

    def _auto_version_prepare_and_check_task_output(
        self, task: MaterializingTask, store: PipeDAGStore, output
    ):
        task_cache_info = TaskCacheInfo(
            task=task,
            input_hash="AUTO_VERSION",
            cache_fn_hash="AUTO_VERSION",
            cache_key="AUTO_VERSION",
        )

        # Replace computation tracer proxy objects in output
        def replace_proxy(x):
            if isinstance(x, ComputationTracerProxy):
                return {
                    "__pipedag_type__": "computation_tracer_proxy",
                    "id": ComputationTraceRef(x).id,
                }
            return x

        output = deep_map(output, replace_proxy)
        output, tables, raw_sqls, blobs = store.prepare_task_output_for_materialization(
            task, task_cache_info, output
        )

        if len(raw_sqls) != 0:
            raise ValueError(f"Task with version={AUTO_VERSION} can't return RawSql.")

        if len(blobs) != 0:
            raise ValueError(f"Task with version={AUTO_VERSION} can't return Blobs.")

        return output, tables


def _get_output_from_store(
    task: MaterializingTask | MaterializingTaskGetItem, as_type: type
) -> Any:
    """Helper to retrieve task output from store"""
    from pydiverse.pipedag.context.run_context import DematerializeRunContext
    from pydiverse.pipedag.materialize.store import dematerialize_output_from_store

    root_task = task if isinstance(task, Task) else task.task

    store = ConfigContext.get().store
    with DematerializeRunContext(root_task.flow):
        cached_output, _ = store.retrieve_most_recent_task_output_from_cache(root_task)
        return dematerialize_output_from_store(store, task, cached_output, as_type)


class PseudoStage:
    def __init__(self, stage: Stage, did_commit: bool):
        self._stage = stage
        self._name = stage.name
        self._transaction_name = stage.name
        self._did_commit = did_commit

        self.id = stage.id

    @property
    def name(self) -> str:
        return self._name

    @property
    def transaction_name(self) -> str:
        return self._transaction_name

    @property
    def current_name(self) -> str:
        if self._did_commit:
            return self.name
        else:
            return self.transaction_name


class AutoVersionType:
    """
    Special constant used to indicate that pipedag should automatically determine
    a version number for a task.

    The version is determined by running the task once to construct a representation
    of the computation performed on the tables (e.g. construct a query plan /
    computational graph).
    Using this representation, a unique version number is constructed such that
    if the computation changes the version number also changes.
    Then, if the task is deemed to be cache invalid, it is run again, but this
    time with actual data.

    This puts the following constraints on which tasks can use `AUTO_VERSION`:

    * The task must be a pure function.
    * The task may not inspect the contents of the input tables.
    * The task must return at least one table.
    * The task may not return :py:class:`RawSql`.
    * The task may not return :py:class:`Blob`.

    .. rubric:: Polars

    Automatic versioning is best supported by polars.
    To use it, you must specify
    :external+pl:doc:`polars.LazyFrame <reference/lazyframe/index>`
    as the task input type and only use LazyFrames inside your task.

    .. rubric:: Pandas

    Pandas support is still *experimental*. It is implemented using a technique
    we call "computation tracing", where we run the task with proxy tables
    as inputs that record all operations performed on them.
    This requires heavily monkey patching the pandas and numpy modules.
    As long as you only use pandas and numpy functions inside your task,
    computation tracing should work successfully.
    However, for the monkey patching to work, you only access pandas / numpy
    functions through their namespace (e.g. ``pd.concat(...)`` is allowed, while
    ``from pandas import concat; concat(...)`` is not allowed).

    Requires dask to be installed.


    Example
    -------
    ::

        @materialize(input_type=pl.LazyFrame, version=AUTO_VERSION)
        def polars_task(x: pl.LazyFrame):
            # Some operations that only utilize pl.LazyFrame...
            return x.with_columns(
                (pl.col("col1") * pl.col("col2")).alias("col3")
            )

    """

    def __str__(self):
        return "AUTO_VERSION"

    def __repr__(self):
        return "AUTO_VERSION"


AUTO_VERSION = AutoVersionType()
