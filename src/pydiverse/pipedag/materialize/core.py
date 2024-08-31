from __future__ import annotations

import copy
import functools
import inspect
import uuid
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Any, overload

import sqlalchemy as sa

from pydiverse.pipedag._typing import CallableT
from pydiverse.pipedag.container import Blob, RawSql, Table
from pydiverse.pipedag.context import ConfigContext, RunContext, TaskContext
from pydiverse.pipedag.context.context import (
    CacheValidationMode,
)
from pydiverse.pipedag.core.task import Task, TaskGetItem, UnboundTask
from pydiverse.pipedag.errors import CacheError
from pydiverse.pipedag.materialize.cache import TaskCacheInfo, task_cache_key
from pydiverse.pipedag.util import deep_map
from pydiverse.pipedag.util.computation_tracing import (
    ComputationTraceRef,
    ComputationTracerProxy,
)
from pydiverse.pipedag.util.hashing import stable_hash
from pydiverse.pipedag.util.json import PipedagJSONEncoder

if TYPE_CHECKING:
    from pydiverse.pipedag import Flow, Stage
    from pydiverse.pipedag.backend.table.base import TableHook
    from pydiverse.pipedag.materialize.store import PipeDAGStore


@overload
def materialize(
    *,
    name: str | None = None,
    input_type: type | None = None,
    version: str | None = None,
    cache: Callable[..., Any] | None = None,
    lazy: bool = False,
    nout: int = 1,
    add_input_source: bool = False,
    ordering_barrier: bool | dict[str, Any] = False,
    call_context: Callable[[], Any] | None = None,
) -> Callable[[CallableT], CallableT | UnboundMaterializingTask]:
    ...


@overload
def materialize(fn: CallableT, /) -> CallableT | UnboundMaterializingTask:
    ...


def materialize(
    fn: CallableT | None = None,
    *,
    name: str | None = None,
    input_type: type | None = None,
    version: str | None = None,
    cache: Callable[..., Any] | None = None,
    lazy: bool = False,
    group_node_tag: str | None = None,
    nout: int = 1,
    add_input_source: bool = False,
    ordering_barrier: bool | dict[str, Any] = False,
    call_context: Callable[[], Any] | None = None,
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
        to this type. See :doc:`Table Backends </table_backends>` for
        more information.
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
    :param group_node_tag:
        Set a tag that may add this task to a configuration based group node.
    :param nout:
        The number of objects returned by the task.
        If set, this allows unpacking and iterating over the results from the task.
    :param add_input_source:
        If true, Table and Blob objects are provided as tuple together with the
        dematerialized object. ``task(a=tbl)`` will result in
        ``a=(dematerialized_tbl, tbl)`` in the task.
    :param ordering_barrier:
        If true, the task will be surrounded by a GroupNode(ordering_barrier=True).
        If a dictionary is provided, it is surrounded by a
        GroupNode(**ordering_barrier). This allows passing style, style_tag, and label
        arguments.
    :param call_context:
        An optional context manager function that is opened before the task or its
        optional cache function is called and closed afterward.

    Example
    -------

    ::

        @materialize(version="1.0", input_type=pd.DataFrame)
        def task(df: pd.DataFrame) -> pd.DataFrame:
            df = ...  # Do something with the dataframe
            return Table(df)

        @materialize(lazy=True, input_type=sa.Table)
        def lazy_task(tbl: sa.Alias) -> sa.Table:
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
            group_node_tag=group_node_tag,
            nout=nout,
            add_input_source=add_input_source,
            ordering_barrier=ordering_barrier,
            call_context=call_context,
        )

    return UnboundMaterializingTask(
        fn,
        name=name,
        input_type=input_type,
        version=version,
        cache=cache,
        lazy=lazy,
        group_node_tag=group_node_tag,
        nout=nout,
        add_input_source=add_input_source,
        ordering_barrier=ordering_barrier,
        call_context=call_context,
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
        name: str | None = None,
        input_type: type | None = None,
        version: str | None = None,
        cache: Callable[..., Any] | None = None,
        lazy: bool = False,
        group_node_tag: str | None = None,
        nout: int = 1,
        add_input_source: bool = False,
        ordering_barrier: bool | dict[str, Any] = False,
        call_context: Callable[[], Any] | None = None,
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
        self.group_node_tag = group_node_tag
        self.add_input_source = add_input_source
        self.call_context = call_context

        if isinstance(ordering_barrier, bool):
            group_node_args = {"ordering_barrier": ordering_barrier}
        elif isinstance(ordering_barrier, dict):
            group_node_args = ordering_barrier.copy()
            if "ordering_barrier" not in group_node_args:
                group_node_args["ordering_barrier"] = True

            group_node_keys = {"ordering_barrier", "label", "style", "style_tag"}
            unexpected_keys = set(ordering_barrier.keys()) - group_node_keys
            if unexpected_keys:
                raise ValueError(
                    "If ordering_barrier is a dictionary, it must only contain keys "
                    "that are valid parameters of class GroupNode constructor."
                    f"{unexpected_keys} are unexpected."
                )
        else:
            raise ValueError(
                "The ordering_barrier parameter must be a boolean or a dictionary."
            )
        self.group_node_args = group_node_args

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
        if self.group_node_args["ordering_barrier"]:
            from pydiverse.pipedag import GroupNode

            with GroupNode(**self.group_node_args):
                return super().__call__(*args, **kwargs)  # type: ignore
        return super().__call__(*args, **kwargs)  # type: ignore

    def _call_original_function(self, *args, **kwargs):
        try:
            task_context = TaskContext.get()  # this may raise Lookup Error

            # this is a subtask call. Thus we demand identical input_types
            sub_task: MaterializingTask = task_context.task  # type: ignore
            if (
                sub_task.input_type is not None
                and self.input_type is not None
                and sub_task.input_type != self.input_type
            ):
                raise RuntimeError(
                    f"Subtask input type {self.input_type} does not match parent task "
                    f"input type {sub_task.input_type}: "
                    f"task={self}, sub_task={sub_task}"
                )
        except LookupError:
            # this is expected when calling the task outside of flow declaration
            pass
        result = self._original_fn(*args, **kwargs)

        def unwrap_mutator(x):
            if isinstance(x, (Table, Blob)):
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
        self.add_input_source = unbound_task.add_input_source
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

    def run(
        self, inputs: dict[int, Any], ignore_position_hashes: bool = False, **kwargs
    ):
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
                in_task, ignore_position_hashes
            )

            cached_output = deep_map(cached_output, replace_stage_with_pseudo_stage)
            inputs[in_id] = cached_output

        return super().run(inputs, **kwargs)

    def get_output_from_store(
        self, as_type: type = None, ignore_position_hashes: bool = False
    ) -> Any:
        """Retrieves the output of the task from the cache.

        No guarantees are made regarding whether the returned values are still
        up-to-date and cache valid.

        :param as_type: The type as which tables produced by this task should
            be dematerialized. If no type is specified, the input type of
            the task is used.
        :param ignore_position_hashes:
            If ``True``, the position hashes of tasks are not checked
            when retrieving the inputs of a task from the cache.
            This simplifies execution of subgraphs if you don't care whether inputs to
            that subgraph are cache invalid. This allows multiple modifications in the
            Graph before the next run updating the cache.
            Attention: This may break automatic cache invalidation.
            And for this to work, any task producing an input
            for the chosen subgraph may never be used more
            than once per stage.
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
        return _get_output_from_store(
            self, as_type, ignore_position_hashes=ignore_position_hashes
        )


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

    def get_output_from_store(
        self, as_type: type = None, ignore_position_hashes: bool = False
    ) -> Any:
        """
        Same as :py:meth:`MaterializingTask.get_output_from_store()`,
        except that it only loads the required subset of the output.
        """
        return _get_output_from_store(
            self, as_type, ignore_position_hashes=ignore_position_hashes
        )


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
            if config_context.cache_validation.disable_cache_function:
                # choose random cache_fn_hash to ensure downstream tasks are
                # invalidated for FORCE_FRESH_INPUT
                cache_fn_hash = stable_hash("CACHE_FN", uuid.uuid4().hex)
            else:
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

            # Cache Lookup (only required if task isn't lazy)
            force_task_execution = (
                config_context.cache_validation.mode
                == CacheValidationMode.FORCE_CACHE_INVALID
                or (
                    config_context.cache_validation.mode
                    == CacheValidationMode.FORCE_FRESH_INPUT
                    and task.cache is not None
                )
            )
            skip_cache_lookup = (
                config_context.cache_validation.ignore_task_version
                and task.version != AUTO_VERSION
            ) or task.version is None
            assert_no_fresh_input = (
                config_context.cache_validation.mode
                == CacheValidationMode.ASSERT_NO_FRESH_INPUT
            )

            if task.version is AUTO_VERSION:
                if (
                    force_task_execution
                    and config_context.cache_validation.disable_cache_function
                ):
                    task.version = None
                    skip_cache_lookup = True
                    task.logger.info(
                        "Skipping determination of auto version of task due to forced "
                        "execution and disable_cache_function=True"
                    )
                else:
                    assert not task.lazy

                    auto_version = self.compute_auto_version(task, store, bound)
                    task.version = auto_version
                    task.logger.info(
                        "Assigned auto version to task", version=auto_version
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
                    task.logger.info(
                        "Found task in cache. Using cached result.",
                        input_hash=input_hash,
                        cache_fn_hash=cache_fn_hash,
                    )
                    TaskContext.get().is_cache_valid = True
                    run_context.trace_hook.task_cache_status(
                        task, input_hash, cache_fn_hash, cache_metadata, cached_output
                    )
                    return cached_output
                except CacheError as e:
                    task.logger.info(
                        "Failed to retrieve task from cache",
                        input_hash=input_hash,
                        cache_fn_hash=cache_fn_hash,
                        version=task.version,
                        cause=str(e),
                    )
                    TaskContext.get().is_cache_valid = False
                    run_context.trace_hook.task_cache_status(
                        task, input_hash, cache_fn_hash, cache_valid=False
                    )

            if not task.lazy:
                if assert_no_fresh_input and task.cache is not None:
                    raise AssertionError(
                        "cache_validation.mode=ASSERT_NO_FRESH_INPUT is a "
                        "protection mechanism to prevent execution of "
                        "source tasks to keep pipeline input stable. However,"
                        "this task was still triggered."
                    ) from None

            if task.lazy:
                # For lazy tasks, is_cache_valid gets set to false during the
                # store.materialize_task procedure
                TaskContext.get().is_cache_valid = True

                if (
                    config_context.cache_validation.mode
                    == CacheValidationMode.IGNORE_FRESH_INPUT
                ):
                    # `cache_fn_hash` is not used for cache retrieval if
                    # ignore_cache_function is set to True.
                    # In that case, cache_metadata.cache_fn_hash may be different
                    # from the cache_fn_hash of the current task run.

                    try:
                        _, cache_metadata = store.retrieve_cached_output(
                            task, input_hash, ""
                        )
                        cache_fn_hash = cache_metadata.cache_fn_hash
                        run_context.trace_hook.task_cache_status(
                            task, input_hash, cache_fn_hash, cache_metadata, lazy=True
                        )
                    except CacheError as e:
                        task.logger.info(
                            "Failed to retrieve lazy task from cache",
                            cause=str(e),
                            input_hash=input_hash,
                            version=task.version,
                            cache_fn_hash=cache_fn_hash,
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
                assert_no_materialization=assert_no_fresh_input,
                force_task_execution=force_task_execution,
            )

            # Compute the input_tables value of the TaskContext
            input_tables = []

            def _input_tables_visitor(x):
                if isinstance(x, Table):
                    input_tables.append(x)
                return x

            deep_map(bound.arguments, _input_tables_visitor)
            task_context.input_tables = input_tables

            # Not found in cache / lazy -> Evaluate Function
            args, kwargs = store.dematerialize_task_inputs(
                task, bound.args, bound.kwargs
            )

            def imperative_materialize(
                table: Table,
                config_context: ConfigContext | None,
                return_as_type: type | None = None,
                return_nothing: bool = False,
            ):
                my_store = config_context.store if config_context is not None else store
                state = task_cache_info.imperative_materialization_state
                if id(table) in state.table_ids:
                    raise RuntimeError(
                        "The table has already been imperatively materialized."
                    )
                table.assumed_dependencies = (
                    list(sorted(state.assumed_dependencies))
                    if len(state.assumed_dependencies) > 0
                    else []
                )
                _ = my_store.materialize_task(
                    task, task_cache_info, table, disable_task_finalization=True
                )
                if not return_nothing:

                    def get_return_obj(return_as_type):
                        if return_as_type is None:
                            return_as_type = task.input_type
                            if (
                                return_as_type is None
                                or not my_store.table_store.get_r_table_hook(
                                    return_as_type
                                ).retrieve_as_reference(return_as_type)
                            ):
                                # dematerialize as sa.Table if it would transfer all
                                # rows to python when dematerializing with input_type
                                return_as_type = sa.Table
                        obj = my_store.dematerialize_item(
                            table, return_as_type, run_context
                        )
                        state.add_table_lookup(obj, table)
                        return obj

                    if isinstance(return_as_type, Iterable):
                        return tuple(get_return_obj(t) for t in return_as_type)
                    else:
                        return get_return_obj(return_as_type)

            task_context.imperative_materialize_callback = imperative_materialize
            run_context.trace_hook.task_pre_call(task)
            result = self.fn(*args, **kwargs)
            run_context.trace_hook.task_post_call(task)
            task_context.imperative_materialize_callback = None
            if task.debug_tainted:
                raise RuntimeError(
                    f"The task {task.name} has been tainted by interactive debugging."
                    f" Aborting."
                )

            def result_finalization_mutator(x):
                state = task_cache_info.imperative_materialization_state
                object_lookup = state.object_lookup
                if id(x) in object_lookup:
                    # substitute imperatively materialized object references with
                    # their respective table objects
                    x = object_lookup[id(x)]
                if isinstance(x, (Table, RawSql)):
                    # fill assumed_dependencies for Tables that were not yet
                    # materialized
                    if len(state.assumed_dependencies) > 0:
                        if x.assumed_dependencies is None:
                            x.assumed_dependencies = list(
                                sorted(state.assumed_dependencies)
                            )
                return x

            result = deep_map(result, result_finalization_mutator)
            result = store.materialize_task(task, task_cache_info, result)

            # Delete underlying objects from result (after materializing them)
            def obj_del_mutator(x):
                if isinstance(x, (Table, Blob)):
                    x.obj = None
                if isinstance(x, RawSql):
                    x.loaded_tables = None
                if isinstance(x, (Table, RawSql)):
                    x.assumed_dependencies = deep_map(
                        x.assumed_dependencies, obj_del_mutator
                    )
                return x

            result = deep_map(result, obj_del_mutator)
            run_context.store_task_memo(task, memo_cache_key, result)
            self.value = result
            run_context.trace_hook.task_complete(task, result)

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
            assert_no_materialization=False,
            force_task_execution=False,
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


@overload
def input_stage_versions(
    *,
    name: str | None = None,
    input_type: type | None = None,
    version: str | None = None,
    cache: Callable[..., Any] | None = None,
    lazy: bool = False,
    group_node_tag: str | None = None,
    nout: int = 1,
    add_input_source: bool = False,
    ordering_barrier: bool | dict[str, Any] = True,
    call_context: Callable[[], Any] | None = None,
    include_views=True,
    lock_source_stages=True,
    pass_args: Iterable[str] = tuple(),
) -> Callable[[CallableT], CallableT | UnboundMaterializingTask]:
    ...


@overload
def input_stage_versions(fn: CallableT, /) -> CallableT | UnboundMaterializingTask:
    ...


def input_stage_versions(
    fn: CallableT | None = None,
    *,
    name: str | None = None,
    input_type: type | None = None,
    version: str | None = None,
    cache: Callable[..., Any] | None = None,
    lazy: bool = False,
    group_node_tag: str | None = None,
    nout: int = 1,
    add_input_source: bool = False,
    ordering_barrier: bool | dict[str, Any] = True,
    call_context: Callable[[], Any] | None = None,
    include_views=True,
    lock_source_stages=True,
    pass_args: Iterable[str] = tuple(),
):
    """
    A decorator that marks a function as a task that receives tables from two versions
    of the same stage. This can either be the currently active schema of this stage
    before schema swapping, or it can be an active schema of another pipeline instance.

    The arguments to the resulting task are significantly different from a task created
    with `@materialize` decorator. An arbitrary expression with lists, tuples, dicts,
    and Table/Blob/RawSQL references can be provided. This decorator will only collect
    tables referenced and will pass them as one dictionary per version to the task. It
    maps table names to some reference for the respective stage version in the form
    specified by input_type parameter.

    When a ConfigContext object is passed as toplevel parameter or named parameter in
    the wiring call of decorated task, it is used for referencing the second stage
    version. The idea is that the other configuration is just another pipeline instance
    accessible with the same SQLAlchemy engine of the table store. It may work also if
    this is not the case, but it may be harder for the task to process the dictionaries
    it receives. Currently, it is not allowed to pass more than one ConfigContext
    object.

    When no parameters (except for an optional ConfigContext) is passed, all currently
    existing tables of both stage versions (or their respective schema) are passed in
    the dictionaries to the task.

    With pass_blobs=True, two additional dictionaries are passed to the task at the end.
    In addition to the arguments mentioned above, pass_args can be used to feed defined
    keyword arguments through to the decorated task.

    :param fn:
        The function that gets executed by this task.
    :param name:
        The name of this task.
        If no `name` is provided, the name of `fn` is used instead.
    :param input_type:
        The data type as which to retrieve table objects from the store.
        All tables passed to this task get loaded from the table store and converted
        to this type. See :doc:`Table Backends </table_backends>` for
        more information.
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
    :param group_node_tag:
        Set a tag that may add this task to a configuration based group node.
    :param nout:
        The number of objects returned by the task.
        If set, this allows unpacking and iterating over the results from the task.
    :param add_input_source:
        If true, Table and Blob objects are provided as tuple together with the
        dematerialized object. ``task(a=tbl)`` will result in
        ``a=(dematerialized_tbl, tbl)`` in the task.
    :param ordering_barrier:
        If true, the task will be surrounded by a GroupNode(ordering_barrier=True).
        If a dictionary is provided, it is surrounded by a
        GroupNode(**ordering_barrier). This allows passing style, style_tag, and label
        arguments.
        Attention: In contrast to @materialize, the default value is True.
    :param call_context:
        An optional context manager function that is opened before the task or its
        optional cache function is called and closed afterward.
    :param include_views:
        In case no explicit table references are given, if true, include views when
        collecting all tables of both stage versions.
    :param lock_source_stages:
        If true, lock the other stage version when a ConfigContext object is passed to
        this task.
    :param pass_args
        A list of named arguments that whould be passed from the call to the task
        function. By default, no arguments are passed, and just tables are extracted.

    Example
    -------

    ::

        @input_stage_versions(lazy=True, input_type=sa.Table)
        def validate_stage(
            transaction: dict[str, sa.Alias],
            other: dict[str, sa.Alias],
        ):
            compare_tables(transaction, other)

        def get_flow():
            with Flow() as f:
                a = produce_a_table()
                validate_stage()  # implicitly receives all tables written before
    """
    from pydiverse.pipedag import Stage

    forward_args = dict(
        name=name,
        input_type=input_type,
        version=version,
        cache=cache,
        lazy=lazy,
        group_node_tag=group_node_tag,
        nout=nout,
        add_input_source=add_input_source,
        ordering_barrier=ordering_barrier,
        call_context=call_context,
        include_views=include_views,
        lock_source_stages=lock_source_stages,
        pass_args=pass_args,
    )

    if fn is None:
        return partial(input_stage_versions, **forward_args)

    class OtherStageLockManager:
        def __init__(self):
            self.stages = None
            self.lock_manager = None

        @contextmanager
        def __call__(self):
            yield
            self.release_all()

        def release_all(self):
            # clear locks after either cache_fn or cache_fn and task function were
            # called
            if self.lock_manager is not None:
                for stage in sorted(self.stages, reverse=True):
                    self.lock_manager.release(stage.name)
                self.lock_manager.dispose()
                self.lock_manager = None

        def memorize(self, stages, lock_manager):
            self.release_all()
            self.stages = stages
            self.lock_manager = lock_manager

    other_stage_lock_manager = OtherStageLockManager()

    def cache_fn(*args, **kwargs):
        user_cache_fn = cache
        if user_cache_fn is None:
            # we don't care about the cache function, but we still need to cache the
            # arguments that are also used to call the task itself
            def user_cache_fn(*args, **kwargs):
                return None

        ret, cfg2, stages, lock_manager = _call_fn(
            user_cache_fn,
            args,
            kwargs,
            input_type=None,
            is_dematerialized=False,
            lock_source_stages=lock_source_stages,
        )
        other_stage_lock_manager.memorize(stages, lock_manager)
        if cfg2 is None:
            return uuid.uuid4().hex  # return random hash to ensure invalidation
        # get a hash of all task outputs of all relevant stage from other pipeline
        # instance
        ret = stable_hash(ret)
        for stage in stages:
            try:
                ret = ret + cfg2.store.table_store.get_stage_hash(stage)
            except sa.exc.ProgrammingError:
                # this happens for example if the other instance did not run, yet
                return uuid.uuid4().hex  # return random hash to ensure invalidation
        return ret

    # extract @materialize arguments from forward_args via __code__.co_varnames:
    n_args = materialize.__code__.co_argcount + materialize.__code__.co_kwonlyargcount
    arg_names = materialize.__code__.co_varnames[0:n_args]
    materialize_args = {k: v for k, v in forward_args.items() if k in arg_names}
    materialize_args["add_input_source"] = True  # we need the names
    materialize_args["cache"] = cache_fn
    if materialize_args.get("name") is None:
        materialize_args["name"] = getattr(fn, "__name__", "input_stage_versions-task")
    materialize_args["call_context"] = other_stage_lock_manager

    # For every input_state_versions-Task, we create a materialize-Task and give it the
    # same name as the input_state_versions-Task should get. The @materialize decorator
    # will dematerialize any Table/Blob/RawSQL objects passed to the
    # input_state_versions-Task. We pass add_input_source=True, so we still receive the
    # Table/Blob/RawSQL reference objects, so we can use them to dematerialize the
    # second stage version. Both are arranged in dictionaries indexable by table name or
    # blob name and handed over to the function annotated with @input_stage_versions.
    @materialize(**materialize_args)
    def task(*args, **kwargs):
        # the locks are already acquired by cache_fn if it is called
        lock_stages = (
            False
            if other_stage_lock_manager.lock_manager is not None
            else lock_source_stages
        )
        ret, _, _, _ = _call_fn(
            fn,
            args,
            kwargs,
            input_type=input_type,
            is_dematerialized=True,
            lock_source_stages=lock_stages,
        )
        return ret

    def _call_fn(
        _fn: Callable[..., Any],
        args: tuple,
        kwargs: dict[str, Any],
        *,
        input_type,
        is_dematerialized,
        lock_source_stages,
    ):
        _task = TaskContext.get().task
        stage = _task.stage
        pass_kwargs = {k: v for k, v in kwargs.items() if k in set(pass_args)}

        (
            stages,
            table_dict,
            blob_dict,
            raw_sqls,
            other_cfg,
            any_data_arguments_collected,
        ) = _collect_arguments(args, kwargs, is_dematerialized)
        stages.add(stage)

        # The connection to the other stage version configuration can either be the same
        # (in this case, we provide the active stage cache before swapping) or another
        # pipeline instance (in this case, we also take the active stage cache schema
        # of the same stage in the other instance)
        cfg1 = ConfigContext.get()
        cfg2 = other_cfg
        if cfg2 is None:
            cfg2 = cfg1
        stage2 = Stage(
            stage.name,
            stage.materialization_details,
            stage.group_node_tag,
            force_committed=True,
        )
        stage2.id = stage.id
        if lock_source_stages and other_cfg is not None:
            lock_manager = cfg2.create_lock_manager()
        else:
            lock_manager = None
            lock_source_stages = False

        transaction_table_dict, other_table_dict = {}, {}
        transaction_blob_dict, other_blob_dict = {}, {}
        try:
            # lock the source stages if they are read from another instance
            if lock_source_stages:
                for stage in sorted(stages):
                    lock_manager.acquire(stage.name)
            # cnt[0] counts the number of arguments in args and kwargs. pass_args within
            # kwargs are neglected.
            # cnt[0] is at least 2 due to deep_map calls on args and kwargs. If a
            # ConfigContext object is passed as argument, cfg_cnt=1 and cnt=3.
            if not any_data_arguments_collected:
                # dematerialize all tables and optionally views of both stage versions

                def _dematerialize_all(cfg, stage):
                    table_dict = {}
                    if include_views:
                        # also include table aliases
                        tbls1 = cfg.store.table_store.get_objects_in_stage(stage)
                    else:
                        # won't detect aliases
                        tbls1 = cfg.store.table_store.get_table_objects_in_stage(
                            stage, include_views=include_views
                        )
                    for name in tbls1:
                        tbl = Table(name=name)
                        tbl.stage = stage
                        if include_views:
                            (
                                tbl.name,
                                tbl.external_schema,
                            ) = cfg.store.table_store.resolve_alias(
                                tbl, stage.current_name
                            )
                            if not cfg.store.table_store.has_table_or_view(
                                tbl.name, tbl.external_schema
                            ):
                                continue  # skip because it is neither view nor alias
                        table_dict[tbl.name] = cfg.store.dematerialize_item(
                            tbl, input_type
                        )
                    return table_dict

                transaction_table_dict = _dematerialize_all(cfg1, stage)
                other_table_dict = _dematerialize_all(cfg2, stage2)
            else:

                def _dematerialize_versions(
                    ref: Table | Blob,
                    obj,
                    transaction_dict: dict[str, Any],
                    other_dict: dict[str, Any],
                    cfg2: ConfigContext,
                    stage: Stage,
                ):
                    key = (
                        ref.name
                        if ref.stage == stage
                        else f"{ref.stage.name}.{ref.name}"
                    )
                    transaction_dict[key] = obj
                    # load committed version of stage (either same pipeline instace or
                    # other)
                    tbl2 = ref.copy_without_obj()
                    tbl2.stage = Stage(
                        ref.stage.name,
                        ref.stage.materialization_details,
                        ref.stage.group_node_tag,
                        force_committed=True,
                    )
                    tbl2.stage.id = ref.stage.id
                    try:
                        store2 = cfg2.store.table_store
                        # Only try to dematerialize if an object with the correct name
                        # exists. Otherwise, we might run into retry loops.
                        if not isinstance(tbl2, Table) or tbl2.name.lower() in {
                            s.lower() for s in store2.get_objects_in_stage(tbl2.stage)
                        }:
                            other_dict[key] = cfg2.store.dematerialize_item(
                                tbl2, input_type
                            )
                        else:
                            other_dict[key] = (
                                "Table not found in other stage version: "
                                f"{PipedagJSONEncoder().encode(tbl2)}"
                            )
                    except Exception as e:
                        _task.logger.error(
                            "Failed to dematerialize table from other stage version",
                            table=ref,
                            stage=tbl2.stage,
                            instance=cfg2.instance_name,
                            instance_id=cfg2.instance_id,
                            cause=str(e),
                        )
                        other_dict[key] = e

                for tbl, obj in table_dict.items():
                    _dematerialize_versions(
                        tbl, obj, transaction_table_dict, other_table_dict, cfg2, stage
                    )
                for blob, obj in blob_dict.items():
                    _dematerialize_versions(
                        blob, obj, transaction_blob_dict, other_blob_dict, cfg2, stage
                    )
                for raw_sql in raw_sqls:
                    for tbl, obj in raw_sql.loaded_tables:
                        _dematerialize_versions(
                            tbl,
                            obj,
                            transaction_table_dict,
                            other_table_dict,
                            cfg2,
                            stage,
                        )

            optional_cfg = [] if other_cfg is None else [other_cfg]

            # restructure arbitrary arguments referencing Tables/Blobs/RawSQL into 2 or
            # 4 dictionaries providing references only to tables and optionally blobs
            if len(transaction_blob_dict) > 0:
                ret = _fn(
                    transaction_table_dict,
                    other_table_dict,
                    transaction_blob_dict,
                    other_blob_dict,
                    *optional_cfg,
                    **pass_kwargs,
                )
            else:
                ret = _fn(
                    transaction_table_dict,
                    other_table_dict,
                    *optional_cfg,
                    **pass_kwargs,
                )
        except Exception as e:
            if lock_source_stages:
                for stage in sorted(stages, reverse=True):
                    lock_manager.release(stage.name)
            raise e
        return ret, cfg2, stages, lock_manager

    def _collect_arguments(args, kwargs, is_dematerialized):
        remaining_kwargs = {k: v for k, v in kwargs.items() if k not in set(pass_args)}
        table_dict, blob_dict, raw_sqls = {}, {}, []
        stages = set()
        other_cfg = [None]  # list to allow modification in visitor
        cnt = [0]
        cfg_cnt = [0]
        str_dict_keys = [0]

        def visitor(x):
            if isinstance(x, dict):
                str_dict_keys[0] += sum(isinstance(k, str) for k in x.keys())
            if not isinstance(x, (list, tuple, dict)):
                cnt[0] += 1  # count non-collection arguments
            if isinstance(x, ConfigContext):
                if cfg_cnt[0] > 0:
                    raise ValueError(
                        "Only one ConfigContext object is allowed when calling a task "
                        "decorated with @input_stage_versions."
                    )
                other_cfg[0] = x
                cfg_cnt[0] += 1
            if is_dematerialized:
                if isinstance(x, tuple) and len(x) == 2:
                    # with the add_input_source parameter of @materialize, we receive
                    # tuples of the reference object (Table/Blob/RawSQL) together with
                    # the dematerialized object
                    obj, ref = x
                else:
                    return x
            else:
                obj, ref = x, x
            if isinstance(ref, (Table, Blob, RawSql)):
                stages.add(ref.stage)
            if isinstance(ref, Table):
                table_dict[ref] = obj
            elif isinstance(ref, Blob):
                blob_dict[ref] = obj
            elif isinstance(ref, RawSql):
                # ref and obj are identical for RawSql
                raw_sqls.append(ref)
            return x

        deep_map(args, visitor)
        deep_map(remaining_kwargs, visitor)
        # dictionary keys don't count since we like to be f(config=cfg) to be considered
        # as not any_data_arguements_collected
        any_data_arguements_collected = cnt[0] > cfg_cnt[0] + str_dict_keys[0]
        return (
            stages,
            table_dict,
            blob_dict,
            raw_sqls,
            other_cfg[0],
            any_data_arguements_collected,
        )

    return task


def _get_output_from_store(
    task: MaterializingTask | MaterializingTaskGetItem,
    as_type: type,
    ignore_position_hashes: bool = False,
) -> Any:
    """Helper to retrieve task output from store"""
    from pydiverse.pipedag.context.run_context import DematerializeRunContext
    from pydiverse.pipedag.materialize.store import dematerialize_output_from_store

    root_task = task if isinstance(task, Task) else task.task

    store = ConfigContext.get().store
    with DematerializeRunContext(root_task.flow):
        cached_output, _ = store.retrieve_most_recent_task_output_from_cache(
            root_task, ignore_position_hashes=ignore_position_hashes
        )
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
