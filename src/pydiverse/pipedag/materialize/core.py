# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

import uuid
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Any, overload

import sqlalchemy as sa

from pydiverse.common.util import deep_map
from pydiverse.common.util.hashing import stable_hash
from pydiverse.pipedag._typing import CallableT
from pydiverse.pipedag.container import Blob, RawSql, Table
from pydiverse.pipedag.context import ConfigContext, TaskContext
from pydiverse.pipedag.core.task import Task
from pydiverse.pipedag.materialize.materializing_task import (
    MaterializingTask,
    MaterializingTaskGetItem,
    UnboundMaterializingTask,
)
from pydiverse.pipedag.util.json import PipedagJSONEncoder

if TYPE_CHECKING:
    from pydiverse.pipedag import Stage


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
) -> Callable[[CallableT], CallableT | UnboundMaterializingTask]: ...


@overload
def materialize(fn: CallableT, /) -> CallableT | UnboundMaterializingTask: ...


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
) -> Callable[[CallableT], CallableT | UnboundMaterializingTask]: ...


@overload
def input_stage_versions(fn: CallableT, /) -> CallableT | UnboundMaterializingTask: ...


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
        A list of named arguments that would be passed from the call to the task
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
        lock_stages = False if other_stage_lock_manager.lock_manager is not None else lock_source_stages
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
                        tbls1 = cfg.store.table_store.get_table_objects_in_stage(stage, include_views=include_views)
                    for name in tbls1:
                        tbl = Table(name=name)
                        tbl.stage = stage
                        if include_views:
                            (
                                tbl.name,
                                tbl.external_schema,
                            ) = cfg.store.table_store.resolve_alias(tbl, stage.current_name)
                            if not cfg.store.table_store.has_table_or_view(tbl.name, tbl.external_schema):
                                continue  # skip because it is neither view nor alias
                        table_dict[tbl.name] = cfg.store.dematerialize_item(tbl, input_type)
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
                    key = ref.name if ref.stage == stage else f"{ref.stage.name}.{ref.name}"
                    transaction_dict[key] = obj
                    # load committed version of stage (either same pipeline instance or
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
                            other_dict[key] = cfg2.store.dematerialize_item(tbl2, input_type)
                        else:
                            other_dict[key] = (
                                f"Table not found in other stage version: {PipedagJSONEncoder().encode(tbl2)}"
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
                    _dematerialize_versions(tbl, obj, transaction_table_dict, other_table_dict, cfg2, stage)
                for blob, obj in blob_dict.items():
                    _dematerialize_versions(blob, obj, transaction_blob_dict, other_blob_dict, cfg2, stage)
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
        # as not any_data_arguments_collected
        any_data_arguments_collected = cnt[0] > cfg_cnt[0] + str_dict_keys[0]
        return (
            stages,
            table_dict,
            blob_dict,
            raw_sqls,
            other_cfg[0],
            any_data_arguments_collected,
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
    def __init__(self, stage: "Stage", did_commit: bool):
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
