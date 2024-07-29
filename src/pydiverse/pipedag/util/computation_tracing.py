from __future__ import annotations

import dis
import inspect
from enum import Enum
from typing import Any

from pydiverse.pipedag.util import deep_map


class Operation(Enum):
    OBJECT = 0
    GETATTR = 10
    SETATTR = 11
    DELATTR = 12
    GETITEM = 20
    SETITEM = 21
    DELITEM = 22
    CALL = 50
    GET = 60
    BOOL = 70

    def __repr__(self):
        return self.name


def _get_tracer(proxy: ComputationTracerProxy) -> ComputationTracer:
    return object.__getattribute__(proxy, "_computation_tracer_")


class ComputationTracer:
    proxy_type: type[ComputationTracerProxy]

    def __init__(self):
        self.trace = []
        self.patcher = MonkeyPatcher()
        self.did_exit = False
        self.proxy_type = ComputationTracerProxy

    def create_proxy(self, identifier=None):
        return self._get_proxy((Operation.OBJECT, identifier))

    def __enter__(self):
        self._monkey_patch()
        # clear trace already filled during patching (modules may issue calls during
        # initialization)
        self.trace = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patcher.undo()
        self.did_exit = True

    def _get_proxy(self, computation: tuple):
        idx = len(self.trace)
        self._add_computation(computation)
        return self.proxy_type(self, idx)

    def _add_computation(self, computation: tuple):
        if not self.did_exit:
            computation = deep_map(computation, self._computation_mapper)
            self.trace.append(computation)
        else:
            raise RuntimeError(
                "Can't modify ComputationTrace after exiting the context."
            )

    @staticmethod
    def _computation_mapper(x):
        if isinstance(x, ComputationTracerProxy):
            return ComputationTraceRef(x)

        if inspect.isfunction(x):
            bytecode = dis.Bytecode(x)
            return (
                Operation.OBJECT,
                "BYTECODE",
                tuple((instr.opcode, instr.argval) for instr in bytecode),
            )

        return x

    def _monkey_patch(self):
        ...

    def trace_hash(self) -> str:
        try:
            from dask.base import tokenize
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "module dask is required to use @materialize(version=AUTO_VERSION)"
                " or computation_tracing."
            ) from None

        return tokenize(self.trace)


class ComputationTracerProxy:
    def __init__(self, tracer: ComputationTracer, identifier: int | str):
        object.__setattr__(self, "_computation_tracer_", tracer)
        object.__setattr__(self, "_computation_tracer_id_", identifier)

    def __getattribute__(self, item):
        if item in ("__class__", "__module__"):
            return object.__getattribute__(self, item)

        tracer = _get_tracer(self)
        return tracer._get_proxy((Operation.GETATTR, self, item))

    def __setattr__(self, key, value):
        tracer = _get_tracer(self)
        tracer._add_computation((Operation.SETATTR, self, key, value))

    def __delattr__(self, key):
        tracer = _get_tracer(self)
        tracer._add_computation((Operation.DELATTR, self, key))

    def __getitem__(self, item):
        tracer = _get_tracer(self)
        return tracer._get_proxy((Operation.GETITEM, self, item))

    def __setitem__(self, key, value):
        tracer = _get_tracer(self)
        tracer._add_computation((Operation.SETITEM, self, key, value))

    def __delitem__(self, key):
        tracer = _get_tracer(self)
        tracer._add_computation((Operation.DELITEM, self, key))

    def __call__(self, *args, **kwargs):
        tracer = _get_tracer(self)
        return tracer._get_proxy((Operation.CALL, self, args, kwargs))

    def __get__(self, instance, owner):
        tracer = _get_tracer(self)
        return tracer._get_proxy((Operation.GET, instance, self))

    def __bool__(self):
        tracer = _get_tracer(self)
        return tracer._get_proxy((Operation.BOOL, self))

    def __iter__(self):
        raise RuntimeError("__iter__ is not supported by ComputationTracerProxy")

    def __contains__(self, item):
        raise RuntimeError("__contains__ is not supported by ComputationTracerProxy")

    def __len__(self):
        raise RuntimeError("__len__ is not supported by ComputationTracerProxy")


__supported_dunder = {
    "__add__",
    "__radd__",
    "__sub__",
    "__rsub__",
    "__mul__",
    "__rmul__",
    "__truediv__",
    "__rtruediv__",
    "__floordiv__",
    "__rfloordiv__",
    "__pow__",
    "__rpow__",
    "__mod__",
    "__rmod__",
    "__round__",
    "__pos__",
    "__neg__",
    "__abs__",
    "__and__",
    "__rand__",
    "__or__",
    "__ror__",
    "__xor__",
    "__rxor__",
    "__invert__",
    "__lt__",
    "__le__",
    "__eq__",
    "__ne__",
    "__gt__",
    "__ge__",
    "__copy__",
    "__deepcopy__",
}


def __create_dunder(name):
    def dunder(self, *args):
        return getattr(self, name)(self, *args)

    return dunder


for dunder_ in __supported_dunder:
    setattr(ComputationTracerProxy, dunder_, __create_dunder(dunder_))


class ComputationTraceRef:
    __slots__ = ("id",)

    def __init__(self, proxy: ComputationTracerProxy):
        self.id = object.__getattribute__(proxy, "_computation_tracer_id_")

    def __str__(self):
        return f"ComputationTraceRef<{self.id}>"

    def __repr__(self):
        return f"ComputationTraceRef<{self.id}>"

    def __dask_tokenize__(self):
        return "ComputationTraceRef", self.id


class MonkeyPatcher:
    """Monkey Patching class inspired by pytest's MonkeyPatch class"""

    def __init__(self):
        self._setattr: list[tuple[object, str, Any]] = []

    def patch_attr(self, obj: object, name: str, value: Any):
        old_value = getattr(obj, name)

        # avoid class descriptors like staticmethod / classmethod
        if inspect.isclass(obj):
            old_value = obj.__dict__[name]

        setattr(obj, name, value)
        self._setattr.append((obj, name, old_value))

    def undo(self):
        for obj, name, value in reversed(self._setattr):
            setattr(obj, name, value)
        self._setattr.clear()


def fully_qualified_name(obj):
    if type(obj).__name__ == "builtin_function_or_method":
        if obj.__module__ is not None:
            module = obj.__module__
        else:
            if inspect.isclass(obj.__self__):
                module = obj.__self__.__module__
            else:
                module = obj.__self__.__class__.__module__
        return f"{module}.{obj.__qualname__}"

    if type(obj).__name__ == "function":
        if hasattr(obj, "__wrapped__"):
            qualname = obj.__wrapped__.__qualname__
        else:
            qualname = obj.__qualname__
        return f"{obj.__module__}.{qualname}"

    if type(obj).__name__ in (
        "member_descriptor",
        "method_descriptor",
        "wrapper_descriptor",
    ):
        return f"{obj.__objclass__.__module__}.{obj.__qualname__}"

    if type(obj).__name__ == "method":
        if inspect.isclass(obj.__self__):
            cls = obj.__self__.__qualname__
        else:
            cls = obj.__self__.__class__.__qualname__
        return f"{obj.__self__.__module__}.{cls}.{obj.__name__}"

    if type(obj).__name__ == "method-wrapper":
        return f"{fully_qualified_name(obj.__self__)}.{obj.__name__}"

    if type(obj).__name__ == "module":
        return obj.__name__

    if type(obj).__name__ == "property":
        return f"{obj.fget.__module__}.{obj.fget.__qualname__}"

    if inspect.isclass(obj):
        return f"{obj.__module__}.{obj.__qualname__}"

    return f"{obj.__class__.__module__}.{obj.__class__.__qualname__}"


def patch(tracer: ComputationTracer, target: object, name: str):
    if isinstance(target, ComputationTracerProxy):
        return

    val = getattr(target, name)

    if isinstance(val, type):
        return patch_type(tracer, target, name)
    if inspect.isfunction(val):
        return patch_function(tracer, target, name)
    if isinstance(val, object):
        return patch_value(tracer, target, name)

    raise RuntimeError


def patch_type(tracer: ComputationTracer, target: object, name: str):
    val = getattr(target, name)
    full_name = fully_qualified_name(val)

    dunder_to_patch = (
        "__getattr__",
        "__setattr__",
        "__delattr__",
        "__getitem__",
        "__setitem__",
        "__delitem__",
        "__call__",
        "__repr__",
        "__str__",
    )

    for key, _value in object.__getattribute__(val, "__dict__").items():
        if key.startswith("__"):
            if key in dunder_to_patch:
                try:
                    tracer.patcher.patch_attr(
                        val, key, tracer.proxy_type(tracer, full_name + "." + key)
                    )
                except AttributeError:
                    pass
            continue
        else:
            tracer.patcher.patch_attr(
                val, key, tracer.proxy_type(tracer, full_name + "." + key)
            )

    tracer.patcher.patch_attr(target, name, tracer.proxy_type(tracer, full_name))


def patch_function(tracer: ComputationTracer, target: object, name: str):
    val = getattr(target, name)
    full_name = fully_qualified_name(val)
    tracer.patcher.patch_attr(target, name, tracer.proxy_type(tracer, full_name))


def patch_value(tracer: ComputationTracer, target: object, name: str):
    full_name = fully_qualified_name(target) + "." + name
    tracer.patcher.patch_attr(target, name, tracer.proxy_type(tracer, full_name))
