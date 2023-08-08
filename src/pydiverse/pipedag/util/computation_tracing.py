from __future__ import annotations

from enum import Enum

from pydiverse.pipedag.util import deep_map


class Operation(Enum):
    COMPUTATION_START = 0
    GETATTR = 10
    SETATTR = 11
    DELATTR = 12
    GETITEM = 20
    SETITEM = 21
    DELITEM = 22
    CALL = 50

    def __repr__(self):
        return self.name


def _get_tracer(proxy: ComputationTracerProxy) -> ComputationTracer:
    return object.__getattribute__(proxy, "_computation_tracer_")


class ComputationTracer:
    def __init__(self):
        self.trace = []

    def create_proxy(self, identifier=None):
        return self._get_proxy((Operation.COMPUTATION_START, identifier))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _get_proxy(self, computation: tuple):
        idx = len(self.trace)
        self._add_computation(computation)
        return ComputationTracerProxy(self, idx)

    def _add_computation(self, computation: tuple):
        computation = deep_map(computation, self._computation_mapper)
        self.trace.append(computation)

    @staticmethod
    def _computation_mapper(x):
        if isinstance(x, ComputationTracerProxy):
            return ComputationTraceRef(x)
        return x


class ComputationTracerProxy:
    def __init__(self, tracer: ComputationTracer, idx: int):
        object.__setattr__(self, "_computation_tracer_", tracer)
        object.__setattr__(self, "_computation_tracer_idx_", idx)

    def __getattribute__(self, item):
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

    def __bool__(self):
        raise RuntimeError("Can't convert a ComputationTracerProxy to a boolean")

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
    __slots__ = ("idx",)

    def __init__(self, proxy: ComputationTracerProxy):
        self.idx = object.__getattribute__(proxy, "_computation_tracer_idx_")

    def __str__(self):
        return f"ComputationTraceRef<{self.idx}>"

    def __repr__(self):
        return f"ComputationTraceRef<{self.idx}>"

    def __dask_tokenize__(self):
        return self.idx
