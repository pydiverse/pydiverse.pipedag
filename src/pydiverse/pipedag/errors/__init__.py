# Copyright (c) QuantCo and pydiverse contributors 2025-2025
# SPDX-License-Identifier: BSD-3-Clause

from pydiverse.common.errors import DisposedError


class FlowError(Exception):
    """
    Exception raised when there is an issue with the flow definition.
    """


class StageError(Exception):
    """
    Exception raised when something is wrong with the stage.
    """


class GroupNodeError(Exception):
    """
    Exception raised when something is wrong with the stage.
    """


class CacheError(Exception):
    """
    Exception raised if something couldn't be retrieved from the cache.
    """


class LockError(Exception):
    """
    Exception raised if something goes wrong while locking, for example if
    a lock expires before it has been released.
    """


class DuplicateNameError(ValueError):
    """
    Exception raised if an object that is supposed to have a unique name doesn't.
    """


class IPCError(Exception):
    """
    Exception raised when inter process communication fails.
    """


class RemoteProcessError(IPCError):
    """
    Exception raised if an exception occurred in the remote IPC process.
    """


class HookCheckException(Exception):
    """
    Exception raised if a hook check fails. It is caught to surface as a
    normal task failure.
    """


class StoreIncompatibleException(Exception):
    """
    Exception raised if a store is incompatible for retrieving a table type.
    """


__all__ = [
    "FlowError",
    "StageError",
    "GroupNodeError",
    "CacheError",
    "LockError",
    "DuplicateNameError",
    "IPCError",
    "RemoteProcessError",
    "DisposedError",
]
