from __future__ import annotations


class FlowError(Exception):
    """
    Exception raised when there is an issue with the flow definition.
    """


class StageError(Exception):
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


class DisposedError(Exception):
    """
    Exception raise when an object has been disposed, but some attributes are
    being accessed nevertheless.
    """
