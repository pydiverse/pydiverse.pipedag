class FlowError(Exception):
    """
    Exception raised when there is an issue with the flow definition.
    """

class SchemaError(Exception):
    """
    Exception raised when something is wrong with the schema.
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