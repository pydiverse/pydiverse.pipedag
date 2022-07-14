class FlowError(Exception):
    """
    Exception raised when there is an issue with the flow definition.
    """

class SchemaError(Exception):
    """
    Exception raised when something is wrong with the schema.
    """

class AnnotationError(SystemError):
    """
    Exception raised if a task is annotated incorrectly.
    """