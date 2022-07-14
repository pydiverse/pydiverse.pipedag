from typing import Callable, TypeVar


def decorator_hint(decorator: Callable) -> Callable:
    # Used to fix incorrect type hints in pycharm
    return decorator


T = TypeVar('T')