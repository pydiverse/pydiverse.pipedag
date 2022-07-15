"""Generic deep mutation operations.

Heavily inspired by the builtin copy module of python:
https://github.com/python/cpython/blob/main/Lib/copy.py
"""
from typing import Callable


_nil = []

def deepmutate(x, mutator: Callable, memo=None):
    if memo is None:
        memo = {}

    d = id(x)
    y = memo.get(d, _nil)
    if y is not _nil:
        return y

    cls = type(x)

    if cls == list:
        y = _deepmutate_list(x, mutator, memo)
    elif cls == tuple:
        y = _deepmutate_tuple(x, mutator, memo)
    elif cls == dict:
        y = _deepmutate_dict(x, mutator, memo)
    else:
        y = mutator(x)

    # If is its own copy, don't memoize.
    if y is not x:
        memo[d] = y
        _keep_alive(x, memo)  # Make sure x lives at least as long as d

    return y

def _deepmutate_list(x, mutator, memo):
    y = []
    append = y.append
    for a in x:
        append(deepmutate(a, mutator, memo))
    return mutator(y)

def _deepmutate_tuple(x, mutator, memo):
    y = [deepmutate(a, mutator, memo) for a in x]
    # We're not going to put the tuple in the memo, but it's still important we
    # check for it, in case the tuple contains recursive mutable structures.
    try:
        return memo[id(x)]
    except KeyError:
        pass
    for k, j in zip(x, y):
        if k is not j:
            y = tuple(y)
            break
    else:
        y = x
    return mutator(y)

def _deepmutate_dict(x, mutator, memo):
    y = {}
    memo[id(x)] = y
    for key, value in x.items():
        y[deepmutate(key, mutator, memo)] = deepmutate(value, mutator, memo)
    return mutator(y)

def _keep_alive(x, memo):
    """Keeps a reference to the object x in the memo.
    Because we remember objects by their id, we have
    to assure that possibly temporary objects are kept
    alive by referencing them.
    We store a reference at the id of the memo, which should
    normally not be used unless someone tries to deepcopy
    the memo itself...
    """
    try:
        memo[id(memo)].append(x)
    except KeyError:
        # aha, this is the first one :-)
        memo[id(memo)]=[x]
