from __future__ import annotations

import base64
import hashlib


def stable_hash(*args: str) -> str:
    """Compute a hash over a set of strings

    :param args: Some strings from which to compute the cache key
    :return: A sha256 base32 digest, trimmed to 20 char length
    """

    combined_hash = hashlib.sha256(b"PIPEDAG")
    for arg in args:
        arg_bytes = str(arg).encode("utf8")
        arg_bytes_len = len(arg_bytes).to_bytes(length=8, byteorder="big")

        combined_hash.update(arg_bytes_len)
        combined_hash.update(arg_bytes)

    # Only take first 20 characters of base32 digest (100 bits). This
    # provides 50 bits of collision resistance, which is more than enough.
    # To illustrate: If you were to generate 1k hashes per second,
    # you still would have to wait over 800k years until you encounter
    # a collision.

    # NOTE: Can't use base64 because it contains lower and upper case
    #       letters; identifiers in pipedag are all lowercase
    hash_digest = combined_hash.digest()
    hash_str = base64.b32encode(hash_digest).decode("ascii").lower()
    return hash_str[:20]
