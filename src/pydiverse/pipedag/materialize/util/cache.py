import base64
import hashlib


def compute_cache_key(*args: str) -> str:
    """Compute a cache key given some inputs

    :param args: Some strings from which to compute the cache key
    :return: A sha256 base32 digest, trimmed to 20 char length
    """

    v_str = "PYDIVERSE|" + "|".join(map(str, args))
    v_bytes = v_str.encode("utf8")
    v_hash = hashlib.sha256(v_bytes)

    # Only take first 20 characters of base32 digest (100 bits). This
    # provides 50 bits of collision resistance, which is more than enough.
    # To illustrate: If you were to generate 1k cache keys per second,
    # you still would have to wait over 800k years until you encounter
    # a collision.

    # NOTE: Can't use base64 because it contains lower and upper case
    #       letters; identifiers in pipedag are all lowercase
    hash_str = base64.b32encode(v_hash.digest()).decode("ascii").lower()
    return hash_str[:20]
