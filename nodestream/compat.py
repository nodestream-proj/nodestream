from functools import wraps
from warnings import warn


def leave_unchanged(v):
    return v


def deprecated_arugment(old, new, converter=leave_unchanged):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if old in kwargs:
                kwargs[new] = converter(kwargs[old])
                del kwargs[old]
                warn(
                    f"Argument {old} is deprecated. Use {new} instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )

            return f(*args, **kwargs)

        return wrapper

    return decorator
