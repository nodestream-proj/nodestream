from functools import wraps
from warnings import warn


def leave_unchanged(v):
    return v


def deprecated_arugment(old, new, converter=leave_unchanged):
    """Decorator to convert an old argument to a new one.

    This decorator will convert an old argument to a new one. It will also
    issue a deprecation warning. By default, the old argument will be passed
    through unchanged. If a converter function is provided, it will be used to
    convert the old argument to the new one.

    Args:
        old: The old argument name.
        new: The new argument name.
        converter: A function to convert the old argument to the new one.
    """

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
