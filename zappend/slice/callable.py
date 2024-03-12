# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import importlib
import inspect
from typing import Any, Type

from ..context import Context
from .source import SliceCallable
from .source import SliceItem


def invoke_slice_callable(
    slice_callable: SliceCallable,
    slice_item: Any,
    ctx: Context,
) -> SliceItem:
    """Utility function that invokes the given `slice_callable` with
    `slice_item` interpreted as its arguments.

    If the callable defines an argument named `ctx`, the given processing
    context `ctx` is passed to it.

    Args:
        slice_callable: A callable of type `SliceCallable`.
        slice_item: A slice item interpreted as argument(s).
        ctx: Current processing context.
    Returns:
        A slice item of type `SliceItem`.
    """
    slice_args, slice_kwargs = to_slice_args(slice_item)

    signature = inspect.signature(slice_callable)
    ctx_parameter = signature.parameters.get("ctx")
    if ctx_parameter is not None:
        if ctx_parameter.default is ctx_parameter.empty:
            # parameter "ctx" given as 1st positional argument
            slice_args = (ctx,) + tuple(slice_args)
        else:
            # parameter "ctx" given as keyword argument
            slice_kwargs = dict(**slice_kwargs, ctx=ctx)

    return slice_callable(*slice_args, **slice_kwargs)


def to_slice_args(arg: Any) -> tuple[tuple[...], dict[str, Any]]:
    if isinstance(arg, tuple):
        try:
            args, kwargs = arg
        except ValueError:
            raise TypeError("tuple of form (args, kwargs) expected")
        if isinstance(args, tuple):
            pass
        elif isinstance(args, list):
            args = tuple(args)
        else:
            raise TypeError(
                "args in tuple of form (args, kwargs) must be a tuple or list"
            )
        if not isinstance(kwargs, dict):
            raise TypeError("kwargs in tuple of form (args, kwargs) must be a dict")
    elif isinstance(arg, list):
        args = tuple(arg)
        kwargs = {}
    elif isinstance(arg, dict):
        args = ()
        kwargs = arg
    else:
        args = (arg,)
        kwargs = {}
    return args, kwargs


def to_slice_callable(slice_source_type: str | Type) -> SliceCallable | None:
    """Convert a string or type into a slice callable.

    Args:
        slice_source_type: a fully qualified class or function name or a callable type.
    Returns:
        A callable or `None`.
    """
    if not slice_source_type:
        return None
    if isinstance(slice_source_type, str):
        slice_source_type = import_attribute(slice_source_type)
    if not callable(slice_source_type):
        raise TypeError(
            "slice_source must a callable or the fully qualified name of a callable"
        )
    return slice_source_type


def import_attribute(name: str) -> Any:
    parts = [part for part in name.split(".") if part]

    module = None

    n = len(parts)
    i0 = -1
    for i in range(n):
        module_name = ".".join(parts[: n - i])
        try:
            module = importlib.import_module(module_name)
            i0 = n - i
            break
        except ModuleNotFoundError:
            pass

    if module is None or i0 == n:
        raise ImportError(f"no attribute found named {name!r}")

    obj = module
    for attr_name in parts[i0:]:
        try:
            obj = getattr(obj, attr_name)
        except AttributeError:
            raise ImportError(
                f"attribute {'.'.join(parts[i0:])!r} not found"
                f" in module {'.'.join(parts[:i0])!r}"
            )

    return obj
