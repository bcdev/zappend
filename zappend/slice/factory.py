# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Callable, Iterable, Type, Iterator
import inspect

import xarray as xr

from .abc import SliceSource
from .memory import MemorySliceSource
from .persistent import PersistentSliceSource
from .temporary import TemporarySliceSource
from ..context import Context
from ..fsutil.fileobj import FileObj


SliceObj = str | FileObj | xr.Dataset | SliceSource
"""The possible types that can represent a slice dataset."""

SliceFactory = Callable[[Context], SliceObj]
"""The type for a factory function that returns a slice object
for a given processing context.
"""


def open_slice_source(
    ctx: Context,
    slice_obj: SliceObj | SliceFactory | Any,
    slice_index: int = 0,
) -> SliceSource:
    """Open the slice source for given slice object `slice_obj`.

    The intended and only use of the returned slice source is as context
    manager. When used as context manager the slice source yields a slice
    dataset.

    If `slice_source` is specified in the configuration, it defines either a
    class derived from `zappend.slice.SliceSource` or a function that returns
    instances of `zappend.slice.SliceSource`. The retrieved slice source will
    be returned by this function. The class or function will receive positional
    and keyword arguments derived from `slice_obj` as follows:

    * `tuple`: a pair of the form `(args, kwargs)`, where `args` is a list
      or tuple of positional arguments and `kwargs` is a dictionary of keyword
      arguments;
    * `list`: positional arguments only;
    * `dict`: keyword arguments only;
    * Any other type is interpreted as single positional argument.

    If `slice_source` is not specified in the configuration, the slice object
    `slice_obj` may have one of the following types:

    * `str`: A local file path or URI pointing to a dataset file such as a
      Zarr or NetCDF. If it is a URI, the `ctx.slice_storage_options` apply.
    * `zappend.fsutil.FileObj`: A file object instance pointing to a dataset
      file.
    * `zappend.slice.SliceSource`: A slice source. Returned as-is.
    * `xarray.Dataset`: An in-memory xarray dataset instance.
    * A slice factory, which is a callable that takes the processing context
      `ctx` as single argument and returns another slice object which is then
      recursively evaluated until the final value is a slice source.

    Args:
        ctx: The processing context
        slice_obj: A slice object
        slice_index: Optional slice index, used for dataset identification

    Returns:
        A new slice source instance
    """
    if ctx.slice_source is not None:
        slice_args, slice_kwargs = normalize_slice_arg(slice_obj)
        slice_factory = to_slice_factory(ctx.slice_source, *slice_args, **slice_kwargs)
        slice_source = slice_factory(ctx)
        if not isinstance(slice_source, SliceSource):
            raise TypeError(
                f"expected an instance of SliceSource"
                f" returned from {ctx.slice_source.__name__!r},"
                f" but got {type(slice_source)}"
            )
        return slice_source

    return _get_slice_dataset_recursive(ctx, slice_obj, slice_index)


def _get_slice_dataset_recursive(
    ctx: Context,
    slice_obj: SliceObj | SliceFactory,
    slice_index: int,
) -> SliceSource:
    if isinstance(slice_obj, SliceSource):
        return slice_obj
    if isinstance(slice_obj, (str, FileObj)):
        if isinstance(slice_obj, str):
            slice_file = FileObj(slice_obj, storage_options=ctx.slice_storage_options)
        else:
            slice_file = slice_obj
        return PersistentSliceSource(ctx, slice_file)
    if isinstance(slice_obj, xr.Dataset):
        if ctx.persist_mem_slices:
            return TemporarySliceSource(ctx, slice_obj, slice_index)
        else:
            return MemorySliceSource(ctx, slice_obj, slice_index)
    if callable(slice_obj):
        slice_factory: SliceFactory = slice_obj
        slice_obj = slice_factory(ctx)
        return _get_slice_dataset_recursive(ctx, slice_obj, slice_index)
    raise TypeError(
        "slice_obj must be a"
        " str,"
        " zappend.fsutil.FileObj,"
        " zappend.slice.SliceSource,"
        " xarray.Dataset,"
        " or a callable"
    )


def to_slice_factories(
    slice_callable: Callable[[...], SliceObj] | Type[SliceSource],
    slice_inputs: Iterable[Any],
) -> Iterator[SliceFactory]:
    """Utility function that generates slice factories for the given callable
    `slice_callable` and iterable of slice inputs `slice_inputs`.

    If the callable defines an argument named `ctx`, the current processing
    context of type [Context][zappend.api.Context] will be passed to it. If it
    is defined as
    a positional argument, it must be the first one in `slice_callable`.

    The slice factories are returned as an iterator that generates a new slice
    factory (closure) using the
    [to_slice_factory()][zappend.api.to_slice_factory] for each item in
    `slice_inputs`. An item may be one of following:

    * `tuple`: a pair of the form `(args, kwargs)`, where `args` is a list
      or tuple of positional arguments and `kwargs` is a dictionary of keyword
      arguments;
    * `list`: positional arguments only;
    * `dict`: keyword arguments only;
    * Any other type is interpreted as single positional argument.

    All items in `slice_inputs` should have the same type that matches the
    signature of `slice_callable`.

    Args:
        slice_callable: A callable that returns a slice object. Can also be
            the class of a custom [SliceSource][zappend.api.SliceSource].
        slice_inputs: An iterable that yields the inputs passed to the
            given `slice_callable`.
    Returns:
        An iterator that returns a slice factory for each item in
        `slice_args`.
    """

    for slice_arg in slice_inputs:
        slice_inputs, slice_kwargs = normalize_slice_arg(slice_arg)
        yield to_slice_factory(slice_callable, *slice_inputs, **slice_kwargs)


def to_slice_factory(
    slice_callable: Callable[[...], SliceObj] | Type[SliceSource],
    *slice_args: Any,
    **slice_kwargs: Any,
) -> SliceFactory:
    """Utility function that generates a slice factory (closure) for the given
    callable and arguments.

    If the callable defines an argument named `ctx`, the current processing
    context of type [Context][zappend.api.Context] will be passed to it.
    If `ctx` is defined as a positional argument, it must be the first one.

    Args:
        slice_callable: A callable that returns a slice object.
            Typically, the class of a custom
            [SliceSource][zappend.api.SliceSource] type will be passed.
        slice_args: The positional arguments that the slice factory will pass
            to `slice_callable`.
        slice_kwargs: The keyword arguments that the slice factory will pass to
            `slice_callable`.
    Returns:
        A slice factory that receives the current processing context as
        single argument.
    """
    signature = inspect.signature(slice_callable)
    ctx_parameter = signature.parameters.get("ctx")

    def slice_factory(ctx: Context) -> SliceObj:
        _slice_args = slice_args
        _slice_kwargs = slice_kwargs
        if ctx_parameter is not None:
            if ctx_parameter.default is ctx_parameter.empty:
                # parameter "ctx" given as positional argument
                _slice_args = (ctx,) + tuple(_slice_args)
            else:
                # parameter "ctx" given as keyword argument
                _slice_kwargs = dict(**_slice_kwargs, ctx=ctx)
        return slice_callable(*_slice_args, **_slice_kwargs)

    return slice_factory


def normalize_slice_arg(arg: Any) -> tuple[tuple[...], dict[str, Any]]:
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


def to_slice_source_type(slice_source_type: str | Type) -> Callable | None:
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
    import importlib

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
