# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import contextlib
from typing import Any

import xarray as xr

from ..context import Context
from .source import SliceSource
from .source import to_slice_source


class SliceSourceContextManager(contextlib.AbstractContextManager):
    """A context manager that wraps a slice source.

    Args:
        slice_source: The slice source.
    """

    def __init__(self, slice_source: SliceSource):
        self._slice_source = slice_source

    @property
    def slice_source(self) -> SliceSource:
        return self._slice_source

    def __enter__(self) -> xr.Dataset:
        return self._slice_source.get_dataset()

    def __exit__(self, *exception_args):
        self._slice_source.dispose()
        self._slice_source = None


def open_slice_dataset(
    ctx: Context,
    slice_item: Any,
    slice_index: int = 0,
) -> SliceSourceContextManager:
    """Open the slice source for given slice item `slice_item`.

    The intended and only use of the returned slice source is as context
    manager. When used as context manager the slice source yields a slice
    dataset.

    If `slice_source` is specified in the configuration, it defines either a
    class derived from `zappend.slice.SliceSource` or a function that returns
    instances of `zappend.slice.SliceSource`. The retrieved slice source will
    be returned by this function. The class or function will receive positional
    and keyword arguments derived from `slice_item` as follows:

    * `tuple`: a pair of the form `(args, kwargs)`, where `args` is a list
      or tuple of positional arguments and `kwargs` is a dictionary of keyword
      arguments;
    * `list`: positional arguments only;
    * `dict`: keyword arguments only;
    * Any other type is interpreted as single positional argument.

    If `slice_source` is not specified in the configuration, the slice item
    `slice_item` may have one of the following types:

    * `str`: A local file path or URI pointing to a dataset file such as a
      Zarr or NetCDF. If it is a URI, the `ctx.slice_storage_options` apply.
    * `xarray.Dataset`: An in-memory xarray dataset instance.
    * `zappend.api.FileObj`: A file object instance pointing to a dataset
      file in a predefined filesystem.
    * `zappend.api.SliceSource`: A slice source. Returned as-is.

    Args:
        ctx: The processing context
        slice_item: A slice item
        slice_index: Optional slice index, used for dataset identification

    Returns:
        A new slice source instance
    """
    slice_source = to_slice_source(ctx, slice_item, slice_index)
    return SliceSourceContextManager(slice_source)
