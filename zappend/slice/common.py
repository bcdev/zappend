# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
from typing import Callable

import xarray as xr

from .abc import SliceSource
from .memory import MemorySliceSource
from .persistent import PersistentSliceSource
from .temporary import TemporarySliceSource
from ..context import Context
from ..fsutil.fileobj import FileObj


SliceObj = str | FileObj | xr.Dataset | SliceSource
SliceFactory = Callable[[Context], SliceObj]


def get_slice_dataset(
    ctx: Context,
    slice_obj: SliceObj | SliceFactory,
    slice_index: int = 0,
) -> SliceSource:
    """
    Get the slice source for given slice object *slice_obj*.

    The intended and only use of the returned slice source is as context manager.
    When used as context manager the slice source yields a slice dataset.

    The slice object *slice_obj* may have one of the following types:

    * ``str``: A local file path or URI pointing to a dataset file such as a
      Zarr or NetCDF. If it is a URI, the `ctx.slice_storage_options` apply.
    * ``zappend.fsutil.FileObj``: A file object instance pointing to a dataset
      file.
    * ``zappend.slice.SliceSource``: A slice source. Returned as-is.
    * ``xarray.Dataset``: An in-memory xarray dataset instance.
    * A callable that receives the processing context *ctx* as input
      and returns a slice object. The primary usage is to return custom
      slice sources of type ``zappend.slice.SliceSource``.

    :param ctx: The processing context
    :param slice_obj: A slice object
    :param slice_index: Optional slice index, used for dataset identification
    :return: A new slice source instance
    """
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
        return get_slice_dataset(ctx, slice_factory(ctx), slice_index=slice_index)
    raise TypeError(
        "slice_obj must be a"
        " str,"
        " zappend.fsutil.FileObj,"
        " zappend.slice.SliceSource,"
        " xarray.Dataset,"
        " or a factory function"
    )
