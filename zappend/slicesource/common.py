# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr

from ..context import Context
from ..fsutil.fileobj import FileObj
from .abc import SliceSource
from .identity import IdentitySliceSource
from .persistent import PersistentSliceSource


def open_slice_source(
    ctx: Context, slice_obj: str | xr.Dataset, slice_index: int = 0
) -> SliceSource:
    """
    Open a slice source from given *slice_obj*.

    :param ctx: Processing context
    :param slice_obj: The slice object
    :param slice_index: Optional slice index (used for logging only)
    :return: A new slice source instance
    """
    if isinstance(slice_obj, xr.Dataset):
        return IdentitySliceSource(ctx, slice_obj, slice_index)
    if isinstance(slice_obj, str):
        slice_file = FileObj(slice_obj, storage_options=ctx.slice_storage_options)
        return PersistentSliceSource(ctx, slice_file)
    raise TypeError("slice_obj must be a str or xarray.Dataset")
