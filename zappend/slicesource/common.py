import xarray as xr

from ..context import Context
from ..fsutil.fileobj import FileObj
from .abc import SliceSource
from .identity import IdentitySliceSource
from .persistent import PersistentSliceSource


def open_slice_source(ctx: Context, slice_obj: str | xr.Dataset) -> SliceSource:
    if isinstance(slice_obj, xr.Dataset):
        return IdentitySliceSource(ctx, slice_obj)
    if isinstance(slice_obj, str):
        slice_file = FileObj(slice_obj,
                             storage_options=ctx.slice_storage_options)
        return PersistentSliceSource(ctx, slice_file)
    raise TypeError("slice_obj must be a str or xarray.Dataset")
