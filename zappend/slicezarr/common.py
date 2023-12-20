import xarray as xr

from ..context import Context
from ..fileobj import FileObj
from .abc import SliceZarr
from .inmemory import InMemorySliceZarr
from .persistent import PersistentSliceZarr


def open_slice_zarr(ctx: Context, slice_obj: str | xr.Dataset) -> SliceZarr:
    if isinstance(slice_obj, xr.Dataset):
        return InMemorySliceZarr(ctx, slice_obj)
    if isinstance(slice_obj, str):
        slice_file = FileObj(slice_obj,
                             storage_options=ctx.slice_storage_options)
        return PersistentSliceZarr(ctx, slice_file)
    raise TypeError("slice_obj must be a str or xarray.Dataset")
