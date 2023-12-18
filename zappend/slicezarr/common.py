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
        slice_fo = FileObj(slice_obj, ctx.slice_fs_options)
        return PersistentSliceZarr(ctx, slice_fo)
    raise TypeError("slice_obj must be a str or xarray.Dataset")
