# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import uuid

import xarray as xr

from ..context import Context
from ..fileobj import FileObj
from .abc import SliceZarr


class InMemorySliceZarr(SliceZarr):
    """A slice Zarr that is available in-memory only as a xarray dataset.

    :param ctx: Processing context
    :param slice_ds: The in-memory dataset
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset):
        super().__init__(ctx)
        self.slice_ds = slice_ds
        self.temp_fo: FileObj | None = None

    def prepare(self) -> FileObj:
        self.temp_fo = self.write_temp(self.slice_ds)
        # TODO: open_zarr() from temp and verify
        #   by using check_compliance()
        return self.temp_fo

    def dispose(self):
        self.slice_ds = None
        if self.temp_fo is not None:
            self.delete_temp()
        self.temp_fo = None
        super().dispose()

    def write_temp(self, dataset: xr.Dataset) -> FileObj:
        temp_fo = self._ctx.temp_fo.for_suffix(f"{uuid.uuid4()}.zarr")
        encoding = {var_name: var_info["encoding"]
                    for var_name, var_info in self._ctx.variables.items()
                    if "encoding" in var_info}
        store = temp_fo.filesystem.get_mapper(root=temp_fo.path, create=True)
        dataset.to_zarr(store,
                        write_empty_chunks=False,
                        encoding=encoding,
                        zarr_version=self._ctx.zarr_version)
        return temp_fo

    def delete_temp(self):
        if self.temp_fo is not None:
            fs = self._ctx.temp_fo.filesystem
            path = self.temp_fo.path
            if fs.isdir(path):
                fs.rm(path, recursive=True)
