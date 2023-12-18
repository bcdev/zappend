# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import uuid

import xarray as xr

from ..context import Context
from .abc import SliceZarr


class InMemorySliceZarr(SliceZarr):
    """A slice Zarr that is available in-memory only as a xarray dataset.

    :param ctx: Processing context
    :param slice_ds: The in-memory dataset
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset):
        super().__init__(ctx)
        self.slice_ds = slice_ds
        self.temp_path = None

    def prepare(self):
        self.temp_path = self.write_temp(self.slice_ds)
        return self.ctx.temp_fs, self.temp_path

    def dispose(self):
        if hasattr(self, "temp_path") and self.temp_path is not None:
            self.delete_temp()
            self.temp_path = None
            del self.temp_path
        super().dispose()

    def write_temp(self, dataset: xr.Dataset) -> str:
        temp_path = f"{self.ctx.temp_path}/{uuid.uuid4()}.zarr"
        encoding = {var_name: var_info["encoding"]
                    for var_name, var_info in self.ctx.variables.items()
                    if "encoding" in var_info}
        store = self.ctx.temp_fs.get_mapper(root=temp_path, create=True)
        dataset.to_zarr(store,
                        write_empty_chunks=False,
                        encoding=encoding,
                        zarr_version=self.ctx.zarr_version)
        return temp_path

    def delete_temp(self):
        if self.temp_path is not None \
                and self.ctx.temp_fs.isdir(self.temp_path):
            self.ctx.temp_fs.rm(self.temp_path, recursive=True)
