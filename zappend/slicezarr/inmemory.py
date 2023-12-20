# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import uuid

import xarray as xr

from ..context import Context
from ..fileobj import FileObj
from ..log import logger
from ..outline import DatasetOutline
from ..outline import check_compliance
from .abc import SliceZarr


class InMemorySliceZarr(SliceZarr):
    """A slice Zarr that is available in-memory only as a xarray dataset.

    :param ctx: Processing context
    :param slice_ds: The in-memory dataset
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset):
        super().__init__(ctx)
        self._slice_ds = slice_ds
        self._temp_fo: FileObj | None = None

    def prepare(self) -> FileObj:
        logger.info("Writing temporary slice")

        self._write_temp_zarr()

        slice_outline = DatasetOutline.from_zarr(self._temp_fo)
        compliant = check_compliance(self._ctx.target_outline,
                                     slice_outline,
                                     self._temp_fo.uri,
                                     error=True)
        if not compliant:
            raise ValueError("Invalid slice dataset")

        return self._temp_fo

    def dispose(self):
        self._slice_ds = None
        if self._temp_fo is not None:
            self._delete_temp_zarr()
        self._temp_fo = None
        super().dispose()

    def _write_temp_zarr(self) -> FileObj:
        temp_fo = self._ctx.temp_fo.for_path(f"{uuid.uuid4()}.zarr")

        # Save reference early so dispose() can delete also
        # if the following code raises
        self.temp_fo = temp_fo

        encoding = {var_name: var_info["encoding"]
                    for var_name, var_info in self._ctx.variables.items()
                    if "encoding" in var_info}
        store = temp_fo.filesystem.get_mapper(root=temp_fo.path, create=True)

        self._slice_ds.to_zarr(store,
                               write_empty_chunks=False,
                               encoding=encoding,
                               zarr_version=self._ctx.zarr_version)
        return temp_fo

    def _delete_temp_zarr(self):
        if self._temp_fo is not None:
            fs = self._ctx.temp_fo.filesystem
            path = self._temp_fo.path
            if fs.isdir(path):
                fs.rm(path, recursive=True)
