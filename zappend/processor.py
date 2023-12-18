# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import fsspec
import xarray as xr
from .context import Context
from .copydir import copy_dir
from .slicezarr import open_slice_zarr


class Processor:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    def process_slices(self,
                       slice_iter: Iterable[str | xr.Dataset]):
        for slice_path in slice_iter:
            self.process_slice(slice_path)

    def process_slice(self, slice_obj: str | xr.Dataset):
        with open_slice_zarr(self.ctx, slice_obj) as (slice_fs, slice_path):
            target_fs = self.ctx.target_fs
            if not target_fs.exists(self.ctx.target_path):
                self.write_slice(slice_fs, slice_path)
            else:
                self.append_slice(slice_fs, slice_path)

    def write_slice(self, slice_fs: fsspec.AbstractFileSystem,
                    slice_path: str):
        copy_dir(self.ctx.target_fs, self.ctx.target_path,
                 slice_fs, slice_path)

    def append_slice(self, slice_fs: fsspec.AbstractFileSystem,
                     slice_path: str):
        pass
