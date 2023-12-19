# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr
from .context import Context
from .copydir import copy_dir
from .fileobj import FileObj
from .slicezarr import open_slice_zarr


class Processor:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    def process_slices(self,
                       slice_iter: Iterable[str | xr.Dataset]):
        for slice_obj in slice_iter:
            self.process_slice(slice_obj)

    def process_slice(self, slice_obj: str | xr.Dataset):
        with open_slice_zarr(self.ctx, slice_obj) as slice_fo:
            target_fs = self.ctx.target_fo.filesystem
            # TODO: wrap the following code block so that it forms
            #  a transaction.
            #  on enter:
            #    - check for existing lock
            #    - lock the target
            #  while:
            #    - backup any changed target files
            #    - remember new files
            #  on error:
            #    - restore changed files
            #    - delete new files
            #  finally:
            #    - remove backup files
            #    - remove lock
            if not target_fs.exists(self.ctx.target_fo.path):
                self.write_slice(slice_fo)
            else:
                self.append_slice(slice_fo)

    def write_slice(self, slice_fo: FileObj):
        copy_dir(slice_fo.filesystem,
                 slice_fo.path,
                 self.ctx.target_fo.filesystem,
                 self.ctx.target_fo.path)

    def append_slice(self, slice_fo: FileObj):
        copy_dir(slice_fo.filesystem,
                 slice_fo.path,
                 self.ctx.target_fo.filesystem,
                 self.ctx.target_fo.path)
