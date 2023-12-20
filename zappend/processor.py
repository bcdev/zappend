# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr
from .context import Context
from .transmit import transmit
from .fileobj import FileObj
from .slicezarr import open_slice_zarr
from .transaction import Transaction
from .transmit import RollbackCallback


class Processor:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    def process_slices(self,
                       slice_iter: Iterable[str | xr.Dataset]):
        for slice_obj in slice_iter:
            self.process_slice(slice_obj)

    def process_slice(self, slice_obj: str | xr.Dataset):
        with open_slice_zarr(self.ctx, slice_obj) as slice_dir:
            target_dir = self.ctx.target_dir
            with Transaction(target_dir, self.ctx.temp_dir) as rollback_cb:
                if not target_dir.exists():
                    self.write_slice(slice_dir, rollback_cb)
                else:
                    self.append_slice(slice_dir, rollback_cb)

    def write_slice(self, slice_dir: FileObj, rollback_cb: RollbackCallback):
        transmit(slice_dir.fs,
                 slice_dir.path,
                 self.ctx.target_dir.fs,
                 self.ctx.target_dir.path,
                 rollback_cb=rollback_cb)

    def append_slice(self, slice_dir: FileObj, rollback_cb: RollbackCallback):
        transmit(slice_dir.fs,
                 slice_dir.path,
                 self.ctx.target_dir.fs,
                 self.ctx.target_dir.path,
                 file_filter=self.filter_slice_component,
                 rollback_cb=rollback_cb)

    def filter_slice_component(self):
        # TODO: implement file_filter
        raise NotImplementedError()
