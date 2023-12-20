# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import json
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
        transmit(slice_dir,
                 self.ctx.target_dir,
                 rollback_cb=rollback_cb)

    def append_slice(self, slice_dir: FileObj, rollback_cb: RollbackCallback):
        transmit(slice_dir,
                 self.ctx.target_dir,
                 file_filter=self.filter_slice_component,
                 rollback_cb=rollback_cb)

    def filter_slice_component(self,
                               slice_comp_path: str,
                               slice_comp_filename: str,
                               slice_data: bytes) -> tuple[str, str] | None:
        if slice_comp_filename == ".zattrs":
            # don't transfer slice attributes
            return None

        if slice_comp_filename == ".zarray":
            append_dim = self.ctx.append_dim
            # TODO: load corresponding target ".zarray" JSON
            # target_data = (self.ctx.target_dir / slice_comp_path).read("rt")
            # target_json = json.loads(target_data)
            # TODO: modify shape for append_dim in target_json.
            #   Note, this requires identifying the append_dim_index!
            # target_json["shape"] =
            # TODO: convert JSON back to bytes --> target_data
            # TODO: return slice_comp_filename, target_data
            # from io import BytesIO
            # array_metadata = json.load(BytesIO(slice_data))
            return None

        # TODO: implement file_filter
        raise NotImplementedError()
