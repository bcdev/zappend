# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr
import zarr.storage
import zarr.convenience

from .context import Context
from .fileobj import FileObj
from .slicezarr import open_slice_zarr
from .transaction import Transaction
from .transmit import RollbackCallback
from .zgroup import get_zarr_updates
from .zgroup import open_zarr_group
from .zgroup import get_zarr_store


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
            update_mode = target_dir.exists()
            with Transaction(target_dir, self.ctx.temp_dir) as rollback_cb:
                if update_mode:
                    self.update_target(slice_dir, rollback_cb)
                else:
                    self.create_target(slice_dir, rollback_cb)

    def create_target(self,
                      slice_dir: FileObj,
                      rollback_cb: RollbackCallback):
        target_dir = self.ctx.target_dir
        try:
            slice_dir.copy(target_dir)
        finally:
            if target_dir.exists():
                rollback_cb("delete_dir", target_dir.path, None)

    def update_target(self,
                      slice_dir: FileObj,
                      rollback_cb: RollbackCallback):

        target_group = open_zarr_group(self.ctx.target_dir)
        slice_group = open_zarr_group(slice_dir)

        update_records = get_zarr_updates(target_group,
                                          slice_group,
                                          self.ctx.append_dim)

        # TODO: notify rollback_cb

        for var_name, (append_axis, _) in update_records.items():
            target_array: zarr.Array = target_group[var_name]
            slice_array: zarr.Array = slice_group[var_name]
            target_array.append(slice_array, axis=append_axis)

        target_store = get_zarr_store(self.ctx.target_dir)
        zarr.convenience.consolidate_metadata(target_store)

    def generate_update_list(self,
                             target_group: zarr.Group,
                             slice_group: zarr.Group,
                             append_dim: str) -> list:
        append_dim = self.ctx.append_dim
        append_list: list[tuple[
            str,  # variable name
            int,  # append_axis
            list[tuple[int, ...]]  # chunks
        ]] = []
        for var_name, value in target_group.arrays():
            target_array: zarr.Array = value

            target_dims = target_array.attrs.get("_ARRAY_DIMENSIONS")
            if target_dims is None:
                # Should actually not come here
                raise ValueError("Missing array dimensions"
                                 " for variable {var_name!r}")

            try:
                append_axis = target_dims.index(append_dim)
            except ValueError:
                # append dimension does not exist in variable,
                # so we cannot append data
                continue

            if var_name not in slice_group or not hasattr(value, "shape"):
                raise ValueError(f"Variable {var_name!r} not found in slice")
            slice_array: zarr.Array = slice_group[var_name]

            slice_dims = slice_array.attrs.get("_ARRAY_DIMENSIONS")
            if target_dims != slice_dims:
                raise ValueError(f"Variable dimensions"
                                 f" for {var_name!r} do not match:"
                                 f" expected {target_dims},"
                                 f" but got {slice_dims}")

            append_list.append((var_name, append_axis, []))
        return append_list
