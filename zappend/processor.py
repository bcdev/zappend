# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr
import zarr.storage
import zarr.convenience

from .context import Context
from .fileobj import FileObj
from .log import logger
from .slicezarr import open_slice_zarr
from .transaction import Transaction
from .transaction import RollbackCallback
from .zutil import get_zarr_arrays_for_dim
from .zutil import open_zarr_group
from .zutil import get_zarr_store
from .zutil import get_chunk_update_range
from .zutil import get_chunk_indices


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
                    update_target_from_slice(self.ctx.target_dir,
                                             self.ctx.append_dim,
                                             slice_dir,
                                             rollback_cb)
                else:
                    create_target_from_slice(self.ctx.target_dir,
                                             slice_dir,
                                             rollback_cb)


def create_target_from_slice(target_dir, slice_dir, rollback_cb):
    try:
        slice_dir.copy(target_dir)
    finally:
        if target_dir.exists():
            rollback_cb("delete_dir", target_dir.path, None)


def update_target_from_slice(target_dir: FileObj,
                             append_dim: str,
                             slice_dir: FileObj,
                             rollback_cb: RollbackCallback):
    target_group = open_zarr_group(target_dir)
    slice_group = open_zarr_group(slice_dir)
    target_arrays = get_zarr_arrays_for_dim(target_group, append_dim)
    for array_name, (target_array, append_axis) in target_arrays.items():
        try:
            slice_array: zarr.Array = slice_group[array_name]
        except KeyError:
            raise ValueError(f"Array {array_name!r} not found in slice")

        target_dims = target_array.attrs.get("_ARRAY_DIMENSIONS")
        slice_dims = slice_array.attrs.get("_ARRAY_DIMENSIONS")
        if target_dims != slice_dims:
            raise ValueError(f"Array dimensions"
                             f" for {array_name!r} do not match:"
                             f" expected {target_dims},"
                             f" but got {slice_dims}")

        array_dir = target_dir / array_name

        array_metadata_file = array_dir / ".zarray"
        array_metadata = array_metadata_file.read()
        rollback_cb("replace_file",
                    array_metadata_file.path, array_metadata)

        chunk_update, append_dim_range = \
            get_chunk_update_range(target_array.shape[append_axis],
                                   target_array.chunks[append_axis],
                                   slice_array.shape[append_axis])

        chunk_indexes = get_chunk_indices(target_array.shape,
                                          target_array.chunks,
                                          append_axis,
                                          append_dim_range)

        start, _ = append_dim_range

        for chunk_index in chunk_indexes:
            chunk_filename = ".".join(map(str, chunk_index))
            chunk_file = array_dir / chunk_filename
            if chunk_update and chunk_index[append_axis] == start:
                try:
                    chunk_data = chunk_file.read()
                except FileNotFoundError:
                    # missing chunk files are ok, fill_value!
                    chunk_data = None
                if chunk_data:
                    rollback_cb("replace_file",
                                chunk_file.path, chunk_data)
                else:
                    rollback_cb("delete_file",
                                chunk_file.path, None)
            else:
                rollback_cb("delete_file",
                            chunk_file.path, None)

        target_array.append(slice_array, axis=append_axis)

    logger.info(f"Consolidating target dataset")
    metadata_file = target_dir / ".zmetadata"
    metadata_data = metadata_file.read()
    rollback_cb("replace_file", metadata_file.path, metadata_data)
    target_store = get_zarr_store(target_dir)
    zarr.convenience.consolidate_metadata(target_store)
