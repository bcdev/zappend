# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr

from .context import Context
from .fsutil.transaction import Transaction
from .fsutil.transaction import RollbackCallback
from .log import logger
from .slicesource import open_slice_source
from .zutil import get_zarr_arrays_for_dim
from .zutil import open_zarr_group
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
        """Process a single slice.

        If there is no target yet, just config and slice:

        * complete config outline from slice outline
        * check config/slice outline compliance
        * copy slice and add/remove vars to/from slice
        * set encoding in slice
        * write target from slice

        If target exists, with config, slice, and target:

        * complete config outline from target outline
        * check config/target outline compliance
        * check config/slice outline compliance
        * copy slice and add/remove vars to/from slice
        * remove encoding from slice
        * update target from slice
        """

        with open_slice_source(self.ctx, slice_obj) as slice_ds:
            target_dir = self.ctx.target_dir
            create = not target_dir.exists()
            with Transaction(target_dir, self.ctx.temp_dir) as rollback_cb:
                if create:
                    create_target_from_slice(self.ctx,
                                             slice_ds,
                                             rollback_cb)
                else:
                    update_target_from_slice(self.ctx,
                                             slice_ds,
                                             rollback_cb)


def create_target_from_slice(ctx: Context,
                             slice_ds: xr.Dataset,
                             rollback_cb: RollbackCallback):

    target_ds = ctx.configure_target_ds(slice_ds)
    target_dir = ctx.target_dir
    try:
        target_ds.to_zarr(store=target_dir.uri,
                          storage_options=target_dir.storage_options,
                          zarr_version=ctx.zarr_version,
                          write_empty_chunks=False,
                          consolidated=True)
    finally:
        if target_dir.exists():
            rollback_cb("delete_dir", target_dir.path, None)


def update_target_from_slice(ctx: Context,
                             slice_ds: xr.Dataset,
                             rollback_cb: RollbackCallback):

    target_dir = ctx.target_dir
    append_dim = ctx.append_dim
    target_group = open_zarr_group(target_dir)
    target_arrays = get_zarr_arrays_for_dim(target_group, append_dim)

    slice_ds = ctx.configure_slice_ds(slice_ds)

    for array_name, (target_array, append_axis) in target_arrays.items():
        try:
            slice_var: xr.DataArray = slice_ds[array_name]
        except KeyError:
            raise ValueError(f"Array {array_name!r} not found in slice")

        # TODO: Do not rely on _ARRAY_DIMENSIONS, instead use
        #   mapping var_name -> append_axis as input
        target_dims = tuple(target_array.attrs.get("_ARRAY_DIMENSIONS"))
        slice_dims = slice_var.dims
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
                                   slice_var.shape[append_axis])

        chunk_indexes = get_chunk_indices(target_array.shape,
                                          target_array.chunks,
                                          append_axis,
                                          append_dim_range)

        start, _ = append_dim_range

        for chunk_index in chunk_indexes:
            chunk_filename = ".".join(map(str, chunk_index))
            chunk_file = array_dir / chunk_filename
            if chunk_update and chunk_index[append_axis] == start:
                # TODO: test this path, i.e.,
                #   rollback actions "delete_dir" and "delete_file"
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

    # TODO: adjust global attributes dependent on append_dim,
    #  e.g., time coverage
    logger.info(f"Consolidating target dataset")
    metadata_file = target_dir / ".zmetadata"
    metadata_data = metadata_file.read()
    rollback_cb("replace_file", metadata_file.path, metadata_data)

    slice_ds.to_zarr(store=target_dir.uri,
                     storage_options=target_dir.storage_options,
                     write_empty_chunks=False,
                     consolidated=True,
                     mode="a",
                     append_dim=append_dim)

    # target_store = get_zarr_store(target_dir)
    # zarr.convenience.consolidate_metadata(target_store)
