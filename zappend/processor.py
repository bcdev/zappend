# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr

from .config import ConfigLike
from .config import normalize_config
from .config import validate_config
from .context import Context
from .fsutil.transaction import Transaction
from .fsutil.transaction import RollbackCallback
from .log import logger
from .slicesource import open_slice_source
from .tailoring import tailor_target_dataset
from .tailoring import tailor_slice_dataset
from .chunkutil import get_chunk_update_range
from .chunkutil import get_chunk_indices


class Processor:
    def __init__(self, config: ConfigLike = None, **kwargs):
        config = normalize_config(config)
        config.update({k: v for k, v in kwargs.items() if v is not None})
        validate_config(config)
        self._config = config

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

        ctx = Context(self._config)

        with open_slice_source(ctx, slice_obj) as slice_ds:
            target_dir = ctx.target_dir
            create = not target_dir.exists()
            with Transaction(target_dir, ctx.temp_dir) as rollback_cb:
                if create:
                    create_target_from_slice(ctx,
                                             slice_ds,
                                             rollback_cb)
                else:
                    update_target_from_slice(ctx,
                                             slice_ds,
                                             rollback_cb)


def create_target_from_slice(ctx: Context,
                             slice_ds: xr.Dataset,
                             rollback_cb: RollbackCallback):
    target_ds = tailor_target_dataset(slice_ds,
                                      ctx.included_var_names,
                                      ctx.excluded_var_names,
                                      ctx.target_variables,
                                      ctx.target_attrs)
    target_dir = ctx.target_dir
    # TODO: adjust global attributes dependent on append_dim,
    #  e.g., time coverage
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
    append_dim_name = ctx.append_dim_name
    target_dim_sizes = ctx.target_dim_sizes

    slice_ds = tailor_slice_dataset(slice_ds,
                                    ctx.included_var_names,
                                    ctx.excluded_var_names,
                                    ctx.target_variables)

    # Emit rollback actions
    for var_name, var_config in ctx.target_variables.items():
        assert "dims" in var_config
        target_var_dim_names = var_config["dims"]

        try:
            append_axis = target_var_dim_names.index(append_dim_name)
        except ValueError:
            # append dimension does not exist in variable,
            # so we cannot append data, hence no need to emit
            # rollback actions
            continue

        assert "encoding" in var_config
        target_var_encoding = var_config["encoding"]

        assert "chunks" in target_var_encoding
        target_var_chunks = target_var_encoding["chunks"]
        target_var_shape = tuple(target_dim_sizes[k]
                                 for k in target_var_dim_names)

        assert var_name in slice_ds
        slice_var = slice_ds.variables[var_name]

        array_dir = target_dir / var_name

        array_metadata_file = array_dir / ".zarray"
        array_metadata = array_metadata_file.read()
        rollback_cb("replace_file",
                    array_metadata_file.path, array_metadata)

        chunk_update, append_dim_range = \
            get_chunk_update_range(target_var_shape[append_axis],
                                   target_var_chunks[append_axis],
                                   slice_var.shape[append_axis])

        chunk_indexes = get_chunk_indices(target_var_shape,
                                          target_var_chunks,
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
                     append_dim=append_dim_name)

    # target_store = get_zarr_store(target_dir)
    # zarr.convenience.consolidate_metadata(target_store)
