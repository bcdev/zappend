# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable, Any, Callable

import dask.array
import numcodecs
import numpy as np
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


def _parse_fill_value(fill_value: Any) -> Any:
    if fill_value == "NaN":
        return float("NaN")
    return fill_value


def _parse_identity(scale_factor: Any) -> Any:
    return scale_factor


def _parse_compressor(compressor: dict | None) -> Any:
    if not compressor:
        return None
    return numcodecs.get_codec(compressor)


def _parse_filters(filters: list[dict] | None) -> Any:
    if not filters:
        return None
    return list(map(numcodecs.get_codec, filters))


_ENCODING_PROPS: dict[str, Callable[[Any]: Any]] = {
    "dtype": np.dtype,
    "fill_value": _parse_fill_value,
    "scale_factor": _parse_identity,
    "add_offset": _parse_identity,
    "compressor": _parse_compressor,
    "filters": _parse_filters
}


def create_target_from_slice(ctx: Context,
                             slice_ds: xr.Dataset,
                             rollback_cb: RollbackCallback):
    target_dir = ctx.target_dir

    slice_ds = slice_ds.copy()

    dataset_var_names = set(map(str, slice_ds.variables.keys()))
    included_var_names = ctx.included_var_names
    excluded_var_names = ctx.excluded_var_names
    if not included_var_names:
        included_var_names = dataset_var_names
    if excluded_var_names:
        included_var_names -= excluded_var_names

    for var_name in included_var_names:
        var_config = ctx.variable_config(var_name)
        if var_name in slice_ds.variables:
            var = slice_ds[var_name]
        else:
            logger.warning(f"Variable {var_name!r} not found in slice dataset;"
                           f" creating it.")
            dims = var_config.get("dims")
            if not dims:
                raise ValueError(f"Cannot create variable {var_name!r}"
                                 f" because its dimensions are unspecified")
            try:
                shape = tuple(map(lambda dim_name: slice_ds.dims[dim_name],
                                  dims))
            except KeyError:
                raise ValueError(f"Cannot create variable {var_name!r}"
                                 f" because at least one of its dimensions"
                                 f" {dims!r} does not exist in the"
                                 f" slice dataset")
            chunks = var_config.get("chunks", shape)

            if ("fill_value" in var_config
                    and var_config["fill_value"] is not None):
                memory_dtype = "float64"
                memory_fill_value = float("NaN")
            else:
                memory_dtype = var_config.get("dtype", "float32")
                memory_fill_value = var_config.get("fill_value")
                if memory_fill_value is None:
                    if memory_dtype in ("float32", "float64"):
                        memory_fill_value = float("NaN")
                    else:
                        memory_fill_value = 0
            var = xr.DataArray(
                dask.array.full(shape,
                                memory_fill_value,
                                chunks=chunks,
                                dtype=np.dtype(memory_dtype)),
                dims=dims
            )

        var.encoding.update({
            k: parse(var_config[k])
            for k, parse in _ENCODING_PROPS.items()
            if k in var_config
        })
        if "attrs" in var_config:
            var_attrs = var_config["attrs"] or {}
            var.attrs.update(var_attrs)

    try:
        slice_ds.to_zarr(target_dir.uri,
                         storage_options=target_dir.storage_options,
                         zarr_version=ctx.zarr_version,
                         write_empty_chunks=False)
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

    for array_name, (target_array, append_axis) in target_arrays.items():
        try:
            slice_var: xr.DataArray = slice_ds[array_name]
        except KeyError:
            raise ValueError(f"Array {array_name!r} not found in slice")

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

    # Remove any encoding and attributes from slice,
    # since both are prescribed by target
    slice_ds = slice_ds.copy()
    slice_ds.attrs = {}
    for slice_var in slice_ds.variables.values():
        slice_var.attrs.clear()
        slice_var.encoding.clear()

    slice_ds.to_zarr(target_dir.uri,
                     storage_options=target_dir.storage_options,
                     write_empty_chunks=False,
                     consolidated=True,
                     append_dim=append_dim)

    # target_store = get_zarr_store(target_dir)
    # zarr.convenience.consolidate_metadata(target_store)
