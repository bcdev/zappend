# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import dask.array
import numpy as np
import xarray as xr

from .context import Context
from .log import logger
from .metadata import DatasetMetadata


def tailor_target_dataset(ctx: Context, slice_ds: xr.Dataset) -> xr.Dataset:
    target_metadata = ctx.target_metadata
    attrs_update_mode = ctx.config.attrs_update_mode
    attrs = ctx.config.attrs

    target_ds = _strip_dataset(slice_ds, target_metadata)
    target_ds = _complete_dataset(target_ds, target_metadata)

    # Set initial dataset attributes
    if attrs_update_mode == "ignore":
        # Ignore attributes from slice dataset
        target_ds.attrs = {}
    else:
        target_ds.attrs = target_metadata.attrs
    if attrs:
        # Always update by configured attributes
        target_ds.attrs.update(attrs)

    # Set variable encoding and attributes
    for var_name, var_metadata in target_metadata.variables.items():
        variable = target_ds.variables[var_name]
        variable.encoding = var_metadata.encoding.to_dict()
        variable.attrs = var_metadata.attrs

    return target_ds


def tailor_slice_dataset(ctx: Context, slice_ds: xr.Dataset) -> xr.Dataset:
    target_metadata = ctx.target_metadata
    append_dim = ctx.config.append_dim
    attrs_update_mode = ctx.config.attrs_update_mode
    attrs = ctx.config.attrs

    slice_ds = _strip_dataset(slice_ds, target_metadata)
    slice_ds = _complete_dataset(slice_ds, target_metadata)

    const_variables = [
        k for k, v in slice_ds.variables.items() if append_dim not in v.dims
    ]
    if const_variables:
        # Strip variables that do not have append_dim
        # as dimension, e.g., "x", "y", "crs", ...
        slice_ds = slice_ds.drop_vars(const_variables)

    # https://github.com/bcdev/zappend/issues/56
    # slice_dataset.to_zarr(store, mode="a", ...) will replace
    # global attributes.
    # Therefore, we must take care how slice dataset attributes
    # are updated.
    # If attrs_update_op is "replace", we just keep slice attributes
    if attrs_update_mode == "keep":
        # Keep existing attributes
        slice_ds.attrs = target_metadata.attrs
    elif attrs_update_mode == "update":
        # Update from last slice dataset
        slice_ds.attrs = target_metadata.attrs | slice_ds.attrs
    elif attrs_update_mode == "ignore":
        # Ignore attributes from slice dataset
        slice_ds.attrs = {}
    if attrs:
        # Always update by configured attributes
        slice_ds.attrs.update(attrs)

    # Remove any encoding and attributes from slice,
    # since both are prescribed by target
    for variable in slice_ds.variables.values():
        variable.encoding = {}
        variable.attrs = {}
    return slice_ds


def _strip_dataset(dataset: xr.Dataset, target_metadata: DatasetMetadata) -> xr.Dataset:
    """Remove unwanted variables from `dataset` and return a copy."""
    drop_var_names = set(map(str, dataset.variables.keys())) - set(
        target_metadata.variables.keys()
    )
    return dataset.drop_vars(drop_var_names)


def _complete_dataset(
    dataset: xr.Dataset, target_metadata: DatasetMetadata
) -> xr.Dataset:
    undefined = object()
    """Chunk existing variables according to chunks in encoding or 
    add missing variables to `dataset` (in-place operation) and return it.
    """
    for var_name, var_metadata in target_metadata.variables.items():
        var = dataset.variables.get(var_name)
        encoding = var_metadata.encoding.to_dict()
        chunks = encoding.get("chunks", undefined)
        if var is not None:
            if chunks is None:
                # May emit warning for large shapes
                chunks = var_metadata.shape
            if chunks is not undefined:
                var = var.chunk(chunks=chunks)
        else:
            logger.warning(
                f"Variable {var_name!r} not found in slice dataset; creating it."
            )
            if chunks is None or chunks is undefined:
                # May emit warning for large shapes
                chunks = var_metadata.shape
            if encoding.get("_FillValue") is not None:
                # Since we have a defined fill value, the decoded in-memory
                # variable uses NaN where fill value will be stored.
                # This ia also what xarray does if decode_cf=True.
                memory_dtype = np.dtype("float64")
                memory_fill_value = float("NaN")
            else:
                # Fill value is not defined, so we use the data type
                # defined in the encoding, if any and fill memory with zeros.
                memory_dtype = encoding.get("dtype", np.dtype("float64"))
                memory_fill_value = 0
            var = xr.DataArray(
                dask.array.full(
                    var_metadata.shape,
                    memory_fill_value,
                    chunks=chunks,
                    dtype=np.dtype(memory_dtype),
                ),
                dims=var_metadata.dims,
            )
        dataset[var_name] = var
    return dataset
