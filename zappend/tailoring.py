# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Literal

import dask.array
import numpy as np
import xarray as xr

from .config import DEFAULT_APPEND_DIM
from .config import DEFAULT_ATTRS_UPDATE_MODE
from .log import logger
from .metadata import DatasetMetadata


def tailor_target_dataset(
    dataset: xr.Dataset, target_metadata: DatasetMetadata
) -> xr.Dataset:
    dataset = _strip_dataset(dataset, target_metadata)
    dataset = _complete_dataset(dataset, target_metadata)

    # Complement dataset attributes and set
    # variable encoding and attributes
    dataset.attrs = target_metadata.attrs
    for var_name, var_metadata in target_metadata.variables.items():
        variable = dataset.variables[var_name]
        variable.encoding = var_metadata.encoding.to_dict()
        variable.attrs = var_metadata.attrs
    return dataset


def tailor_slice_dataset(
    slice_ds: xr.Dataset,
    target_metadata: DatasetMetadata,
    append_dim: str = DEFAULT_APPEND_DIM,
    attrs_update_mode: (
        Literal["keep"] | Literal["replace"] | Literal["update"]
    ) = DEFAULT_ATTRS_UPDATE_MODE,
    attrs: dict[str, Any] | None = None,
) -> xr.Dataset:
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
    drop_var_names = set(map(str, dataset.variables.keys())) - set(
        target_metadata.variables.keys()
    )
    return dataset.drop_vars(drop_var_names)


def _complete_dataset(
    dataset: xr.Dataset, target_metadata: DatasetMetadata
) -> xr.Dataset:
    for var_name, var_metadata in target_metadata.variables.items():
        var = dataset.variables.get(var_name)
        if var is not None:
            continue
        logger.warning(
            f"Variable {var_name!r} not found in slice dataset;" f" creating it."
        )
        encoding = var_metadata.encoding.to_dict()
        chunks = encoding.get("chunks")
        if chunks is None:
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
