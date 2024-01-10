# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import dask.array
import numpy as np
import xarray as xr

from .metadata import DatasetMetadata
from .log import logger


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
    dataset: xr.Dataset, target_metadata: DatasetMetadata, append_dim_name: str
) -> xr.Dataset:
    dataset = _strip_dataset(dataset, target_metadata)
    dataset = _complete_dataset(dataset, target_metadata)

    const_variables = [
        k for k, v in dataset.variables.items() if append_dim_name not in v.dims
    ]
    if const_variables:
        # Strip variables that do not have append_dim_name
        # as dimension, e.g., "x", "y", "crs", ...
        dataset = dataset.drop_vars(const_variables)

    # Remove any encoding and attributes from slice,
    # since both are prescribed by target
    dataset.attrs.clear()
    for variable in dataset.variables.values():
        variable.encoding = {}
        variable.attrs = {}
    return dataset


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
